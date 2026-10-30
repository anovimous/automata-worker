package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/buildinfo"

	informerP "github.com/anovimous/automata-worker/orchestrator/internal/central/informer"
	pollerP "github.com/anovimous/automata-worker/orchestrator/internal/central/poller"
	configP "github.com/anovimous/automata-worker/orchestrator/internal/config"
	"github.com/anovimous/automata-worker/orchestrator/internal/job"
	runnerP "github.com/anovimous/automata-worker/orchestrator/internal/job/runner"
	"github.com/anovimous/automata-worker/orchestrator/internal/job/socket"
	watcherP "github.com/anovimous/automata-worker/orchestrator/internal/job/watcher"
	"github.com/anovimous/automata-worker/orchestrator/internal/job/workdir"
	mapperP "github.com/anovimous/automata-worker/orchestrator/internal/mapper"
	queueP "github.com/anovimous/automata-worker/orchestrator/internal/queue"
	"github.com/anovimous/automata-worker/orchestrator/internal/retry"
	rustfsP "github.com/anovimous/automata-worker/orchestrator/internal/rustfs"
	"github.com/anovimous/automata-worker/orchestrator/internal/telemetry"
	wordlistP "github.com/anovimous/automata-worker/orchestrator/internal/wordlist"
)

// how long to wait before looking at the queue again when it was empty
const idleQueuePollInterval = 2 * time.Second

// uploading the result is retried like the reports to the central service
var uploadPolicy = retry.Policy{Attempts: 5, BaseDelay: time.Second, MaxDelay: 30 * time.Second}

// how long the wordlists of a job may take to download; the queue message stays unacked meanwhile, and
// RabbitMQ's own limit for that is 30 minutes
const wordlistDownloadTimeout = 20 * time.Minute

// a wordlist download is retried (a dropped connection, a file which came out wrong) before the job fails
var wordlistPolicy = retry.Policy{Attempts: 3, BaseDelay: 2 * time.Second, MaxDelay: 15 * time.Second}

// the job loop wakes up this often to show that it is alive, when nothing else happens
const progressInterval = 5 * time.Second

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck(os.Getenv("TELEMETRY_ADDR"), &http.Client{Timeout: 3 * time.Second}))
	}
	os.Exit(run())
}

// run is the orchestrator. It returns the exit code instead of exiting, so that the deferred cleanup runs first:
// 1 for a fatal error (config, connections, the queue), which a restart policy on-failure restarts.
func run() int {

	// Load global config
	cfg, err := configP.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		return 1
	}
	// from here on every record carries the worker id (errors of the config above use the default logger)
	log.SetDefault(newLogger(cfg.Telemetry.LogFormat, cfg.Telemetry.WorkerID, os.Stderr))

	// The status of the worker, served for the monitor
	tracker := telemetry.NewTracker(cfg.Telemetry.WorkerID,
		telemetry.BuildInfo{Version: buildinfo.Version, Commit: buildinfo.Commit, GoVersion: runtime.Version()}, nil, time.Now)
	if cfg.Telemetry.Addr != "" {
		server := telemetry.NewServer(cfg.Telemetry.Addr, tracker)
		if err := server.Start(); err != nil {
			log.Error("failed to start the telemetry server", "error", err)
			return 1
		}
		// deferred first so that it runs last: the status stays readable during the cleanup
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			server.Shutdown(ctx)
		}()
	}

	// Load mapping config internally into the mapper
	mapper, err := mapperP.Load()
	if err != nil {
		log.Error("failed to load mappings", "error", err)
		return 1
	}

	// Create Rustfs
	rustfs, err := rustfsP.New(cfg.RustFS)
	if err != nil {
		log.Error("failed to create rustfs client", "error", err)
		return 1
	}

	// Create the cache of the wordlist files
	wordlists, err := wordlistP.New(cfg.WordlistCache.Dir, cfg.WordlistCache.MaxBytes,
		observedDownloader{downloader: rustfs, recorder: tracker}, wordlistPolicy)
	if err != nil {
		log.Error("failed to create the wordlist cache", "error", err)
		return 1
	}
	tracker.SetCacheStats(wordlistCacheStats(wordlists))

	// Create Poller and Informer; their results show in the status as the core's
	poller := pollerP.New(cfg.Central.BaseURL, cfg.Central.StateEndpoint, cfg.Central.PollInterval)
	poller.Observe(func(err error) { tracker.DependencyResult(telemetry.DepCore, err) })
	informer := observedInformer{informer: informerP.New(cfg.Central.BaseURL, cfg.Central.OutcomeEndpoint), recorder: tracker}

	// Create Runner
	runner := runnerP.New(cfg.Worker)

	// Create Watcher
	watcher := watcherP.New(cfg.Worker.HeartbeatTimeout)

	// Create new RabbitMQ connection
	consumer, err := queueP.New(cfg.RabbitMQ)
	if err != nil {
		log.Error("failed to create rabbitmq consumer", "error", err)
		return 1
	}

	defer consumer.Close()

	// a lost connection shows in the status before the next Get fails
	closed := consumer.NotifyClosed()
	go func() {
		for err := range closed {
			tracker.DependencyDown(telemetry.DepRabbitMQ, err)
		}
	}()

	// Create the two sockets responsible for job IPC
	offReadSocket, err := socket.New(cfg.Worker.SocketDir, 'r')
	if err != nil {
		log.Error("failed to create read socket", "error", err)
		return 1
	}
	offWriteSocket, err := socket.New(cfg.Worker.SocketDir, 'w')
	if err != nil {
		log.Error("failed to create write socket", "error", err)
		return 1
	}

	defer offReadSocket.Listener.Close()
	defer offWriteSocket.Listener.Close()

	// Take one message at a time, only when no job is running
	tracker.SetPhase(telemetry.PhaseIdle, "")
	for {

		msg, ok, err := consumer.Next()
		tracker.DependencyResult(telemetry.DepRabbitMQ, err)
		if err != nil {
			log.Error("failed to take a message from the queue", "error", err)
			return 1
		}
		if !ok {
			tracker.Touch()
			time.Sleep(idleQueuePollInterval)
			continue
		}

		// The message is acked once the job has started, or when it needs no more
		// attempts. Until then a crash of this orchestrator puts it back on the queue.
		ack := ackOnce(func() error { return consumer.Ack(msg) })

		/* MAIN LOOP FLOW:
		get jobId from message
		download from rustfs JobDetailsFile and JobData file into TMP_DIR
		initial poll
		get the wordlists into the cache and give the connector their paths
		start the job
		report RUNNING and ack message
		start the poller
		start the watcher
		*/
		log.Debug("entered the message loop")
		func() {
			// back to idle once the message is over, after the cleanup deferred below
			defer tracker.SetPhase(telemetry.PhaseIdle, "")

			// Decode message payload into JSON and extract jobId
			var payload queueP.Message

			if err := json.Unmarshal(msg.Body, &payload); err != nil {
				log.Error("failed to unmarshal message payload", "error", err)
				tracker.MessageSkipped()
				ack()
				return
			}

			jobId := payload.JobId
			tracker.SetPhase(telemetry.PhasePreparing, "CREATING_WORKDIR")
			tracker.MessageTaken(jobId, payload.RoutineKey)

			// Everything of this job lives in its own directory, removed when the job is over
			jobDir, err := workdir.Create(cfg.TempDir, jobId)
			if err != nil {
				log.Error("failed to create the work directory", "jobId", jobId, "error", err)
				inform(informer, jobId, job.StateFailed, "failed to create the work directory of the job", job.FailurePreparation)
				tracker.JobEnded(job.StateFailed)
				ack()
				return
			}
			defer func() {
				if err := workdir.Remove(jobDir); err != nil {
					log.Error("failed to remove the work directory", "jobId", jobId, "dir", jobDir, "error", err)
				}
			}()

			// Download from Rustfs
			tracker.SetDetail("DOWNLOADING_FILES")
			detailsLocation, dataLocation, err := rustfs.DownloadJobDetailsAndData(jobId, jobDir)
			tracker.DependencyResult(telemetry.DepObjectStorage, err)
			if err != nil {
				log.Error("failed to download from rustfs the required two files", "jobId", jobId, "error", err)
				inform(informer, jobId, job.StateFailed, "failed to download details and data files from rustfs", job.FailurePreparation)
				tracker.JobEnded(job.StateFailed)
				ack()
				return
			}
			log.Info("Downloaded from rustfs")

			// Poll initially to make sure there is not state change request
			tracker.SetDetail("INITIAL_POLL")
			initialPollResult, err := poller.SinglePoll(jobId)
			tracker.DependencyResult(telemetry.DepCore, err)
			if err != nil {
				log.Error("failed to poll central service", "jobId", jobId, "error", err)
				inform(informer, jobId, job.StateFailed, "job has failed before start at initial poll due to failure to poll", job.FailurePoll)
				tracker.JobEnded(job.StateFailed)
				ack()
				return
			}
			start, report, message := decideInitialPoll(initialPollResult.DesiredState)
			if !start {
				if report == "" {
					log.Info("skipping the message, the job is not waiting to be started", "jobId", jobId, "desiredState", initialPollResult.DesiredState)
					tracker.MessageSkipped()
				} else {
					inform(informer, jobId, report, message, "")
					tracker.JobEnded(report)
				}
				ack()
				return
			}

			// Get the wordlists of the job into the cache, after the poll so that a job which is not
			// started does not download them
			tracker.SetDetail("WORDLISTS")
			wordlistsCtx, cancelWordlists := context.WithTimeout(context.Background(), wordlistDownloadTimeout)
			releaseWordlists, err := prepareWordlists(wordlistsCtx, wordlists, detailsLocation)
			cancelWordlists()
			if err != nil {
				log.Error("failed to prepare the wordlists", "jobId", jobId, "error", err)
				inform(informer, jobId, job.StateFailed, fmt.Sprintf("failed to prepare the wordlists: %s", err), job.FailurePreparation)
				tracker.JobEnded(job.StateFailed)
				ack()
				return
			}
			defer releaseWordlists()

			// Get the command tagged to the RoutineKey
			tracker.SetDetail("MAPPING")
			command, err := mapper.Lookup(payload.RoutineKey)
			if err != nil {
				log.Error("failed to find mapping for routineKey", "routineKey", payload.RoutineKey, "jobId", jobId, "error", err)
				inform(informer, jobId, job.StateFailed, "failed to find mapping for routineKey", job.FailurePreparation)
				tracker.JobEnded(job.StateFailed)
				ack()
				return
			}

			// Start Job process
			tracker.SetPhase(telemetry.PhaseStartingJob, "STARTING_CONNECTOR")
			tracker.JobCommand(command)
			process, err := runner.Start(jobId, jobDir, command, detailsLocation, dataLocation, offReadSocket, offWriteSocket)
			if err != nil {
				log.Error("failed to start process", "jobId", jobId, "error", err)
				inform(informer, jobId, job.StateFailed, fmt.Sprintf("Job process failed to start at orchestrator runner.start: %s", err), job.FailureToolStart)
				tracker.JobEnded(job.StateFailed)
				ack()
				return
			}
			tracker.ConnectorStarted(cfg.Worker.HeartbeatInterval, cfg.Worker.HeartbeatTimeout)
			// the job cgroup exists from here on, so it goes away on every exit path
			defer func() {
				if err := process.Cgroup().Destroy(); err != nil {
					log.Error("failed to remove job cgroup", "jobId", jobId, "error", err)
				}
			}()

			log.Info("Started job successfully, informing central of RUNNING state", "jobId", jobId)
			inform(informer, jobId, job.StateRunning, "", "")
			ack()
			tracker.JobStarted()

			// Accept connections on sockets
			tracker.SetDetail("ACCEPTING_SOCKETS")
			activeReader, activeWriter, err1, err2 := socket.AcceptConns(offReadSocket, offWriteSocket, 30)
			if err1 != nil || err2 != nil {
				process.Terminate()
				inform(informer, jobId, job.StateSelfTerminated, "Process didn't connect to the sockets in the required time", job.FailureToolStart)
				tracker.JobEnded(job.StateSelfTerminated)
				log.Info("Job process terminated due to not connecting to sockets before timeout", "jobId", jobId)
				return
			}
			tracker.ConnectorConnected()
			tracker.SetPhase(telemetry.PhaseRunningJob, "")

			ctx, cancel := context.WithCancel(context.Background())

			// Listen for messages via the socket
			fmtMessageCh := activeReader.Listen(ctx)

			// Watch formatted socket messages forwarded from reader
			// Watcher is terminated automatically on completion or timeout
			// The status learns of every message, the heartbeats included
			completionCh, stateChangeCh, timeoutCh := watcher.Watch(fmtMessageCh, func(m job.JobMessage) { tracker.ConnectorMessage(m.Typ) })

			// Start polling central-service for new state update
			pollCh, resendPolledState := poller.Start(jobId, ctx)

			// a running job that needs nothing wakes no case below, the ticker shows the loop is alive anyway
			progress := time.NewTicker(progressInterval)
			defer progress.Stop()

		loop:
			for {
				select {
				case <-progress.C:
					tracker.Touch()
				case pollResult := <-pollCh:
					tracker.Touch()
					if stopsTheJob(pollResult.DesiredState) {
						// the core gave the job up because it heard nothing from this worker, so
						// there is nothing to report and the tool must not go on running
						log.Warn("the central service considers the job lost, stopping it", "jobId", jobId)
						cancel()
						process.Terminate()
						tracker.JobEnded(job.StateLostContact)
						break loop
					}
					// forward state to worker via socket
					log.Info("ORCH case pollResult called")
					if err := activeWriter.SendState(pollResult.DesiredState); err != nil {
						// the worker did not get it, so it is sent again on the next poll
						log.Error("failed to send the desired state to the worker", "jobId", jobId, "desiredState", pollResult.DesiredState, "error", err)
						resendPolledState()
					}
					log.Info("ORCH case pollResult finished after sending state")
					// COMPLETE HERE CHECKING WHY WHEN RUNNING, when s.sendMessage is sent in sdk.New it reaches the watcher, while the standard heartbeat loop is not reaching
				case jobResult := <-completionCh:
					// job finished or failed
					log.Info("ORCH case jobResult called")
					tracker.SetPhase(telemetry.PhaseFinishing, "UPLOADING_RESULT")
					cancel()
					finishState, finishMessage, finishCategory := completionReport(jobId, jobResult, func(jobId int, resultPath string) error {
						err := uploadPolicy.Do(func() error { return rustfs.UploadResult(jobId, resultPath) })
						tracker.DependencyResult(telemetry.DepObjectStorage, err)
						return err
					})
					tracker.SetDetail("REPORTING")
					inform(informer, jobId, finishState, finishMessage, finishCategory)
					tracker.JobEnded(finishState)
					break loop
				case state := <-stateChangeCh:
					// state changed internally or as a result of sending a polled state
					log.Info("ORCH case state called")
					tracker.Touch()
					tracker.JobState(state)
					inform(informer, jobId, state, "", "")

				case <-timeoutCh:
					// heartbeat timeout
					log.Info("ORCH case TIMOUT called")
					tracker.Touch()
					cancel()
					process.Terminate()
					inform(informer, jobId, job.StateSelfTerminated, "Timeout has been reached", job.FailureUnknown)
					tracker.JobEnded(job.StateSelfTerminated)
					break loop
				}
			}

			cancel()

			activeReader.Connection.Close()
			activeWriter.Connection.Close()
		}()

	}

}

// decideInitialPoll decides what to do with a job from the state the core
// answers with before the job is started. Only a QUEUED job is started. A
// job which the user canceled while it was queued is reported CANCELED. For
// any other state the message is stale or a duplicate (another orchestrator
// may be running the job), so the job is not started and nothing is reported
// (report is empty).
func decideInitialPoll(desired job.JobState) (start bool, report job.JobState, message string) {
	switch desired {
	case job.StateQueued:
		return true, "", ""
	case job.StateCanceled:
		return false, job.StateCanceled, "job was canceled before it started"
	default:
		return false, "", ""
	}
}

// stopsTheJob tells whether the state the core answers with while the job runs
// means that the job has to be stopped without any report. LOSTCONTACT is the
// core taking the job away from a worker which it thought gone, so it is a
// final state the core does not accept reports for anymore.
func stopsTheJob(desired job.JobState) bool {
	return desired == job.StateLostContact
}
