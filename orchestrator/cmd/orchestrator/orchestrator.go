package main

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"os"
	"time"

	informerP "github.com/anovimous/automata-worker/orchestrator/internal/central/informer"
	pollerP "github.com/anovimous/automata-worker/orchestrator/internal/central/poller"
	configP "github.com/anovimous/automata-worker/orchestrator/internal/config"
	"github.com/anovimous/automata-worker/orchestrator/internal/job"
	runnerP "github.com/anovimous/automata-worker/orchestrator/internal/job/runner"
	"github.com/anovimous/automata-worker/orchestrator/internal/job/socket"
	watcherP "github.com/anovimous/automata-worker/orchestrator/internal/job/watcher"
	mapperP "github.com/anovimous/automata-worker/orchestrator/internal/mapper"
	queueP "github.com/anovimous/automata-worker/orchestrator/internal/queue"
	"github.com/anovimous/automata-worker/orchestrator/internal/retry"
	rustfsP "github.com/anovimous/automata-worker/orchestrator/internal/rustfs"
)

// uploading the result is retried like the reports to the central service
var uploadPolicy = retry.Policy{Attempts: 5, BaseDelay: time.Second, MaxDelay: 30 * time.Second}

func main() {

	// Load global config
	cfg, err := configP.Load()
	if err != nil {
		log.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Load mapping config internally into the mapper
	mapper, err := mapperP.Load()
	if err != nil {
		log.Error("failed to load mappings", "error", err)
		os.Exit(1)
	}

	// Create Rustfs
	rustfs, err := rustfsP.New(cfg.RustFS, cfg.TempDir)
	if err != nil {
		log.Error("failed to create rustfs client", "error", err)
		os.Exit(1)
	}

	// Create Poller and Informer
	poller := pollerP.New(cfg.Central.BaseURL, cfg.Central.StateEndpoint, cfg.Central.PollInterval)
	informer := informerP.New(cfg.Central.BaseURL, cfg.Central.OutcomeEndpoint)

	// Create Runner
	runner := runnerP.New(cfg.Worker, cfg.TempDir)

	// Create Watcher
	watcher := watcherP.New(cfg.Worker.HeartbeatTimeout)

	// Create new RabbitMQ connection
	consumer, err := queueP.New(cfg.RabbitMQ)
	if err != nil {
		log.Error("failed to create rabbitmq consumer", "error", err)
		os.Exit(1)
	}

	defer consumer.Close()

	// Create the two sockets responsible for job IPC
	offReadSocket, err := socket.New(cfg.Worker.SocketDir, 'r')
	if err != nil {
		log.Error("failed to create read socket", "error", err)
		os.Exit(1)
	}
	offWriteSocket, err := socket.New(cfg.Worker.SocketDir, 'w')
	if err != nil {
		log.Error("failed to create write socket", "error", err)
		os.Exit(1)
	}

	defer offReadSocket.Listener.Close()
	defer offWriteSocket.Listener.Close()

	// Get queue messages channel
	messagesCh, err := consumer.Consume()
	if err != nil {
		log.Error("failed to start consuming from rabbitmq", "error", err)
		os.Exit(1)
	}
	// Consume messages when they arrive
	for msg := range messagesCh {

		/* MAIN LOOP FLOW:
		get jobId from message
		download from rustfs JobDetailsFile and JobData file into TMP_DIR
		ack message
		initial poll
		start the job
		start the poller
		start the watcher
		*/
		fmt.Printf("Entered messages loop\n")
		// Decode message payload into JSON and extract jobId
		var payload queueP.Message

		if err := json.Unmarshal(msg.Body, &payload); err != nil {
			log.Error("failed to unmarshal message payload", "error", err)
			consumer.Ack(msg)
			continue
		}

		jobId := payload.JobId

		// Download from Rustfs
		detailsLocation, dataLocation, err := rustfs.DownloadJobDetailsAndData(jobId)
		if err != nil {
			log.Error("failed to download from rustfs the required two files", "jobId", jobId, "error", err)
			inform(informer, jobId, job.StateFailed, "failed to download details and data files from rustfs")
			consumer.Ack(msg)
			continue
		}
		log.Info("Downloaded from rustfs")
		// Ack message
		if err := consumer.Ack(msg); err != nil {
			log.Error("failed to ack message", "jobId", jobId, "error", err)
		}

		// Poll initially to make sure there is not state change request
		initialPollResult, err := poller.SinglePoll(jobId)
		if err != nil {
			log.Error("failed to poll central service", "jobId", jobId, "error", err)
			inform(informer, jobId, job.StateFailed, "job has failed before start at initial poll due to failure to poll")
			continue
		}
		start, report, message := decideInitialPoll(initialPollResult.DesiredState)
		if !start {
			if report == "" {
				log.Info("skipping the message, the job is not waiting to be started", "jobId", jobId, "desiredState", initialPollResult.DesiredState)
			} else {
				inform(informer, jobId, report, message)
			}
			continue
		}

		// Get the command tagged to the RoutineKey
		command, err := mapper.Lookup(payload.RoutineKey)
		if err != nil {
			log.Error("failed to find mapping for routineKey", "routineKey", payload.RoutineKey, "jobId", jobId, "error", err)
			inform(informer, jobId, job.StateFailed, "failed to find mapping for routineKey")
			continue
		}

		// Start Job process
		process, err := runner.Start(jobId, command, detailsLocation, dataLocation, offReadSocket, offWriteSocket)
		if err != nil {
			log.Error("failed to start process", "jobId", jobId, "error", err)
			inform(informer, jobId, job.StateFailed, fmt.Sprintf("Job process failed to start at orchestrator runner.start: %s", err))
			continue
		}

		log.Info("Started job successfully, informing central of RUNNING state", "jobId", jobId)
		inform(informer, jobId, job.StateRunning, "")

		// Accept connections on sockets
		activeReader, activeWriter, err1, err2 := socket.AcceptConns(offReadSocket, offWriteSocket, 30)
		if err1 != nil || err2 != nil {
			process.Terminate()
			inform(informer, jobId, job.StateTerminated, "Process didn't connect to the sockets in the required time")
			log.Info("Job process terminated due to not connecting to sockets before timeout", "jobId", jobId)
			continue
		}

		ctx, cancel := context.WithCancel(context.Background())

		// Listen for messages via the socket
		fmtMessageCh := activeReader.Listen(ctx)

		// Watch formatted socket messages forwarded from reader
		// Watcher is terminated automatically on completion or timeout
		completionCh, stateChangeCh, timeoutCh := watcher.Watch(fmtMessageCh)

		// Start polling central-service for new state update
		pollCh, resendPolledState := poller.Start(jobId, ctx)

	loop:
		for {
			select {
			case pollResult := <-pollCh:
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
				cancel()
				finishState, finishMessage := completionReport(jobId, jobResult, func(jobId int, resultPath string) error {
					return uploadPolicy.Do(func() error { return rustfs.UploadResult(jobId, resultPath) })
				})
				inform(informer, jobId, finishState, finishMessage)
				break loop
			case state := <-stateChangeCh:
				// state changed internally or as a result of sending a polled state
				log.Info("ORCH case state called")
				inform(informer, jobId, state, "")

			case <-timeoutCh:
				// heartbeat timeout
				log.Info("ORCH case TIMOUT called")
				cancel()
				process.Terminate()
				inform(informer, jobId, job.StateTerminated, "Timeout has been reached")
				break loop
			}
		}

		cancel()

		activeReader.Connection.Close()
		activeWriter.Connection.Close()

		process.Cgroup().Destroy()

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
