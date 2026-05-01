package main

import (
	"context"
	"encoding/json"
	"strconv"

	informerP "github.com/anovimous/orchestrator/internal/central/informer"
	pollerP "github.com/anovimous/orchestrator/internal/central/poller"
	configP "github.com/anovimous/orchestrator/internal/config"
	"github.com/anovimous/orchestrator/internal/job"
	jobP "github.com/anovimous/orchestrator/internal/job"
	runnerP "github.com/anovimous/orchestrator/internal/job/runner"
	"github.com/anovimous/orchestrator/internal/job/socket"
	watcherP "github.com/anovimous/orchestrator/internal/job/watcher"
	mapperP "github.com/anovimous/orchestrator/internal/mapper"
	queueP "github.com/anovimous/orchestrator/internal/queue"
	rustfsP "github.com/anovimous/orchestrator/internal/rustfs"
)

func main() {

	// Load global config
	cfg, err := configP.Load()
	if err != nil {

	}

	// Load mapping config internally into the mapper
	mapper, err := mapperP.Load(cfg.MappingsFile)
	if err != nil {

	}

	// Create Rustfs
	rustfs, err := rustfsP.New(cfg.RustFS, cfg.TempDir)

	// Create Poller and Informer
	poller := pollerP.New(cfg.Central.BaseURL, cfg.Central.StateEndpoint, cfg.Central.PollInterval)
	informer := informerP.New(cfg.Central.BaseURL, cfg.Central.OutcomeEndpoint)

	// Create Runner
	runner := runnerP.New(cfg.Worker)

	// Create Watcher
	watcher := watcherP.New(cfg.Worker.HeartbeatTimeout)

	// Create new RabbitMQ connection
	consumer, err := queueP.New(cfg.RabbitMQ)
	if err != nil {

	}

	defer consumer.Close()

	// Create the two sockets responsible for job IPC
	offReadSocket, err := socket.New(cfg.Worker.SocketDir, 'r')
	if err != nil {

	}
	offWriteSocket, err := socket.New(cfg.Worker.SocketDir, 'w')
	if err != nil {

	}

	defer offReadSocket.Listener.Close()
	defer offWriteSocket.Listener.Close()

	// Get queue messages channel
	messagesCh, err := consumer.Consume()
	if err != nil {

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

		// Decode message payload into JSON and extract jobId
		var payload queueP.Message

		if err := json.Unmarshal(msg.Body, &payload); err != nil {
		}

		jobId, err := strconv.Atoi(payload.JobId)
		if err != nil {

		}
		// Download from Rustfs
		detailsLocation, dataLocation, err := rustfs.DownloadJobDetailsAndData(jobId)

		// Ack message
		if err := consumer.Ack(msg); err != nil {
		}

		// Poll initially to make sure there is not state change request
		initialPollResult, err := poller.SinglePoll(jobId)
		if err != nil {

		}
		if initialPollResult.DesiredState != jobP.StateQueued {
			informer.Inform(jobId, job.StateCanceled, "Job has been canceled before start at initial poll")
			continue
		}

		// Get the command tagged to the RoutineKey
		command, err := mapper.Lookup(payload.RoutineKey)
		if err != nil {

		}

		// Start Job process
		process, err := runner.Start(jobId, command, detailsLocation, dataLocation, offReadSocket, offWriteSocket)
		if err != nil {

		}

		// Accept connections on sockets
		activeReader, activeWriter, err1, err2 := socket.AcceptConns(offReadSocket, offWriteSocket)
		if err1 != nil || err2 != nil {

		}

		ctx, cancel := context.WithCancel(context.Background())

		// Listen for messages via the socket
		fmtMessageCh, err := activeReader.Listen(ctx)

		// Watch formatted socket messages forwarded from reader
		// Watcher is terminated automatically on completion or timeout
		completionCh, stateChangeCh, timeoutCh, err := watcher.Watch(fmtMessageCh)

		// Start polling central-service for new state update
		pollCh, err := poller.Start(jobId, ctx)

	loop:
		for {
			select {
			case pollResult := <-pollCh:
				// forward state to worker via socket
				activeWriter.SendState(pollResult.DesiredState)

			case jobResult := <-completionCh:
				// job finished or failed
				rustfs.UploadResult(jobId, jobResult.ResultFilePath)
				informer.Inform(jobId, jobResult.FinishState, jobResult.Message)
				break loop
			case state := <-stateChangeCh:
				// state changed internally or as a result of sending a polled state
				informer.Inform(jobId, state, "")

			case <-timeoutCh:
				// heartbeat timeout
				process.Terminate()
				informer.Inform(jobId, job.StateTerminated, "Timeout has been reached")
				break loop
			}
		}

		cancel()

		activeReader.Connection.Close()
		activeWriter.Connection.Close()

	}

}
