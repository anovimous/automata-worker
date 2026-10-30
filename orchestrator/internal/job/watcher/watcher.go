package watcher

import (
	log "log/slog"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

// main functionality is to to track the progress of the job including checking for heartbeats

type Watcher struct {
	heartbeatTimeout time.Duration
}

func New(heartBeatTimeoutSecs int) *Watcher {
	return &Watcher{heartbeatTimeout: time.Duration(heartBeatTimeoutSecs) * time.Second}
}

// Watch reads the messages of the connector until it finishes, goes silent for the heartbeat timeout or its
// connection closes. onMessage, when not nil, is called with every message before it is handled, so that
// others learn of the heartbeats too (they only reset the timeout here).
func (w *Watcher) Watch(fmtMessagesCh <-chan job.JobMessage, onMessage func(job.JobMessage)) (completionResult <-chan job.JobResult, stateUpdate <-chan job.JobState, timeout <-chan bool) {

	completionResultBi := make(chan job.JobResult, 1)
	stateUpdateBi := make(chan job.JobState)
	timeoutBi := make(chan bool, 1)
	log.Info("watcher.watch() called")
	go func() {
		for {
			select {
			case msg, ok := <-fmtMessagesCh:
				if !ok {
					// connection gone, treat like timeout
					log.Info("ORCH watcher.watch(): fmtMessagesCh channel closed, treating it like timout")
					timeoutBi <- true
					return
				}
				log.Info("message received to watcher", "messageType", msg.Typ)
				if onMessage != nil {
					onMessage(msg)
				}
				switch msg.Typ {
				case job.MessageHeartbeat:
					log.Info("Hearbeat received in watcher")
					continue
				case job.MessageStateUpdate:
					stateUpdateBi <- msg.State
					continue
				case job.MessageFinishResult:
					completionResultBi <- msg.Result
					return
				}
			case <-time.After(w.heartbeatTimeout):
				timeoutBi <- true
				return
			}

		}

	}()

	return completionResultBi, stateUpdateBi, timeoutBi
}
