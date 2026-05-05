package watcher

import (
	"time"

	"github.com/anovimous/orchestrator/internal/job"
)

// main functionality is to to track the progress of the job including checking for heartbeats

type Watcher struct {
	heartBeatTimeoutSecs int
}

func New(heartBeatTimeoutSecs int) *Watcher {
	return &Watcher{heartBeatTimeoutSecs: heartBeatTimeoutSecs}
}

func (w *Watcher) Watch(fmtMessagesCh <-chan job.JobMessage) (completionResult <-chan job.JobResult, stateUpdate <-chan job.JobState, timeout <-chan bool) {

	completionResultBi := make(chan job.JobResult, 1)
	stateUpdateBi := make(chan job.JobState)
	timeoutBi := make(chan bool, 1)

	go func() {
		for {
			select {
			case msg := <-fmtMessagesCh:

				switch msg.Typ {
				case job.MessageHeartbeat:
					continue
				case job.MessageStateUpdate:
					stateUpdateBi <- msg.State
					continue
				case job.MessageFinishResult:
					completionResultBi <- msg.Result
					return
				}
			case <-time.After(time.Duration(w.heartBeatTimeoutSecs) * time.Second):
				timeoutBi <- true
				return
			}

		}

	}()

	return completionResultBi, stateUpdateBi, timeoutBi
}
