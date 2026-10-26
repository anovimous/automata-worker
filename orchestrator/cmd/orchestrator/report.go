package main

import (
	"fmt"
	log "log/slog"
	"os"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

type stateInformer interface {
	Inform(jobId int, state job.JobState, message string, category job.FailureCategory) error
}

// inform reports a state to the central service. The informer has already
// retried what can be retried, so a failure here is final: it is logged, which
// is the only trace of a report the central service never got.
func inform(informer stateInformer, jobId int, state job.JobState, message string, category job.FailureCategory) error {
	err := informer.Inform(jobId, state, message, category)
	if err != nil {
		log.Error("failed to report the state to the central service, the report is dropped", "jobId", jobId, "state", state, "error", err)
	}
	return err
}

// completionReport decides which state, message and failure category are reported to the central
// service when the job process sent its finish message. Only a FINISHED job has
// a result: it is uploaded first, and if there is none or it can't be uploaded
// the job is reported FAILED (category RESULT), so a FINISHED job always has a result to
// download. A FAILED or CANCELED job has no result and nothing is uploaded; it keeps
// the category its connector gave.
func completionReport(jobId int, result job.JobResult, upload func(jobId int, resultPath string) error) (job.JobState, string, job.FailureCategory) {
	if result.FinishState != job.StateFinished {
		return result.FinishState, result.Message, result.FailureCategory
	}

	if result.ResultFilePath == "" {
		return job.StateFailed, "the job finished without a result file", job.FailureResult
	}

	if _, err := os.Stat(result.ResultFilePath); err != nil {
		return job.StateFailed, fmt.Sprintf("the result file of the job is missing: %v", err), job.FailureResult
	}

	if err := upload(jobId, result.ResultFilePath); err != nil {
		return job.StateFailed, fmt.Sprintf("failed to upload the result of the job: %v", err), job.FailureResult
	}

	return job.StateFinished, result.Message, ""
}

// ackOnce returns a function which acks a message the first time it is called
// and does nothing after, since the paths of the main loop that end a message
// are many and a message can only be acked once. A failure to ack is logged: the
// message then comes back, and the initial poll skips it.
func ackOnce(ack func() error) func() {
	done := false

	return func() {
		if done {
			return
		}
		done = true

		if err := ack(); err != nil {
			log.Error("failed to ack the message", "error", err)
		}
	}
}
