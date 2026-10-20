package main

import (
	"fmt"
	log "log/slog"
	"os"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

type stateInformer interface {
	Inform(jobId int, state job.JobState, message string) error
}

// inform reports a state to the central service. The informer has already
// retried what can be retried, so a failure here is final: it is logged, which
// is the only trace of a report the central service never got.
func inform(informer stateInformer, jobId int, state job.JobState, message string) error {
	err := informer.Inform(jobId, state, message)
	if err != nil {
		log.Error("failed to report the state to the central service, the report is dropped", "jobId", jobId, "state", state, "error", err)
	}
	return err
}

// completionReport decides which state and message are reported to the central
// service when the job process sent its finish message. Only a FINISHED job has
// a result: it is uploaded first, and if there is none or it can't be uploaded
// the job is reported FAILED, so a FINISHED job always has a result to
// download. A FAILED or CANCELED job has no result and nothing is uploaded.
func completionReport(jobId int, result job.JobResult, upload func(jobId int, resultPath string) error) (job.JobState, string) {
	if result.FinishState != job.StateFinished {
		return result.FinishState, result.Message
	}

	if result.ResultFilePath == "" {
		return job.StateFailed, "the job finished without a result file"
	}

	if _, err := os.Stat(result.ResultFilePath); err != nil {
		return job.StateFailed, fmt.Sprintf("the result file of the job is missing: %v", err)
	}

	if err := upload(jobId, result.ResultFilePath); err != nil {
		return job.StateFailed, fmt.Sprintf("failed to upload the result of the job: %v", err)
	}

	return job.StateFinished, result.Message
}
