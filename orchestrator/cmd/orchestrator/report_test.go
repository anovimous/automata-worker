package main

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

type fakeInformer struct {
	err    error
	jobId    int
	state    job.JobState
	category job.FailureCategory
	called   int
}

func (f *fakeInformer) Inform(jobId int, state job.JobState, message string, category job.FailureCategory) error {
	f.called++
	f.jobId, f.state, f.category = jobId, state, category
	return f.err
}

func TestInformPassesTheReportOn(t *testing.T) {
	f := &fakeInformer{}

	if err := inform(f, 4, job.StateFailed, "boom", job.FailureToolExit); err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if f.called != 1 || f.jobId != 4 || f.state != job.StateFailed || f.category != job.FailureToolExit {
		t.Errorf("informer=%+v", f)
	}
}

func TestInformReturnsTheFailureOfTheInformer(t *testing.T) {
	failure := errors.New("core is down")

	if err := inform(&fakeInformer{err: failure}, 4, job.StateRunning, "", ""); !errors.Is(err, failure) {
		t.Errorf("err=%v, want %v", err, failure)
	}
}

func resultFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "1.result")
	if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func neverUploaded(t *testing.T) func(int, string) error {
	return func(int, string) error {
		t.Error("nothing should be uploaded")
		return nil
	}
}

func TestFinishedJobIsUploadedAndReportedFinished(t *testing.T) {
	path := resultFile(t)
	var uploadedId int
	var uploadedPath string

	state, message, category := completionReport(5, job.JobResult{FinishState: job.StateFinished, ResultFilePath: path, Message: "done"},
		func(id int, p string) error { uploadedId, uploadedPath = id, p; return nil })

	if state != job.StateFinished || message != "done" || category != "" {
		t.Errorf("state=%s message=%q category=%q", state, message, category)
	}
	if uploadedId != 5 || uploadedPath != path {
		t.Errorf("uploaded %d %s", uploadedId, uploadedPath)
	}
}

func TestFinishedJobWhoseUploadFailsIsReportedFailed(t *testing.T) {
	state, message, category := completionReport(5, job.JobResult{FinishState: job.StateFinished, ResultFilePath: resultFile(t)},
		func(int, string) error { return errors.New("bucket is gone") })

	if state != job.StateFailed || !strings.Contains(message, "failed to upload") || !strings.Contains(message, "bucket is gone") || category != job.FailureResult {
		t.Errorf("state=%s message=%q category=%q", state, message, category)
	}
}

func TestFinishedJobWithoutAResultPathIsReportedFailed(t *testing.T) {
	state, message, category := completionReport(5, job.JobResult{FinishState: job.StateFinished}, neverUploaded(t))

	if state != job.StateFailed || !strings.Contains(message, "without a result file") || category != job.FailureResult {
		t.Errorf("state=%s message=%q category=%q", state, message, category)
	}
}

func TestFinishedJobWhoseResultFileIsMissingIsReportedFailed(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone.result")

	state, message, category := completionReport(5, job.JobResult{FinishState: job.StateFinished, ResultFilePath: missing}, neverUploaded(t))

	if state != job.StateFailed || !strings.Contains(message, "missing") || category != job.FailureResult {
		t.Errorf("state=%s message=%q category=%q", state, message, category)
	}
}

func TestFailedAndCanceledJobsUploadNothingAndKeepTheirState(t *testing.T) {
	for _, finish := range []job.JobState{job.StateFailed, job.StateCanceled} {
		state, message, category := completionReport(5, job.JobResult{FinishState: finish, Message: "why", FailureCategory: job.FailureToolExit}, neverUploaded(t))

		if state != finish || message != "why" || category != job.FailureToolExit {
			t.Errorf("%s: state=%s message=%q category=%q", finish, state, message, category)
		}
	}
}

func TestAckOnceAcksOnlyTheFirstTime(t *testing.T) {
	calls := 0
	ack := ackOnce(func() error { calls++; return nil })

	ack()
	ack()
	ack()

	if calls != 1 {
		t.Errorf("acked %d times, want 1", calls)
	}
}

func TestAckOnceDoesNotTryAgainAfterAFailure(t *testing.T) {
	calls := 0
	ack := ackOnce(func() error { calls++; return errors.New("channel closed") })

	ack()
	ack()

	if calls != 1 {
		t.Errorf("tried %d times, want 1", calls)
	}
}

func TestAckOnceDoesNothingUntilCalled(t *testing.T) {
	calls := 0
	ackOnce(func() error { calls++; return nil })

	if calls != 0 {
		t.Errorf("acked %d times before the call", calls)
	}
}
