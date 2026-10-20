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
	jobId  int
	state  job.JobState
	called int
}

func (f *fakeInformer) Inform(jobId int, state job.JobState, message string) error {
	f.called++
	f.jobId, f.state = jobId, state
	return f.err
}

func TestInformPassesTheReportOn(t *testing.T) {
	f := &fakeInformer{}

	if err := inform(f, 4, job.StateRunning, ""); err != nil {
		t.Errorf("unexpected error %v", err)
	}
	if f.called != 1 || f.jobId != 4 || f.state != job.StateRunning {
		t.Errorf("informer=%+v", f)
	}
}

func TestInformReturnsTheFailureOfTheInformer(t *testing.T) {
	failure := errors.New("core is down")

	if err := inform(&fakeInformer{err: failure}, 4, job.StateRunning, ""); !errors.Is(err, failure) {
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

	state, message := completionReport(5, job.JobResult{FinishState: job.StateFinished, ResultFilePath: path, Message: "done"},
		func(id int, p string) error { uploadedId, uploadedPath = id, p; return nil })

	if state != job.StateFinished || message != "done" {
		t.Errorf("state=%s message=%q", state, message)
	}
	if uploadedId != 5 || uploadedPath != path {
		t.Errorf("uploaded %d %s", uploadedId, uploadedPath)
	}
}

func TestFinishedJobWhoseUploadFailsIsReportedFailed(t *testing.T) {
	state, message := completionReport(5, job.JobResult{FinishState: job.StateFinished, ResultFilePath: resultFile(t)},
		func(int, string) error { return errors.New("bucket is gone") })

	if state != job.StateFailed || !strings.Contains(message, "failed to upload") || !strings.Contains(message, "bucket is gone") {
		t.Errorf("state=%s message=%q", state, message)
	}
}

func TestFinishedJobWithoutAResultPathIsReportedFailed(t *testing.T) {
	state, message := completionReport(5, job.JobResult{FinishState: job.StateFinished}, neverUploaded(t))

	if state != job.StateFailed || !strings.Contains(message, "without a result file") {
		t.Errorf("state=%s message=%q", state, message)
	}
}

func TestFinishedJobWhoseResultFileIsMissingIsReportedFailed(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "gone.result")

	state, message := completionReport(5, job.JobResult{FinishState: job.StateFinished, ResultFilePath: missing}, neverUploaded(t))

	if state != job.StateFailed || !strings.Contains(message, "missing") {
		t.Errorf("state=%s message=%q", state, message)
	}
}

func TestFailedAndCanceledJobsUploadNothingAndKeepTheirState(t *testing.T) {
	for _, finish := range []job.JobState{job.StateFailed, job.StateCanceled} {
		state, message := completionReport(5, job.JobResult{FinishState: finish, Message: "why"}, neverUploaded(t))

		if state != finish || message != "why" {
			t.Errorf("%s: state=%s message=%q", finish, state, message)
		}
	}
}
