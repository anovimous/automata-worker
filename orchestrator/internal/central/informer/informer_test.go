package informer

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
	"github.com/anovimous/automata-worker/orchestrator/internal/retry"
)

func informerFor(server *httptest.Server) *Informer {
	i := New(server.URL, "/update")
	i.retry = retry.Policy{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, Sleep: func(time.Duration) {}}
	return i
}

func TestInformPostsTheStateToTheJobEndpoint(t *testing.T) {
	var gotPath string
	var got informMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		json.NewDecoder(r.Body).Decode(&got)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	err := informerFor(server).Inform(7, job.StateFailed, "boom")

	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}
	if gotPath != "/update/7" || got.JobId != 7 || got.State != job.StateFailed || got.Msg != "boom" {
		t.Errorf("path=%s message=%+v", gotPath, got)
	}
}

func TestInformRetriesServerErrorsUntilItSucceeds(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	err := informerFor(server).Inform(1, job.StateRunning, "")

	if err != nil || calls != 3 {
		t.Errorf("err=%v calls=%d", err, calls)
	}
}

func TestInformGivesUpWhenTheServerKeepsFailing(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	err := informerFor(server).Inform(1, job.StateRunning, "")

	if err == nil || calls != 3 {
		t.Errorf("err=%v calls=%d", err, calls)
	}
}

func TestInformDoesNotRetryARejectedReport(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte("A worker can't report the state RUNNING for a job which is RUNNING"))
	}))
	defer server.Close()

	err := informerFor(server).Inform(1, job.StateRunning, "")

	if calls != 1 {
		t.Errorf("calls=%d, want 1", calls)
	}
	if err == nil || !strings.Contains(err.Error(), "409") || !strings.Contains(err.Error(), "can't report the state RUNNING") {
		t.Errorf("err=%v, want the status and the answer of the central service", err)
	}
}

func TestInformRetriesWhenTheServerIsUnreachable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := server.URL
	server.Close()

	i := New(url, "/update")
	attempts := 0
	i.retry = retry.Policy{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond,
		Sleep: func(time.Duration) { attempts++ }}

	err := i.Inform(1, job.StateRunning, "")

	if err == nil || attempts != 2 {
		t.Errorf("err=%v waits=%d, want 2 waits between 3 attempts", err, attempts)
	}
}

func TestInformTimesOutOnAHungServer(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer server.Close()
	defer close(release)

	i := informerFor(server)
	i.client.Timeout = 50 * time.Millisecond
	i.retry.Attempts = 1

	start := time.Now()
	err := i.Inform(1, job.StateRunning, "")

	if err == nil || time.Since(start) > 2*time.Second {
		t.Errorf("err=%v took %v, want a timeout error", err, time.Since(start))
	}
}
