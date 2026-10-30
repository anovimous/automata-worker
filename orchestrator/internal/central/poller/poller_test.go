package poller

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

func pollerFor(server *httptest.Server) *Poller {
	p := New(server.URL, "/poll", 1)
	p.interval = 5 * time.Millisecond
	return p
}

func serverAnswering(state *atomic.Value) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"requestedState":"` + state.Load().(string) + `"}`))
	}))
}

func next(t *testing.T, ch <-chan PollInfo) PollInfo {
	t.Helper()
	select {
	case info := <-ch:
		return info
	case <-time.After(2 * time.Second):
		t.Fatal("no state was sent")
		return PollInfo{}
	}
}

func TestSinglePollReadsTheDesiredState(t *testing.T) {
	var state atomic.Value
	state.Store("PAUSED")
	server := serverAnswering(&state)
	defer server.Close()

	info, err := pollerFor(server).SinglePoll(3)

	if err != nil || info.DesiredState != job.StatePaused {
		t.Errorf("info=%+v err=%v", info, err)
	}
}

func TestSinglePollFailsOnAnErrorStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	if _, err := pollerFor(server).SinglePoll(3); err == nil {
		t.Error("expected an error")
	}
}

func TestSinglePollTimesOutOnAHungServer(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
	}))
	defer server.Close()
	defer close(release)

	p := pollerFor(server)
	p.client.Timeout = 50 * time.Millisecond

	start := time.Now()
	_, err := p.SinglePoll(3)

	if err == nil || time.Since(start) > 2*time.Second {
		t.Errorf("err=%v took %v, want a timeout error", err, time.Since(start))
	}
}

func TestStartSendsEachChangeOnlyOnce(t *testing.T) {
	var state atomic.Value
	state.Store("PAUSED")
	server := serverAnswering(&state)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, _ := pollerFor(server).Start(3, ctx)

	if info := next(t, ch); info.DesiredState != job.StatePaused {
		t.Errorf("info=%+v", info)
	}
	select {
	case info := <-ch:
		t.Errorf("the same state was sent again: %+v", info)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestStartSendsTheStateAgainAfterItWasForgotten(t *testing.T) {
	var state atomic.Value
	state.Store("PAUSED")
	server := serverAnswering(&state)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, forget := pollerFor(server).Start(3, ctx)

	next(t, ch)
	forget()

	if info := next(t, ch); info.DesiredState != job.StatePaused {
		t.Errorf("info=%+v, want the same state sent again", info)
	}
}

func TestStartDoesNotSendTheStateTheJobAlreadyHas(t *testing.T) {
	var state atomic.Value
	state.Store("RUNNING")
	server := serverAnswering(&state)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, _ := pollerFor(server).Start(3, ctx)

	select {
	case info := <-ch:
		t.Errorf("unexpected state %+v", info)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestStartGivesEveryPollResultToTheObserver(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// the first poll fails, the others succeed
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte(`{"requestedState":"RUNNING"}`))
	}))
	defer server.Close()

	results := make(chan error, 16)
	p := pollerFor(server)
	p.Observe(func(err error) {
		select {
		case results <- err:
		default:
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(3, ctx)

	first, second := <-results, <-results
	if first == nil {
		t.Error("the failed poll was given to the observer as a success")
	}
	if second != nil {
		t.Errorf("the successful poll was given as %v", second)
	}
}

func TestStartWorksWithoutAnObserver(t *testing.T) {
	var state atomic.Value
	state.Store("PAUSED")
	server := serverAnswering(&state)
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, _ := pollerFor(server).Start(3, ctx)

	if info := next(t, ch); info.DesiredState != job.StatePaused {
		t.Errorf("info = %+v, want PAUSED", info)
	}
}
