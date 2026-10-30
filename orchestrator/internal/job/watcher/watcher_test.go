package watcher

import (
	"sync"
	"testing"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

// a watcher with a short timeout, so that the tests run quickly
func newTestWatcher(timeout time.Duration) *Watcher {
	return &Watcher{heartbeatTimeout: timeout}
}

func TestNewTakesTheTimeoutInSeconds(t *testing.T) {
	if got := New(60).heartbeatTimeout; got != time.Minute {
		t.Errorf("heartbeatTimeout = %s, want 1m", got)
	}
}

func TestHeartbeatsResetTheTimeout(t *testing.T) {
	msgs := make(chan job.JobMessage)
	_, _, timeout := newTestWatcher(150*time.Millisecond).Watch(msgs, nil)

	for range 5 {
		time.Sleep(60 * time.Millisecond)
		msgs <- job.JobMessage{Typ: job.MessageHeartbeat}
	}
	select {
	case <-timeout:
		t.Fatal("timed out although heartbeats arrived")
	default:
	}

	select {
	case <-timeout:
	case <-time.After(time.Second):
		t.Fatal("no timeout after the heartbeats stopped")
	}
}

func TestStateUpdatesAreForwarded(t *testing.T) {
	msgs := make(chan job.JobMessage)
	_, states, _ := newTestWatcher(time.Second).Watch(msgs, nil)

	msgs <- job.JobMessage{Typ: job.MessageStateUpdate, State: job.StatePaused}
	select {
	case s := <-states:
		if s != job.StatePaused {
			t.Errorf("state = %s, want PAUSED", s)
		}
	case <-time.After(time.Second):
		t.Fatal("the state update was not forwarded")
	}
}

func TestTheFinishIsForwardedAndEndsTheWatch(t *testing.T) {
	msgs := make(chan job.JobMessage, 1)
	completion, _, timeout := newTestWatcher(100*time.Millisecond).Watch(msgs, nil)

	msgs <- job.JobMessage{Typ: job.MessageFinishResult, Result: job.JobResult{FinishState: job.StateFinished, ResultFilePath: "/tmp/r"}}
	select {
	case r := <-completion:
		if r.FinishState != job.StateFinished || r.ResultFilePath != "/tmp/r" {
			t.Errorf("result = %+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("the finish was not forwarded")
	}

	select {
	case <-timeout:
		t.Error("timed out after the finish, the watch should have ended")
	case <-time.After(300 * time.Millisecond):
	}
}

func TestAClosedConnectionIsATimeout(t *testing.T) {
	msgs := make(chan job.JobMessage)
	_, _, timeout := newTestWatcher(time.Minute).Watch(msgs, nil)
	close(msgs)

	select {
	case <-timeout:
	case <-time.After(time.Second):
		t.Fatal("no timeout after the connection closed")
	}
}

func TestOnMessageSeesEveryMessage(t *testing.T) {
	var mu sync.Mutex
	var seen []job.MessageType
	onMessage := func(m job.JobMessage) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, m.Typ)
	}

	msgs := make(chan job.JobMessage)
	completion, states, _ := newTestWatcher(time.Second).Watch(msgs, onMessage)
	msgs <- job.JobMessage{Typ: job.MessageHeartbeat}
	msgs <- job.JobMessage{Typ: job.MessageStateUpdate, State: job.StateRunning}
	<-states
	msgs <- job.JobMessage{Typ: job.MessageFinishResult}
	<-completion

	mu.Lock()
	defer mu.Unlock()
	want := []job.MessageType{job.MessageHeartbeat, job.MessageStateUpdate, job.MessageFinishResult}
	if len(seen) != len(want) {
		t.Fatalf("seen = %v, want %v", seen, want)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Errorf("seen = %v, want %v", seen, want)
		}
	}
}
