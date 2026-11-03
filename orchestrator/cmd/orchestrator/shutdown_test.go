package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
	"github.com/anovimous/automata-worker/orchestrator/internal/telemetry"
)

type fakeWaker struct {
	ch chan struct{}
}

func (f fakeWaker) Wake() <-chan struct{} { return f.ch }

func TestWaitIdleWaitsTheWholeTime(t *testing.T) {
	start := time.Now()
	waitIdle(fakeWaker{ch: make(chan struct{})}, 50*time.Millisecond)

	if took := time.Since(start); took < 50*time.Millisecond {
		t.Errorf("waited %s, want 50ms", took)
	}
}

func TestWaitIdleEndsOnAWake(t *testing.T) {
	w := fakeWaker{ch: make(chan struct{}, 1)}
	w.ch <- struct{}{}

	start := time.Now()
	waitIdle(w, time.Minute)

	if took := time.Since(start); took > time.Second {
		t.Errorf("waited %s after a wake, want it to end at once", took)
	}
}

type fakeDrainer bool

func (f fakeDrainer) Draining() bool { return bool(f) }

func TestShouldExit(t *testing.T) {
	done, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name      string
		draining  bool
		ctx       context.Context
		wantExit  bool
		wantPhase telemetry.Phase
	}{
		{"neither", false, context.Background(), false, ""},
		{"draining", true, context.Background(), true, telemetry.PhaseDrained},
		{"shutting down", false, done, true, telemetry.PhaseShuttingDown},
		{"both: the shutdown wins", true, done, true, telemetry.PhaseShuttingDown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			exit, phase := shouldExit(fakeDrainer(tt.draining), tt.ctx)
			if exit != tt.wantExit || phase != tt.wantPhase {
				t.Errorf("shouldExit = %v, %q, want %v, %q", exit, phase, tt.wantExit, tt.wantPhase)
			}
		})
	}
}

type fakeRequeuer struct{ requeued int }

func (f *fakeRequeuer) Requeue() { f.requeued++ }

// fakeTracker records what the shutdown helpers tell the status.
type fakeTracker struct {
	phases  []telemetry.Phase
	details []string
	ended   []job.JobState
}

func (f *fakeTracker) SetPhase(p telemetry.Phase, detail string) {
	f.phases = append(f.phases, p)
	f.details = append(f.details, detail)
}

func (f *fakeTracker) SetDetail(detail string) { f.details = append(f.details, detail) }

func (f *fakeTracker) JobEnded(outcome job.JobState) { f.ended = append(f.ended, outcome) }

func TestRequeueOnShutdownDoesNothingWhileRunning(t *testing.T) {
	s, tr := &fakeRequeuer{}, &fakeTracker{}

	if requeueOnShutdown(context.Background(), s, tr, 7) {
		t.Error("requeueOnShutdown returned true without a shutdown")
	}
	if s.requeued != 0 || len(tr.ended) != 0 {
		t.Errorf("requeued %d, ended %v, want nothing", s.requeued, tr.ended)
	}
}

func TestRequeueOnShutdownPutsTheMessageBack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, tr := &fakeRequeuer{}, &fakeTracker{}

	if !requeueOnShutdown(ctx, s, tr, 7) {
		t.Error("requeueOnShutdown returned false during a shutdown")
	}
	if s.requeued != 1 || len(tr.ended) != 1 || tr.ended[0] != telemetry.OutcomeNotStarted {
		t.Errorf("requeued %d, ended %v, want one requeue counted NOT_STARTED", s.requeued, tr.ended)
	}
}

type fakeProcess struct {
	terminated int
	err        error
}

func (f *fakeProcess) Terminate() error { f.terminated++; return f.err }

func TestShutDownJobEndsTheToolAndReportsSelfTerminated(t *testing.T) {
	for _, terminateErr := range []error{nil, errors.New("cgroup gone")} {
		tr, p, informer := &fakeTracker{}, &fakeProcess{err: terminateErr}, &fakeInformer{}

		shutDownJob(tr, p, informer, 42)

		if p.terminated != 1 {
			t.Errorf("terminated %d times, want 1", p.terminated)
		}
		if informer.called != 1 || informer.jobId != 42 || informer.state != job.StateSelfTerminated || informer.category != job.FailureUnknown {
			t.Errorf("report = %+v, want SELFTERMINATED/UNKNOWN for job 42", informer)
		}
		if len(tr.phases) != 1 || tr.phases[0] != telemetry.PhaseShuttingDown ||
			len(tr.details) != 2 || tr.details[0] != "TERMINATING" || tr.details[1] != "REPORTING" {
			t.Errorf("status = phases %v details %v, want SHUTTING_DOWN, TERMINATING then REPORTING", tr.phases, tr.details)
		}
		if len(tr.ended) != 1 || tr.ended[0] != job.StateSelfTerminated {
			t.Errorf("ended = %v, want SELFTERMINATED", tr.ended)
		}
	}
}

// the core must learn why the job ended, in the words of the contract
func TestShutDownJobReportsTheShutdownMessage(t *testing.T) {
	informer := &messageInformer{}
	shutDownJob(&fakeTracker{}, &fakeProcess{}, informer, 1)

	if informer.message != "the worker was shut down" {
		t.Errorf("message = %q", informer.message)
	}
}

type messageInformer struct{ message string }

func (m *messageInformer) Inform(jobId int, state job.JobState, message string, category job.FailureCategory) error {
	m.message = message
	return nil
}

// 3 attempts with 5 s requests and 1 s + 2 s between them fit in the room left by Docker's stop timeout
func TestTheShutdownReportFitsInTheStopTimeout(t *testing.T) {
	worst := time.Duration(shutdownReportPolicy.Attempts) * shutdownReportTimeout
	delay := shutdownReportPolicy.BaseDelay
	for i := 1; i < shutdownReportPolicy.Attempts; i++ {
		worst += min(delay, shutdownReportPolicy.MaxDelay)
		delay *= 2
	}
	if worst > 20*time.Second {
		t.Errorf("the shutdown report may take %s, want at most 20s", worst)
	}
}
