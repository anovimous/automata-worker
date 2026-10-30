package main

import (
	"context"
	log "log/slog"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
	"github.com/anovimous/automata-worker/orchestrator/internal/retry"
	"github.com/anovimous/automata-worker/orchestrator/internal/telemetry"
)

// what a job ended by a shutdown is reported with
const shutdownMessage = "the worker was shut down"

// the report of a job ended by a shutdown must fit in Docker's stop timeout (60 s by the monitor, at least 45): 5 s
// to end the tool, then at most about 20 s here
var shutdownReportPolicy = retry.Policy{Attempts: 3, BaseDelay: time.Second, MaxDelay: 5 * time.Second}

const shutdownReportTimeout = 5 * time.Second

// waker is what an idle wait needs of the lifecycle controller.
type waker interface {
	Wake() <-chan struct{}
}

// waitIdle waits d before the queue is looked at again, less when a drain or a shutdown is asked for meanwhile.
func waitIdle(w waker, d time.Duration) {
	select {
	case <-time.After(d):
	case <-w.Wake():
	}
}

// drainer is what shouldExit needs of the lifecycle controller.
type drainer interface {
	Draining() bool
}

// shouldExit tells, at the top of the main loop (no job), whether the worker has to exit, and the phase to show
// while it does.
func shouldExit(d drainer, shutdownCtx context.Context) (exit bool, phase telemetry.Phase) {
	if shutdownCtx.Err() != nil {
		return true, telemetry.PhaseShuttingDown
	}
	if d.Draining() {
		return true, telemetry.PhaseDrained
	}
	return false, ""
}

type requeuer interface {
	Requeue()
}

type jobEnder interface {
	JobEnded(outcome job.JobState)
}

// requeueOnShutdown puts the message back on the queue when the worker shuts down before the job started (RUNNING
// was not reported, so the message is not acked): another worker takes it, and nothing is reported to the core. It
// returns true when it did, and the message is then over.
func requeueOnShutdown(shutdownCtx context.Context, s requeuer, t jobEnder, jobId int) bool {
	if shutdownCtx.Err() == nil {
		return false
	}
	log.Info("shutting down before the job started, the message goes back to the queue", "jobId", jobId)
	s.Requeue()
	t.JobEnded(telemetry.OutcomeNotStarted)
	return true
}

type terminator interface {
	Terminate() error
}

// shutdownTracker is what shutDownJob tells the status.
type shutdownTracker interface {
	SetPhase(p telemetry.Phase, detail string)
	SetDetail(detail string)
	JobEnded(outcome job.JobState)
}

// shutDownJob ends a started job because the container is being stopped: the tool is ended and the core is told,
// which otherwise waits 30 minutes before it gives the job up (LOSTCONTACT).
func shutDownJob(t shutdownTracker, process terminator, informer stateInformer, jobId int) {
	log.Info("shutting down during the job, ending it", "jobId", jobId)
	t.SetPhase(telemetry.PhaseShuttingDown, "TERMINATING")
	if err := process.Terminate(); err != nil {
		log.Error("failed to end the job process", "jobId", jobId, "error", err)
	}
	t.SetDetail("REPORTING")
	inform(informer, jobId, job.StateSelfTerminated, shutdownMessage, job.FailureUnknown)
	t.JobEnded(job.StateSelfTerminated)
}
