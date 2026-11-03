package telemetry

import "time"

// Phase is what the main loop is doing (docs/telemetry-contract.md §3.2).
type Phase string

const (
	PhaseStarting     Phase = "STARTING"
	PhaseIdle         Phase = "IDLE"
	PhasePreparing    Phase = "PREPARING"
	PhaseStartingJob  Phase = "STARTING_JOB"
	PhaseRunningJob   Phase = "RUNNING_JOB"
	PhaseFinishing    Phase = "FINISHING"
	PhaseDrained      Phase = "DRAINED"
	PhaseShuttingDown Phase = "SHUTTING_DOWN"
)

// the budgets follow the timeouts of each phase: the wordlists may take 20 minutes, the job loop ticks every 5 s,
// the result upload and the final report retry for several minutes
var phaseBudgets = map[Phase]time.Duration{
	PhaseStarting:     60 * time.Second,
	PhaseIdle:         30 * time.Second,
	PhasePreparing:    1500 * time.Second,
	PhaseStartingJob:  60 * time.Second,
	PhaseRunningJob:   30 * time.Second,
	PhaseFinishing:    900 * time.Second,
	PhaseDrained:      30 * time.Second,
	PhaseShuttingDown: 90 * time.Second,
}

// PhaseBudget is how long the main loop may go without progress in a phase before it counts as stuck. The
// monitor reads it from the status and never hard-codes it, so this is the one place to tune it.
func PhaseBudget(p Phase) time.Duration {
	if b, ok := phaseBudgets[p]; ok {
		return b
	}
	return phaseBudgets[PhaseStarting]
}
