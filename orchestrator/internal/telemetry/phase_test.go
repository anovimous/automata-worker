package telemetry

import (
	"testing"
	"time"
)

func TestPhaseBudget(t *testing.T) {
	tests := []struct {
		phase Phase
		want  time.Duration
	}{
		{PhaseStarting, 60 * time.Second},
		{PhaseIdle, 30 * time.Second},
		{PhasePreparing, 1500 * time.Second},
		{PhaseStartingJob, 60 * time.Second},
		{PhaseRunningJob, 30 * time.Second},
		{PhaseFinishing, 900 * time.Second},
		{PhaseDrained, 30 * time.Second},
		{PhaseShuttingDown, 90 * time.Second},
		{Phase("UNKNOWN"), 60 * time.Second},
	}
	for _, tt := range tests {
		t.Run(string(tt.phase), func(t *testing.T) {
			if got := PhaseBudget(tt.phase); got != tt.want {
				t.Errorf("PhaseBudget(%s) = %s, want %s", tt.phase, got, tt.want)
			}
		})
	}
}

// the wordlist step alone may take 20 minutes, a budget below would call a normal download stuck
func TestPreparingBudgetCoversTheWordlistDownload(t *testing.T) {
	if PhaseBudget(PhasePreparing) <= 20*time.Minute {
		t.Errorf("PREPARING budget = %s, want more than the 20 minutes of the wordlist download", PhaseBudget(PhasePreparing))
	}
}
