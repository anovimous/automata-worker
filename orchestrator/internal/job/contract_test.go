package job

import (
	"encoding/json"
	"os"
	"slices"
	"testing"
)

// The worker knows only some of the states of the core, the ones it reports
// or has to act on. Each of them must be a state which the core has.
func TestEveryStateOfTheWorkerIsAStateOfTheCore(t *testing.T) {
	body, err := os.ReadFile("../../../contract/samples/job-states.json")
	if err != nil {
		t.Fatal(err)
	}
	var coreStates []string
	if err := json.Unmarshal(body, &coreStates); err != nil {
		t.Fatal(err)
	}

	for _, s := range []JobState{StateQueued, StateRunning, StatePaused, StateCanceled, StateFinished, StateFailed, StateSelfTerminated, StateLostContact} {
		if !slices.Contains(coreStates, string(s)) {
			t.Errorf("state %q is not a state of the core %v", s, coreStates)
		}
	}
}
