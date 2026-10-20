package poller

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

func TestPollInfoIsReadFromTheSampleOfTheCore(t *testing.T) {
	body, err := os.ReadFile("../../../../contract/samples/poll-response.json")
	if err != nil {
		t.Fatal(err)
	}

	var info PollInfo
	if err := json.Unmarshal(body, &info); err != nil {
		t.Fatalf("Unmarshal returned an error: %v", err)
	}

	if info.DesiredState != job.StatePaused {
		t.Errorf("DesiredState = %q, want PAUSED", info.DesiredState)
	}
}
