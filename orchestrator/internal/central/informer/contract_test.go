package informer

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

func TestReportIsWhatTheCoreExpects(t *testing.T) {
	sample, err := os.ReadFile("../../../../contract/samples/update-request.json")
	if err != nil {
		t.Fatal(err)
	}
	written, err := json.Marshal(informMessage{JobId: 42, State: job.StateFailed, Msg: "failed to start the tool"})
	if err != nil {
		t.Fatal(err)
	}

	var want, got map[string]any
	if err := json.Unmarshal(sample, &want); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(written, &got); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("report = %s, want %s", written, sample)
	}
}
