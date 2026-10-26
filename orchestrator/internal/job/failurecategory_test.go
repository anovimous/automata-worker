package job

import (
	"encoding/json"
	"testing"
)

// The names are the ones of FailureCategory in the core, which reads any other as UNKNOWN
func TestFailureCategoriesAreTheNamesOfTheContract(t *testing.T) {
	want := map[FailureCategory]string{
		FailurePreparation:   "PREPARATION",
		FailurePoll:          "POLL",
		FailureInvalidConfig: "INVALID_CONFIG",
		FailureToolStart:     "TOOL_START",
		FailureToolExit:      "TOOL_EXIT",
		FailureResult:        "RESULT",
		FailureUnknown:       "UNKNOWN",
	}

	for category, name := range want {
		if string(category) != name {
			t.Errorf("category %q, want %q", category, name)
		}
	}
	if len(want) != 7 {
		t.Errorf("%d categories, the contract has 7", len(want))
	}
}

func TestTheFinishMessageOfAConnectorCarriesItsCategory(t *testing.T) {
	line := `{"type":"finish","result":{"resultPath":"","message":"tool exited with error: exit status 1","finishState":"FAILED","failureCategory":"TOOL_EXIT"}}`

	var message JobMessage
	if err := json.Unmarshal([]byte(line), &message); err != nil {
		t.Fatal(err)
	}

	if message.Result.FailureCategory != FailureToolExit || message.Result.FinishState != StateFailed {
		t.Errorf("result = %+v", message.Result)
	}
}

func TestTheFinishMessageOfAnOlderConnectorHasNoCategory(t *testing.T) {
	line := `{"type":"finish","result":{"resultPath":"","message":"boom","finishState":"FAILED"}}`

	var message JobMessage
	if err := json.Unmarshal([]byte(line), &message); err != nil {
		t.Fatal(err)
	}

	if message.Result.FailureCategory != "" {
		t.Errorf("category = %q, want none", message.Result.FailureCategory)
	}
}
