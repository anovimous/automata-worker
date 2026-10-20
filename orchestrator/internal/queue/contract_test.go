package queue

import (
	"encoding/json"
	"os"
	"testing"
)

func TestMessageIsReadFromTheSampleOfTheCore(t *testing.T) {
	body, err := os.ReadFile("../../../contract/samples/queue-message.json")
	if err != nil {
		t.Fatal(err)
	}

	var msg Message
	if err := json.Unmarshal(body, &msg); err != nil {
		t.Fatalf("Unmarshal returned an error: %v", err)
	}

	if msg.JobId != 42 || msg.RoutineKey != "fuzz" {
		t.Errorf("msg = %+v, want job 42 and routine fuzz", msg)
	}
}
