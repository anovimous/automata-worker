package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewLoggerWritesJSONWithTheWorkerID(t *testing.T) {
	var out bytes.Buffer
	newLogger("json", "w-host1-k3f9qa", &out).Info("job started", "jobId", 4211)

	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatalf("the record is not JSON: %q", out.String())
	}
	if record["workerId"] != "w-host1-k3f9qa" || record["msg"] != "job started" || record["jobId"] != float64(4211) {
		t.Errorf("record = %v", record)
	}
}

func TestNewLoggerWritesTextWithTheWorkerID(t *testing.T) {
	var out bytes.Buffer
	newLogger("text", "w-host1-k3f9qa", &out).Info("job started")

	if line := out.String(); !strings.Contains(line, "workerId=w-host1-k3f9qa") || strings.HasPrefix(line, "{") {
		t.Errorf("line = %q, want text carrying workerId", line)
	}
}
