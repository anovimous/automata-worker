package runner

import (
	"reflect"
	"testing"

	"github.com/anovimous/automata-worker/orchestrator/internal/config"
	"github.com/anovimous/automata-worker/orchestrator/internal/job/socket"
)

func TestConnectorArgsUseTheWorkDirectoryOfTheJob(t *testing.T) {
	r := New(config.WorkerConfig{HeartbeatInterval: 5})
	read := &socket.Socket{Path: "/s/read"}
	write := &socket.Socket{Path: "/s/write"}

	got := r.connectorArgs(7, "/tmp/job-7", "/tmp/job-7/details-7.json", "/tmp/job-7/data-7.jsonl", read, write)

	want := []string{
		"--jobId", "7",
		"--details", "/tmp/job-7/details-7.json",
		"--data", "/tmp/job-7/data-7.jsonl",
		"--readSocket", "/s/write",
		"--writeSocket", "/s/read",
		"--heartbeatInterval", "5",
		"--tmpDir", "/tmp/job-7",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("args = %v, want %v", got, want)
	}
}
