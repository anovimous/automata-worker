package telemetry

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// The samples in contract/telemetry are the worker's own; the monitor keeps an equal copy and parses it. A renamed
// or dropped field fails here or there.

func readSample(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("../../../contract/telemetry/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestSamplesHaveNoFieldThatStatusLacks(t *testing.T) {
	for _, name := range []string{"worker-status.v1.running.json", "worker-status.v1.idle.json"} {
		t.Run(name, func(t *testing.T) {
			dec := json.NewDecoder(bytes.NewReader(readSample(t, name)))
			dec.DisallowUnknownFields()
			var s Status
			if err := dec.Decode(&s); err != nil {
				t.Errorf("decoding the sample: %v", err)
			}
		})
	}
}

func at(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

func ptr[T any](v T) *T { return &v }

// the running sample, built from the types
func runningStatus() Status {
	return Status{
		SchemaVersion:      1,
		WorkerID:           "w-host1-k3f9qa",
		Version:            "0.4.0",
		Commit:             "1a4c23f",
		GoVersion:          "go1.26.2",
		PID:                7,
		StartedAt:          at("2026-10-29T09:58:02Z"),
		Now:                at("2026-10-29T10:42:13Z"),
		Phase:              PhaseRunningJob,
		PhaseDetail:        "",
		PhaseSince:         at("2026-10-29T10:30:41Z"),
		PhaseBudgetSeconds: 30,
		LastProgressAt:     at("2026-10-29T10:42:11Z"),
		Job: &Job{
			JobID:      4211,
			RoutineKey: "fuzz",
			Command:    "ffuf-connector",
			State:      "RUNNING",
			TakenAt:    at("2026-10-29T10:28:55Z"),
			StartedAt:  ptr(at("2026-10-29T10:30:40Z")),
			Connector: &Connector{
				Connected:                true,
				LastMessageAt:            ptr(at("2026-10-29T10:42:10Z")),
				LastHeartbeatAt:          ptr(at("2026-10-29T10:42:10Z")),
				HeartbeatIntervalSeconds: 5,
				HeartbeatTimeoutSeconds:  60,
			},
			Resources: &JobResources{
				SampledAt: at("2026-10-29T10:42:13Z"),
				Job:       &CgroupUsage{CPUUsageSeconds: 812.41},
				Tool:      &CgroupUsage{CPUUsageSeconds: 803.02},
			},
		},
		Dependencies: Dependencies{
			RabbitMQ: DependencyStatus{Up: ptr(true), LastSuccessAt: ptr(at("2026-10-29T10:28:55Z"))},
			Core: DependencyStatus{
				LastSuccessAt: ptr(at("2026-10-29T10:42:05Z")),
				LastErrorAt:   ptr(at("2026-10-29T10:12:00Z")),
				LastError:     `failed to poll central service for job 4188: Get "http://***@localhost:8080/api/external/jobs/http/poll/4188": dial tcp 127.0.0.1:8080: connect: connection refused`,
				ErrorsTotal:   3,
			},
			ObjectStorage: DependencyStatus{LastSuccessAt: ptr(at("2026-10-29T10:29:31Z"))},
		},
		WordlistCache: &WordlistCache{Dir: "/var/cache/automata/wordlists", Bytes: 734003200, Files: 4, MaxBytes: 1610612736, PinnedFiles: 1},
		Counters: Counters{
			MessagesTaken:   37,
			MessagesSkipped: 2,
			JobsStarted:     34,
			JobsByOutcome:   map[string]int{"FINISHED": 30, "FAILED": 2, "CANCELED": 1, "NOT_STARTED": 2},
		},
	}
}

func TestStatusIsWrittenAsTheRunningSample(t *testing.T) {
	got, err := json.Marshal(runningStatus())
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(t, got, readSample(t, "worker-status.v1.running.json")) {
		t.Errorf("the status differs from the sample:\n%s", got)
	}
}

// the sample's error was written by Sanitize
func TestTheSampleErrorIsSanitized(t *testing.T) {
	e := runningStatus().Dependencies.Core.LastError
	if got := Sanitize(e); got != e {
		t.Errorf("Sanitize changed the sample error:\n got  %q\n want %q", got, e)
	}
}

// what the tracker writes for an idle worker that has run nothing, with the times of the idle sample
func TestTrackerOfAnIdleWorkerIsWrittenAsTheIdleSample(t *testing.T) {
	clock := &fakeClock{now: at("2026-10-29T10:40:00Z")}
	tr := NewTracker("w-host1-p0m2zt", BuildInfo{Version: "0.4.0", Commit: "1a4c23f", GoVersion: "go1.26.2"}, nil, clock.Now)
	clock.Advance(time.Second)
	tr.SetPhase(PhaseIdle, "")
	clock.Advance(131 * time.Second)
	tr.DependencyResult(DepRabbitMQ, nil)
	tr.Touch()
	clock.Advance(time.Second)

	s := tr.Snapshot()
	s.PID = 7 // the sample's pid, the test's own differs
	got, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(t, got, readSample(t, "worker-status.v1.idle.json")) {
		t.Errorf("the status differs from the idle sample:\n%s", got)
	}
}
