package main

import (
	"testing"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

func TestDecideInitialPoll(t *testing.T) {
	tests := []struct {
		name        string
		desired     job.JobState
		wantStart   bool
		wantReport  job.JobState
		wantMessage bool
	}{
		{"a queued job is started", job.StateQueued, true, "", false},
		{"a job canceled while queued is reported canceled", job.StateCanceled, false, job.StateCanceled, true},
		{"a running job is a duplicate and is skipped", job.StateRunning, false, "", false},
		{"a paused job is skipped", job.StatePaused, false, "", false},
		{"a finished job is skipped", job.StateFinished, false, "", false},
		{"a failed job is skipped", job.StateFailed, false, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, report, message := decideInitialPoll(tt.desired)

			if start != tt.wantStart {
				t.Errorf("start = %v, want %v", start, tt.wantStart)
			}
			if report != tt.wantReport {
				t.Errorf("report = %q, want %q", report, tt.wantReport)
			}
			if (message != "") != tt.wantMessage {
				t.Errorf("message = %q, want a message: %v", message, tt.wantMessage)
			}
		})
	}
}

func TestStopsTheJob(t *testing.T) {
	tests := []struct {
		desired job.JobState
		want    bool
	}{
		{job.StateLostContact, true},
		{job.StateRunning, false},
		{job.StatePaused, false},
		{job.StateCanceled, false},
		{job.StateSelfTerminated, false},
		{job.StateFinished, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.desired), func(t *testing.T) {
			if got := stopsTheJob(tt.desired); got != tt.want {
				t.Errorf("stopsTheJob(%s) = %v, want %v", tt.desired, got, tt.want)
			}
		})
	}
}
