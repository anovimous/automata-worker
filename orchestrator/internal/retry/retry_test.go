package retry

import (
	"errors"
	"testing"
	"time"
)

func policyRecordingSleeps(attempts int, sleeps *[]time.Duration) Policy {
	return Policy{
		Attempts:  attempts,
		BaseDelay: time.Second,
		MaxDelay:  3 * time.Second,
		Sleep:     func(d time.Duration) { *sleeps = append(*sleeps, d) },
	}
}

func TestDoReturnsAtOnceOnSuccess(t *testing.T) {
	var sleeps []time.Duration
	calls := 0

	err := policyRecordingSleeps(3, &sleeps).Do(func() error {
		calls++
		return nil
	})

	if err != nil || calls != 1 || len(sleeps) != 0 {
		t.Errorf("err=%v calls=%d sleeps=%v", err, calls, sleeps)
	}
}

func TestDoRetriesUntilItSucceeds(t *testing.T) {
	var sleeps []time.Duration
	calls := 0

	err := policyRecordingSleeps(5, &sleeps).Do(func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})

	if err != nil || calls != 3 {
		t.Errorf("err=%v calls=%d", err, calls)
	}
	if len(sleeps) != 2 || sleeps[0] != time.Second || sleeps[1] != 2*time.Second {
		t.Errorf("sleeps=%v", sleeps)
	}
}

func TestDoGivesUpAfterTheAttemptsAndKeepsTheLastError(t *testing.T) {
	var sleeps []time.Duration
	calls := 0
	last := errors.New("still down")

	err := policyRecordingSleeps(4, &sleeps).Do(func() error {
		calls++
		return last
	})

	if !errors.Is(err, last) {
		t.Errorf("err=%v does not wrap the last error", err)
	}
	if calls != 4 {
		t.Errorf("calls=%d, want 4", calls)
	}
	// no sleep after the last attempt, and the delay is capped at MaxDelay
	if len(sleeps) != 3 || sleeps[0] != time.Second || sleeps[1] != 2*time.Second || sleeps[2] != 3*time.Second {
		t.Errorf("sleeps=%v", sleeps)
	}
}

func TestDoStopsAtAPermanentError(t *testing.T) {
	var sleeps []time.Duration
	calls := 0
	rejected := errors.New("rejected")

	err := policyRecordingSleeps(5, &sleeps).Do(func() error {
		calls++
		return Permanent(rejected)
	})

	if err != rejected {
		t.Errorf("err=%v, want the unwrapped permanent error", err)
	}
	if calls != 1 || len(sleeps) != 0 {
		t.Errorf("calls=%d sleeps=%v", calls, sleeps)
	}
}

func TestPermanentOfNilIsNil(t *testing.T) {
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) must be nil")
	}
}
