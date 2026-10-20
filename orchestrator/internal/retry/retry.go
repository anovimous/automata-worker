package retry

import (
	"errors"
	"fmt"
	"time"
)

// Policy retries an operation with an exponential backoff.
type Policy struct {
	Attempts  int
	BaseDelay time.Duration
	MaxDelay  time.Duration

	// Sleep waits between two attempts. It is time.Sleep when nil.
	Sleep func(time.Duration)
}

type permanentError struct {
	err error
}

func (e permanentError) Error() string { return e.err.Error() }

func (e permanentError) Unwrap() error { return e.err }

// Permanent marks an error as one which a retry can't fix, so Do stops at it.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err: err}
}

// Do calls fn until it succeeds, returns a Permanent error or the attempts are
// used up. The delay between attempts starts at BaseDelay and doubles up to
// MaxDelay.
func (p Policy) Do(fn func() error) error {
	sleep := p.Sleep
	if sleep == nil {
		sleep = time.Sleep
	}

	delay := p.BaseDelay
	var err error

	for attempt := 1; attempt <= p.Attempts; attempt++ {
		err = fn()
		if err == nil {
			return nil
		}

		var permanent permanentError
		if errors.As(err, &permanent) {
			return permanent.err
		}

		if attempt == p.Attempts {
			break
		}

		sleep(delay)
		delay *= 2
		if delay > p.MaxDelay {
			delay = p.MaxDelay
		}
	}

	return fmt.Errorf("giving up after %d attempts: %w", p.Attempts, err)
}
