package lifecycle

import (
	"context"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"
)

// recorder records the callbacks and the exits.
type recorder struct {
	mu        sync.Mutex
	drains    []bool
	shutdowns int
	exits     []int
}

func (r *recorder) onDrain(d bool) { r.mu.Lock(); defer r.mu.Unlock(); r.drains = append(r.drains, d) }

func (r *recorder) onShutdown() { r.mu.Lock(); defer r.mu.Unlock(); r.shutdowns++ }

func (r *recorder) exit(code int) { r.mu.Lock(); defer r.mu.Unlock(); r.exits = append(r.exits, code) }

// started is a controller fed by an unbuffered channel: a send returns once the controller took the signal.
func started(t *testing.T) (*Controller, chan os.Signal, *recorder) {
	t.Helper()
	sigs := make(chan os.Signal)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rec := &recorder{}
	return New(ctx, sigs, rec.exit, rec.onDrain, rec.onShutdown), sigs, rec
}

// settle sends one more signal that the controller ignores, so that the ones before are fully handled.
func settle(sigs chan os.Signal) {
	sigs <- syscall.SIGHUP
}

func TestDrainAndUndrain(t *testing.T) {
	c, sigs, rec := started(t)

	sigs <- syscall.SIGUSR1
	settle(sigs)
	if !c.Draining() {
		t.Fatal("not draining after SIGUSR1")
	}

	sigs <- syscall.SIGUSR2
	settle(sigs)
	if c.Draining() {
		t.Fatal("still draining after SIGUSR2")
	}

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if len(rec.drains) != 2 || !rec.drains[0] || rec.drains[1] {
		t.Errorf("onDrain calls = %v, want [true false]", rec.drains)
	}
}

func TestARepeatedDrainDoesNothing(t *testing.T) {
	c, sigs, rec := started(t)
	sigs <- syscall.SIGUSR1
	sigs <- syscall.SIGUSR1
	sigs <- syscall.SIGUSR2
	sigs <- syscall.SIGUSR2
	settle(sigs)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if c.Draining() || len(rec.drains) != 2 {
		t.Errorf("draining %v, onDrain calls %v, want one call each way", c.Draining(), rec.drains)
	}
}

func TestShutdownCancelsTheContext(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			c, sigs, rec := started(t)
			if c.ShutdownContext().Err() != nil {
				t.Fatal("the shutdown context is done before any signal")
			}

			sigs <- sig
			settle(sigs)

			if c.ShutdownContext().Err() == nil {
				t.Error("the shutdown context is not done")
			}
			rec.mu.Lock()
			defer rec.mu.Unlock()
			if rec.shutdowns != 1 || len(rec.exits) != 0 {
				t.Errorf("shutdowns %d, exits %v, want one shutdown and no exit", rec.shutdowns, rec.exits)
			}
		})
	}
}

func TestASecondShutdownSignalExitsWithOne(t *testing.T) {
	_, sigs, rec := started(t)
	sigs <- syscall.SIGTERM
	sigs <- syscall.SIGINT
	settle(sigs)

	rec.mu.Lock()
	defer rec.mu.Unlock()
	if rec.shutdowns != 1 || len(rec.exits) != 1 || rec.exits[0] != 1 {
		t.Errorf("shutdowns %d, exits %v, want one shutdown and exit(1)", rec.shutdowns, rec.exits)
	}
}

func TestUndrainIsIgnoredOnceShuttingDown(t *testing.T) {
	c, sigs, _ := started(t)
	sigs <- syscall.SIGUSR1
	sigs <- syscall.SIGTERM
	sigs <- syscall.SIGUSR2
	settle(sigs)

	if !c.Draining() {
		t.Error("the undrain was applied after the shutdown started")
	}
}

func TestWakeGetsAValueOnAChange(t *testing.T) {
	for _, sig := range []os.Signal{syscall.SIGUSR1, syscall.SIGTERM} {
		t.Run(sig.String(), func(t *testing.T) {
			c, sigs, _ := started(t)
			sigs <- sig
			select {
			case <-c.Wake():
			case <-time.After(time.Second):
				t.Fatal("no wake after the change")
			}
		})
	}
}

func TestWakeDoesNotBlockTheSignals(t *testing.T) {
	c, sigs, _ := started(t)
	// nobody reads Wake: the changes must still go through
	sigs <- syscall.SIGUSR1
	sigs <- syscall.SIGUSR2
	sigs <- syscall.SIGUSR1
	settle(sigs)

	if !c.Draining() {
		t.Error("not draining after USR1 USR2 USR1")
	}
}

func TestOtherSignalsAndNilCallbacksAreFine(t *testing.T) {
	sigs := make(chan os.Signal)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := New(ctx, sigs, func(int) { t.Error("exit called") }, nil, nil)

	sigs <- syscall.SIGHUP
	sigs <- syscall.SIGUSR1
	sigs <- syscall.SIGTERM
	settle(sigs)

	if !c.Draining() || c.ShutdownContext().Err() == nil {
		t.Error("the signals were not applied without callbacks")
	}
}

func TestTheControllerStopsWithItsContext(t *testing.T) {
	sigs := make(chan os.Signal)
	ctx, cancel := context.WithCancel(context.Background())
	New(ctx, sigs, func(int) {}, nil, nil)
	cancel()

	select {
	case sigs <- syscall.SIGUSR1:
		// the goroutine may take one more signal before it sees the context; a second one must block
		select {
		case sigs <- syscall.SIGUSR1:
			t.Error("the controller still reads signals after its context ended")
		case <-time.After(100 * time.Millisecond):
		}
	case <-time.After(100 * time.Millisecond):
	}
}
