// Package lifecycle turns the signals the container gets into the worker's lifecycle (docs/telemetry-contract.md
// §5): SIGUSR1 drains (finish the current job, take no new one, exit), SIGUSR2 undoes a drain, SIGTERM and SIGINT
// shut down now, and a second shutdown signal exits at once.
package lifecycle

import (
	"context"
	log "log/slog"
	"os"
	"sync"
	"syscall"
)

// Controller holds the drain and shutdown state. Its methods are safe for concurrent use.
type Controller struct {
	mu       sync.Mutex
	draining bool
	shutdown bool

	shutdownCtx context.Context
	cancel      context.CancelFunc
	wake        chan struct{}
}

// New starts reading sigs (pass the channel given to signal.Notify) until ctx is done. exit is called with 1 on a
// second shutdown signal; it is os.Exit in production. onDrain and onShutdown, when not nil, are called when the
// drain state changes and on the first shutdown signal.
func New(ctx context.Context, sigs <-chan os.Signal, exit func(code int), onDrain func(bool), onShutdown func()) *Controller {
	shutdownCtx, cancel := context.WithCancel(context.Background())
	c := &Controller{shutdownCtx: shutdownCtx, cancel: cancel, wake: make(chan struct{}, 1)}
	go c.run(ctx, sigs, exit, onDrain, onShutdown)
	return c
}

func (c *Controller) run(ctx context.Context, sigs <-chan os.Signal, exit func(code int), onDrain func(bool), onShutdown func()) {
	for {
		select {
		case <-ctx.Done():
			return
		case sig := <-sigs:
			switch sig {
			case syscall.SIGUSR1:
				c.setDraining(true, onDrain)
			case syscall.SIGUSR2:
				c.setDraining(false, onDrain)
			case syscall.SIGTERM, syscall.SIGINT:
				if !c.beginShutdown(onShutdown) {
					log.Warn("second shutdown signal, exiting now", "signal", sig)
					exit(1)
				}
			}
		}
	}
}

// setDraining changes the drain state; an undrain is ignored once the shutdown started, and a repeated signal does
// nothing.
func (c *Controller) setDraining(draining bool, onDrain func(bool)) {
	c.mu.Lock()
	if c.draining == draining || (!draining && c.shutdown) {
		c.mu.Unlock()
		return
	}
	c.draining = draining
	c.mu.Unlock()

	log.Info("drain changed", "draining", draining)
	if onDrain != nil {
		onDrain(draining)
	}
	c.signalWake()
}

// beginShutdown starts the shutdown; it returns false when it had already started.
func (c *Controller) beginShutdown(onShutdown func()) bool {
	c.mu.Lock()
	if c.shutdown {
		c.mu.Unlock()
		return false
	}
	c.shutdown = true
	c.mu.Unlock()

	log.Info("shutting down")
	c.cancel()
	if onShutdown != nil {
		onShutdown()
	}
	c.signalWake()
	return true
}

func (c *Controller) signalWake() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

// Draining tells whether a drain was requested and not undone.
func (c *Controller) Draining() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.draining
}

// ShutdownContext is canceled on the first SIGTERM or SIGINT.
func (c *Controller) ShutdownContext() context.Context {
	return c.shutdownCtx
}

// Wake gets a value when the drain or the shutdown state changes, so that an idle wait can end early.
func (c *Controller) Wake() <-chan struct{} {
	return c.wake
}
