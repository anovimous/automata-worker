package poller

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
)

// polls the central service for the status of the job

const requestTimeout = 10 * time.Second

type Poller struct {
	baseURL  string
	endpoint string
	interval time.Duration
	client   *http.Client
	observe  func(err error)
}

type PollInfo struct {
	DesiredState job.JobState `json:"requestedState"`
}

func New(baseURL, stateEndpoint string, pollInterval int) *Poller {
	return &Poller{baseURL: baseURL,
		endpoint: stateEndpoint,
		interval: time.Duration(pollInterval) * time.Second,
		client:   &http.Client{Timeout: requestTimeout}}
}

func (p *Poller) SinglePoll(jobId int) (PollInfo, error) {
	url := fmt.Sprintf("%s%s/%d", p.baseURL, p.endpoint, jobId)

	resp, err := p.client.Get(url)
	if err != nil {
		return PollInfo{}, fmt.Errorf("failed to poll central service for job %d: %w", jobId, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return PollInfo{}, fmt.Errorf("central service returned status %d for job %d", resp.StatusCode, jobId)
	}

	var info PollInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return PollInfo{}, fmt.Errorf("failed to decode poll response for job %d: %w", jobId, err)
	}

	return info, nil
}

// Observe gives fn the result of every poll that Start makes, nil for a success: Start does nothing else with a
// failure (the next poll comes soon), and the worker's status shows them. Call it before Start.
func (p *Poller) Observe(fn func(err error)) {
	p.observe = fn
}

// Start polls the central service until ctx is done and sends the desired state
// of the job each time it differs from the last one it sent. The returned
// function makes the poller forget what it sent, so the desired state is sent
// again on the next poll. It is for when a sent state could not be delivered.
func (p *Poller) Start(jobId int, ctx context.Context) (<-chan PollInfo, func()) {

	var mu sync.Mutex
	currentState := job.StateRunning

	infoCh := make(chan PollInfo)
	log.Info("poller.Start() called")
	go func() {
		defer close(infoCh)

		ticker := time.NewTicker(p.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				info, err := p.SinglePoll(jobId)
				if p.observe != nil {
					p.observe(err)
				}
				if err != nil {
					continue
				}

				mu.Lock()
				changed := info.DesiredState != currentState
				if changed {
					currentState = info.DesiredState
				}
				mu.Unlock()

				if changed {
					infoCh <- info
				}
			}
		}
	}()

	forget := func() {
		mu.Lock()
		defer mu.Unlock()
		currentState = ""
	}

	return infoCh, forget
}
