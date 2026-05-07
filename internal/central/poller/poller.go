package poller

import (
	"context"
	"encoding/json"
	"fmt"
	log "log/slog"
	"net/http"
	"time"

	"github.com/anovimous/orchestrator/internal/job"
)

// polls the central service for the status of the job

type Poller struct {
	baseURL  string
	endpoint string
	interval int
}

type PollInfo struct {
	DesiredState job.JobState `json:"requestedState"`
}

func New(baseURL, stateEndpoint string, pollInterval int) *Poller {
	return &Poller{baseURL: baseURL,
		endpoint: stateEndpoint,
		interval: pollInterval}
}

func (p *Poller) SinglePoll(jobId int) (PollInfo, error) {
	url := fmt.Sprintf("%s%s/%d", p.baseURL, p.endpoint, jobId)

	resp, err := http.Get(url)
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

func (p *Poller) Start(jobId int, ctx context.Context) <-chan PollInfo {

	currentState := job.StateRunning

	infoCh := make(chan PollInfo)
	log.Info("poller.Start() called")
	go func() {
		defer close(infoCh)

		ticker := time.NewTicker(time.Duration(p.interval) * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				info, err := p.SinglePoll(jobId)
				if err != nil {
					continue
				}

				if info.DesiredState != currentState {
					currentState = info.DesiredState
					infoCh <- info
				}
			}
		}
	}()

	return infoCh
}
