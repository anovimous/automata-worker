package informer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/anovimous/orchestrator/internal/job"
)

// main functionality is to inform the central service when the job fails or finishes or any other detail

type Informer struct {
	baseURL  string
	endpoint string
}

type informMessage struct {
	JobId int          `json:"jobId"`
	State job.JobState `json:"state"`
	Msg   string       `json:"message"`
}

func New(baseURL, outcomeEndpoint string) *Informer {
	return &Informer{baseURL: baseURL,
		endpoint: outcomeEndpoint}
}

func (i *Informer) Inform(jobId int, state job.JobState, message string) error {
	payload := informMessage{
		JobId: jobId,
		State: state,
		Msg:   message,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal inform payload for job %d: %w", jobId, err)
	}

	url := fmt.Sprintf("%s%s/%d", i.baseURL, i.endpoint, jobId)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to inform central service for job %d: %w", jobId, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("central service returned status %d for job %d", resp.StatusCode, jobId)
	}

	return nil

}
