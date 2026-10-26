package informer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
	"github.com/anovimous/automata-worker/orchestrator/internal/retry"
)

// main functionality is to inform the central service when the job fails or finishes or any other detail

const (
	requestTimeout = 10 * time.Second
	maxAttempts    = 5
	baseDelay      = time.Second
	maxDelay       = 30 * time.Second

	// how much of the answer of the central service is kept in an error
	maxAnswerInError = 512
)

type Informer struct {
	baseURL  string
	endpoint string
	client   *http.Client
	retry    retry.Policy
}

type informMessage struct {
	JobId           int                 `json:"jobId"`
	State           job.JobState        `json:"newState"`
	Msg             string              `json:"message"`
	FailureCategory job.FailureCategory `json:"failureCategory,omitempty"`
}

func New(baseURL, outcomeEndpoint string) *Informer {
	return &Informer{baseURL: baseURL,
		endpoint: outcomeEndpoint,
		client:   &http.Client{Timeout: requestTimeout},
		retry:    retry.Policy{Attempts: maxAttempts, BaseDelay: baseDelay, MaxDelay: maxDelay},
	}
}

// Inform reports the state of the job to the central service. A network error,
// a timeout or a 5xx answer is retried with a backoff. Any other answer than
// 204 means the central service rejected the report, which a retry can't fix,
// so it is returned at once.
func (i *Informer) Inform(jobId int, state job.JobState, message string, category job.FailureCategory) error {
	payload := informMessage{
		JobId:           jobId,
		State:           state,
		Msg:             message,
		FailureCategory: category,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal inform payload for job %d: %w", jobId, err)
	}

	url := fmt.Sprintf("%s%s/%d", i.baseURL, i.endpoint, jobId)

	return i.retry.Do(func() error {
		return i.post(url, body, jobId)
	})
}

func (i *Informer) post(url string, body []byte, jobId int) error {
	resp, err := i.client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to inform central service for job %d: %w", jobId, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return nil
	}

	answer, _ := io.ReadAll(io.LimitReader(resp.Body, maxAnswerInError))
	err = fmt.Errorf("central service returned status %d for job %d: %s", resp.StatusCode, jobId, answer)

	if resp.StatusCode >= http.StatusInternalServerError {
		return err
	}

	return retry.Permanent(err)
}
