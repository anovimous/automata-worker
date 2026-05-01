package runner

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/anovimous/orchestrator/internal/config"
	"github.com/anovimous/orchestrator/internal/job/process"
	"github.com/anovimous/orchestrator/internal/job/socket"
)

type Runner struct {
	heartbeatInterval int
}

func New(cfg config.WorkerConfig) *Runner {
	return &Runner{cfg.HeartbeatInterval}
}

func (r *Runner) Start(jobId int, command string, detailsLocation string, dataLocation string, offReadSocket *socket.Socket, offWriteSocket *socket.Socket) (*process.Process, error) {
	args := []string{
		"--jobId", fmt.Sprintf("%d", jobId),
		"--details", detailsLocation,
		"--data", dataLocation,
		"--readSocket", offReadSocket.Path,
		"--writeSocket", offWriteSocket.Path,
		"--heartbeatInterval", fmt.Sprintf("%d", r.heartbeatInterval),
	}

	parts := strings.Fields(command)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command for job %d", jobId)
	}

	cmd := exec.Command(parts[0], append(parts[1:], args...)...)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start process for job %d: %w", jobId, err)
	}

	return process.New(cmd), nil
}
