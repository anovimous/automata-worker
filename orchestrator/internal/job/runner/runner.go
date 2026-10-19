package runner

import (
	"fmt"
	log "log/slog"
	"os/exec"
	"strings"

	"github.com/anovimous/cgroup/pkg/cgroup"
	"github.com/anovimous/orchestrator/internal/config"
	"github.com/anovimous/orchestrator/internal/job/process"
	"github.com/anovimous/orchestrator/internal/job/socket"
)

type Runner struct {
	heartbeatInterval int
	tmpDir            string
}

func New(cfg config.WorkerConfig, tmpDir string) *Runner {
	return &Runner{
		heartbeatInterval: cfg.HeartbeatInterval,
		tmpDir:            tmpDir,
	}
}

func (r *Runner) Start(jobId int, command string, detailsLocation string, dataLocation string, offReadSocket *socket.Socket, offWriteSocket *socket.Socket) (*process.Process, error) {
	args := []string{
		"--jobId", fmt.Sprintf("%d", jobId),
		"--details", detailsLocation,
		"--data", dataLocation,
		"--readSocket", offWriteSocket.Path,
		"--writeSocket", offReadSocket.Path,
		"--heartbeatInterval", fmt.Sprintf("%d", r.heartbeatInterval),
		"--tmpDir", r.tmpDir,
	}

	parts := strings.Fields(command)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty command for job %d", jobId)
	}

	cmd := exec.Command(parts[0], append(parts[1:], args...)...)
	log.Info("full command ran by orchestrator.Runner.Start(): " + cmd.String())

	cmd.Stdin = nil
	cmd.Stdout = nil // DEV ONLY
	cmd.Stderr = nil // DEV ONLY

	// Create job cgroup before starting the process
	jobCg, err := cgroup.New(fmt.Sprintf("automata/job-%d", jobId))
	if err != nil {
		return nil, fmt.Errorf("failed to create job cgroup for job %d: %w", jobId, err)
	}

	if err := cmd.Start(); err != nil {
		jobCg.Destroy()
		return nil, fmt.Errorf("failed to start process for job %d: %w", jobId, err)
	}
	// NOTE: This is commented since in the connector runner the connector moves itself in and out of the jobCg and toolCg
	// // Add connector to job cgroup
	// if err := jobCg.AddPid(cmd.Process.Pid); err != nil {
	// 	cmd.Process.Kill()
	// 	jobCg.Destroy()
	// 	return nil, fmt.Errorf("failed to add connector to job cgroup for job %d: %w", jobId, err)
	// }

	go func() {
		cmd.Wait()
	}()

	return process.New(cmd, jobCg), nil
}
