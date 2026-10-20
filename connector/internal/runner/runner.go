package runner

import (
	"fmt"
	log "log/slog"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/anovimous/automata-worker/cgroup/pkg/cgroup"
	"github.com/anovimous/automata-worker/sdk/go/sdk"
)

func Run(s *sdk.SDK, command string, args []string, outputPath string) error {

	// Create tool cgroup nested inside the job cgroup
	jobCgName := fmt.Sprintf("automata/job-%d", s.JobId())
	toolCgName := fmt.Sprintf("automata/job-%d/tool", s.JobId())

	toolCg, err := cgroup.New(toolCgName)
	if err != nil {
		s.Fail(fmt.Sprintf("failed to create tool cgroup: %v", err))
		return fmt.Errorf("failed to create tool cgroup: %w", err)
	}
	defer func() {
		toolCg.Kill()
		toolCg.Destroy()
	}()

	cmd := exec.Command(command, args...)
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	// Move self into tool cgroup so ffuf inherits it at fork time
	if err := toolCg.AddPid(os.Getpid()); err != nil {
		s.Fail(fmt.Sprintf("failed to move self into tool cgroup: %v", err))
		return fmt.Errorf("failed to move self into tool cgroup: %w", err)
	}

	if err := cmd.Start(); err != nil {
		// Move back to job cgroup before returning
		if jobCg, e := cgroup.Open(jobCgName); e == nil {
			jobCg.AddPid(os.Getpid())
		}
		s.Fail(fmt.Sprintf("failed to start tool: %v, full command is: %s %s, the output path is: %s", err, command, strings.Join(args, " "), outputPath))
		return fmt.Errorf("failed to start tool: %w", err)
	}

	// Move self back to job cgroup immediately after fork —
	// ffuf is already born inside the tool cgroup
	if jobCg, e := cgroup.Open(jobCgName); e == nil {
		if e := jobCg.AddPid(os.Getpid()); e != nil {
			log.Error("connector.runner: failed to move self back to job cgroup", "error", e)
		}
	} else {
		log.Error("connector.runner: failed to open job cgroup", "error", e)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	paused := false
	doneCh := make(chan error, 1)

	go func() {
		doneCh <- cmd.Wait()
		log.Info("cmd.Wait() for connector cmd.start returned", "exitCode", cmd.ProcessState.ExitCode())
	}()

	for {
		select {
		case err := <-doneCh:
			if err != nil {
				s.Fail(fmt.Sprintf("tool exited with error: %v, full command was: %s %s", err, command, strings.Join(args, " ")))
				return fmt.Errorf("tool exited with error: %w", err)
			}
			s.Finish(outputPath)
			return nil

		case <-ticker.C:
			if s.IsCancelled() {
				toolCg.Terminate(5 * time.Second)
				s.Cancel("job was cancelled")
				return nil
			}

			if s.IsPaused() && !paused {
				toolCg.Signal(syscall.SIGSTOP)
				paused = true
				s.UpdateState("PAUSED")
			}

			if !s.IsPaused() && paused {
				toolCg.Signal(syscall.SIGCONT)
				paused = false
				s.UpdateState("RUNNING")
			}
		}
	}
}
