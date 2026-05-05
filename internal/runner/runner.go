package runner

import (
	"fmt"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/anovimous/automatasdk/sdk"
)

func Run(s *sdk.SDK, command string, outputPath string) error {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		s.Fail(fmt.Sprintf("failed to start tool: %v", err))
		return fmt.Errorf("failed to start tool: %w", err)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	paused := false
	doneCh := make(chan error, 1)

	go func() {
		doneCh <- cmd.Wait()
	}()

	for {
		select {
		case err := <-doneCh:
			if err != nil {
				s.Fail(fmt.Sprintf("tool exited with error: %v", err))
				return fmt.Errorf("tool exited with error: %w", err)
			}
			s.Finish(outputPath)
			return nil

		case <-ticker.C:
			if s.IsCancelled() {
				syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				s.Fail("job was cancelled")
				return nil
			}

			if s.IsPaused() && !paused {
				syscall.Kill(-cmd.Process.Pid, syscall.SIGSTOP)
				paused = true
				s.UpdateState("PAUSED")
			}

			if !s.IsPaused() && paused {
				syscall.Kill(-cmd.Process.Pid, syscall.SIGCONT)
				paused = false
				s.UpdateState("RUNNING")
			}
		}
	}
}
