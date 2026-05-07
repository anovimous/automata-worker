package runner

import (
	"fmt"
	log "log/slog"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/anovimous/automatasdk/sdk"
)

func Run(s *sdk.SDK, command string, args []string, outputPath string) error {

	cmd := exec.Command(command, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}

	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil

	if err := cmd.Start(); err != nil {
		s.Fail(fmt.Sprintf("failed to start tool: %v, full command is: %s %s, the output path is: %s", err, command, strings.Join(args, " "), outputPath))
		return fmt.Errorf("failed to start tool: %w", err)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	paused := false
	doneCh := make(chan error, 1)

	go func() {
		doneCh <- cmd.Wait()
		log.Info("cmd.Wait() for connector cmd.start returned", "exitCode", cmd.ProcessState.ExitCode())
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)
	go func() {
		<-sigCh
		syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		select {
		case <-doneCh:
		case <-time.After(5 * time.Second):
			syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}()

	for {
		select {
		case err := <-doneCh:
			if err != nil {
				s.Fail(fmt.Sprintf("tool exited with error: %v, full command was ", err))
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
