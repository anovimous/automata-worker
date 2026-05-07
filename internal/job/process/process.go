package process

import (
	log "log/slog"
	"os/exec"
	"syscall"
	"time"
)

type Process struct {
	cmd *exec.Cmd
}

func New(cmd *exec.Cmd) *Process {
	return &Process{cmd: cmd}
}

func (p *Process) Terminate() error {

	log.Info("ORCH process.Terminate(): going to terminate process of", "pid", p.cmd.Process.Pid)
	syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	log.Info("ORCH process.Terminate(): syscall.terminate finished")
	done := make(chan error)
	go func() {
		done <- p.cmd.Wait()
		log.Info("ORCH process.Terminate(): cmd.Wait returned with", "error", done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		log.Info("ORCH process.Terminate(): syscall.KILL finished")
		<-done
		log.Info("ORCH process.Terminate(): done channel received after SIGKILL")
	}

	log.Info("ORCH process.Terminate(): going to return")
	return nil
}

func (p *Process) Wait() <-chan error {
	errCh := make(chan error, 1)
	errCh <- p.cmd.Wait()
	return errCh
}
