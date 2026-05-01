package process

import (
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

	p.cmd.Process.Signal(syscall.SIGTERM)

	done := make(chan error)
	go func() {
		done <- p.cmd.Wait()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		p.cmd.Process.Kill() // force kill if it didn't exit in time
	}
	return nil
}
