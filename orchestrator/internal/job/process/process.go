package process

import (
	log "log/slog"
	"os/exec"
	"time"

	"github.com/anovimous/cgroup/pkg/cgroup"
)

type Process struct {
	cmd    *exec.Cmd
	cgroup *cgroup.Cgroup
}

func New(cmd *exec.Cmd, cg *cgroup.Cgroup) *Process {
	return &Process{cmd: cmd, cgroup: cg}
}

func (p *Process) Terminate() error {
	log.Info("ORCH process.Terminate(): terminating job cgroup", "pid", p.cmd.Process.Pid)

	if err := p.cgroup.Terminate(5 * time.Second); err != nil {
		log.Error("ORCH process.Terminate(): cgroup termination failed", "error", err)
		return err
	}

	// Commented since it is called in the runner
	//p.cmd.Wait()

	log.Info("ORCH process.Terminate(): done")
	return nil
}

func (p *Process) Cgroup() *cgroup.Cgroup {
	return p.cgroup
}
