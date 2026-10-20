package cgroup

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const basePath = "/sys/fs/cgroup"

type Cgroup struct {
	Path string
}

// New creates a new cgroup directory under /sys/fs/cgroup
func New(name string) (*Cgroup, error) {
	path := filepath.Join(basePath, name)
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cgroup %s: %w", name, err)
	}

	// Allow child processes to create sub-cgroups and add themselves
	if err := os.Chmod(path, 0777); err != nil {
		return nil, fmt.Errorf("failed to chmod cgroup dir: %w", err)
	}
	if err := os.Chmod(filepath.Join(path, "cgroup.procs"), 0666); err != nil {
		return nil, fmt.Errorf("failed to chmod cgroup.procs: %w", err)
	}

	return &Cgroup{Path: path}, nil
}

// Open references an existing cgroup without creating it
func Open(name string) (*Cgroup, error) {
	path := filepath.Join(basePath, name)
	if _, err := os.Stat(path); err != nil {
		return nil, fmt.Errorf("cgroup %s does not exist: %w", name, err)
	}
	return &Cgroup{Path: path}, nil
}

// AddPid moves a process into this cgroup
func (c *Cgroup) AddPid(pid int) error {
	if err := os.WriteFile(
		filepath.Join(c.Path, "cgroup.procs"),
		[]byte(strconv.Itoa(pid)),
		0644,
	); err != nil {
		return fmt.Errorf("failed to add pid %d to cgroup %s: %w", pid, c.Path, err)
	}
	return nil
}

// allPids recursively collects all PIDs from this cgroup and all sub-cgroups
func (c *Cgroup) allPids() ([]int, error) {
	var pids []int

	err := filepath.WalkDir(c.Path, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.Name() != "cgroup.procs" {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return nil
		}

		for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
			if pid, err := strconv.Atoi(line); err == nil {
				pids = append(pids, pid)
			}
		}
		return nil
	})

	return pids, err
}

// Signal sends a signal to all processes in this cgroup and all sub-cgroups
func (c *Cgroup) Signal(sig syscall.Signal) error {
	pids, err := c.allPids()
	if err != nil {
		return err
	}
	for _, pid := range pids {
		syscall.Kill(pid, sig)
	}
	return nil
}

// Terminate sends SIGTERM to all processes and waits for them to exit.
// Falls back to SIGKILL after the given timeout.
func (c *Cgroup) Terminate(timeout time.Duration) error {
	if err := c.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	deadline := time.After(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			return c.Kill()
		case <-ticker.C:
			pids, _ := c.allPids()
			if len(pids) == 0 {
				return nil
			}
		}
	}
}

// Kill sends SIGKILL to all processes in this cgroup and all sub-cgroups
func (c *Cgroup) Kill() error {
	// Try cgroup.kill first (kernel 5.14+, atomic freeze+kill)
	err := os.WriteFile(filepath.Join(c.Path, "cgroup.kill"), []byte("1"), 0644)
	if err == nil {
		return nil
	}

	// Fallback for older kernels — freeze then kill
	os.WriteFile(filepath.Join(c.Path, "cgroup.freeze"), []byte("1"), 0644)
	c.Signal(syscall.SIGKILL)
	os.WriteFile(filepath.Join(c.Path, "cgroup.freeze"), []byte("0"), 0644)
	return nil
}

// Destroy removes the cgroup and all sub-cgroups.
// Must only be called after all processes have exited.
func (c *Cgroup) Destroy() error {
	// Collect all directories depth-first
	var dirs []string
	filepath.WalkDir(c.Path, func(path string, d os.DirEntry, err error) error {
		if err == nil && d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	})

	// Remove deepest first so parent directories are empty before removal
	var errs []error
	for i := len(dirs) - 1; i >= 0; i-- {
		if err := os.Remove(dirs[i]); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}
