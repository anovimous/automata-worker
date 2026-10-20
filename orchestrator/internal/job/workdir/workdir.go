// Package workdir gives each job its own temp directory, so that everything
// the job downloads or produces (the details file holds credentials) can be
// removed in one go when the job is over.
package workdir

import (
	"fmt"
	"os"
	"path/filepath"
)

// Create makes the private directory base/job-<jobId>. A directory left over
// from an earlier attempt at the same job is emptied first.
func Create(base string, jobId int) (string, error) {
	dir := filepath.Join(base, fmt.Sprintf("job-%d", jobId))

	if err := os.RemoveAll(dir); err != nil {
		return "", fmt.Errorf("failed to clear the work directory of job %d: %w", jobId, err)
	}
	if err := os.MkdirAll(base, 0755); err != nil {
		return "", fmt.Errorf("failed to create the temp directory %s: %w", base, err)
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return "", fmt.Errorf("failed to create the work directory of job %d: %w", jobId, err)
	}
	return dir, nil
}

// Remove deletes the directory with everything in it. A missing directory is not an error.
func Remove(dir string) error {
	return os.RemoveAll(dir)
}
