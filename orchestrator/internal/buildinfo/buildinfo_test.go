package buildinfo

import "testing"

// the ldflags path is checked by the Docker build
func TestDefaultsWhenNotSetAtBuildTime(t *testing.T) {
	if Version != "dev" || Commit != "unknown" {
		t.Errorf("Version, Commit = %q, %q, want dev, unknown", Version, Commit)
	}
}
