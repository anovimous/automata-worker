package rustfs

import (
	"path/filepath"
	"testing"
)

func TestLocalPathsAreInsideTheJobDirectory(t *testing.T) {
	dir := filepath.Join("tmp", "job-5")

	details, data := localPaths(dir, 5)

	if want := filepath.Join(dir, "details-5.json"); details != want {
		t.Errorf("details = %s, want %s", details, want)
	}
	if want := filepath.Join(dir, "data-5.jsonl"); data != want {
		t.Errorf("data = %s, want %s", data, want)
	}
}
