package cgroup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDestroyRemovesTheNestedDirectories(t *testing.T) {
	root := filepath.Join(t.TempDir(), "job-1")
	if err := os.MkdirAll(filepath.Join(root, "tool"), 0o755); err != nil {
		t.Fatal(err)
	}

	if err := (&Cgroup{Path: root}).Destroy(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("cgroup directory still exists: %v", err)
	}
}

func TestDestroyOfAMissingCgroupIsNotAnError(t *testing.T) {
	if err := (&Cgroup{Path: filepath.Join(t.TempDir(), "gone")}).Destroy(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestDestroyReportsADirectoryThatCanNotBeRemoved(t *testing.T) {
	root := filepath.Join(t.TempDir(), "job-1")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	// a file inside stands for the busy case: the directory can not be removed
	if err := os.WriteFile(filepath.Join(root, "cgroup.procs"), []byte("1"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := (&Cgroup{Path: root}).Destroy(); err == nil {
		t.Error("expected an error for a directory that is not empty")
	}
}
