package workdir

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateMakesAPrivateDirectoryPerJob(t *testing.T) {
	base := filepath.Join(t.TempDir(), "tmp")

	dir, err := Create(base, 7)
	if err != nil {
		t.Fatalf("Create returned an error: %v", err)
	}

	if dir != filepath.Join(base, "job-7") {
		t.Errorf("dir = %s, want %s", dir, filepath.Join(base, "job-7"))
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("the directory was not created: %v", err)
	}
	if info.Mode().Perm() != 0700 {
		t.Errorf("mode = %o, want 700", info.Mode().Perm())
	}
}

func TestCreateGivesDifferentJobsDifferentDirectories(t *testing.T) {
	base := t.TempDir()

	a, _ := Create(base, 1)
	b, _ := Create(base, 2)

	if a == b {
		t.Errorf("both jobs got %s", a)
	}
}

func TestCreateEmptiesALeftoverDirectory(t *testing.T) {
	base := t.TempDir()
	dir, _ := Create(base, 3)
	leftover := filepath.Join(dir, "details-3.json")
	if err := os.WriteFile(leftover, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := Create(base, 3); err != nil {
		t.Fatalf("Create returned an error: %v", err)
	}

	if _, err := os.Stat(leftover); !os.IsNotExist(err) {
		t.Errorf("the leftover file is still there (err = %v)", err)
	}
}

func TestCreateFailsWhenTheBaseIsAFile(t *testing.T) {
	base := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(base, []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	if _, err := Create(base, 1); err == nil {
		t.Error("expected an error, got none")
	}
}

func TestRemoveDeletesTheDirectoryWithItsFiles(t *testing.T) {
	dir, _ := Create(t.TempDir(), 4)
	if err := os.WriteFile(filepath.Join(dir, "a.result"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}

	if err := Remove(dir); err != nil {
		t.Fatalf("Remove returned an error: %v", err)
	}

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Errorf("the directory is still there (err = %v)", err)
	}
}

func TestRemoveOfAMissingDirectoryIsNotAnError(t *testing.T) {
	if err := Remove(filepath.Join(t.TempDir(), "job-9")); err != nil {
		t.Errorf("Remove returned an error: %v", err)
	}
}
