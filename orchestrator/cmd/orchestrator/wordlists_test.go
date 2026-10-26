package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anovimous/automata-worker/orchestrator/internal/wordlist"
)

type fakeEnsurer struct {
	paths    []string
	err      error
	seen     []wordlist.Ref
	released int
}

func (f *fakeEnsurer) Ensure(ctx context.Context, refs []wordlist.Ref) ([]string, func(), error) {
	f.seen = refs
	if f.err != nil {
		return nil, nil, f.err
	}
	return f.paths, func() { f.released++ }, nil
}

func detailsFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "details-1.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestPrepareWordlistsEnsuresThemAndGivesTheConnectorTheirPaths(t *testing.T) {
	path := detailsFile(t, `{"jobId":1,"wordlists":[{"id":7,"name":"common","sha256":"aa","sizeBytes":3}]}`)
	cache := &fakeEnsurer{paths: []string{"/cache/aa"}}

	release, err := prepareWordlists(context.Background(), cache, path)
	if err != nil {
		t.Fatalf("prepareWordlists returned an error: %v", err)
	}

	if len(cache.seen) != 1 || cache.seen[0].Id != 7 || cache.seen[0].Sha256 != "aa" {
		t.Errorf("refs given to the cache = %+v", cache.seen)
	}
	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), `"wordlistsPaths":["/cache/aa"]`) || strings.Contains(string(data), `"wordlists":`) {
		t.Errorf("details file = %s", data)
	}
	if cache.released != 0 {
		t.Error("the files were released before the job ran")
	}
	release()
	if cache.released != 1 {
		t.Errorf("released = %d, want 1", cache.released)
	}
}

func TestPrepareWordlistsOfAJobWithoutWordlistsStillWritesAnEmptyList(t *testing.T) {
	path := detailsFile(t, `{"jobId":1,"wordlists":[]}`)
	cache := &fakeEnsurer{paths: []string{}}

	release, err := prepareWordlists(context.Background(), cache, path)
	if err != nil {
		t.Fatal(err)
	}
	release()

	data, _ := os.ReadFile(path)
	if !strings.Contains(string(data), `"wordlistsPaths":[]`) {
		t.Errorf("details file = %s", data)
	}
}

func TestPrepareWordlistsReturnsTheErrorOfTheCacheAndKeepsTheFile(t *testing.T) {
	original := `{"jobId":1,"wordlists":[{"id":7,"sha256":"aa","sizeBytes":3}]}`
	path := detailsFile(t, original)
	cache := &fakeEnsurer{err: errors.New("failed to get wordlist 7: no such key")}

	release, err := prepareWordlists(context.Background(), cache, path)

	if err == nil || !strings.Contains(err.Error(), "wordlist 7") {
		t.Fatalf("err = %v", err)
	}
	if release != nil {
		t.Error("release is set on an error")
	}
	if data, _ := os.ReadFile(path); string(data) != original {
		t.Errorf("details file changed: %s", data)
	}
}

func TestPrepareWordlistsOfAnUnreadableDetailsFileFailsBeforeTheCache(t *testing.T) {
	cache := &fakeEnsurer{}

	if _, err := prepareWordlists(context.Background(), cache, filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("prepareWordlists returned no error")
	}

	if cache.seen != nil {
		t.Error("the cache was asked for wordlists")
	}
}
