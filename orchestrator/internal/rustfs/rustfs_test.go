package rustfs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/config"
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

func TestWordlistObjectIsNamedByTheIdOfTheWordlist(t *testing.T) {
	if got := wordlistObjectName(42); got != "42" {
		t.Errorf("wordlistObjectName = %q, want 42", got)
	}
}

// a shutdown must not wait for a download that hangs
func TestDownloadJobDetailsAndDataStopsWithItsContext(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(release)

	r, err := New(config.RustFSConfig{Endpoint: strings.TrimPrefix(server.URL, "http://"), AccessKey: "a", SecretKey: "b", JobsBucket: "job-configuration"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, _, err = r.DownloadJobDetailsAndData(ctx, 5, t.TempDir())

	if err == nil {
		t.Fatal("the download returned no error after the cancel")
	}
	if took := time.Since(start); took > 3*time.Second {
		t.Errorf("the download took %s after the cancel, want it to stop soon", took)
	}
}
