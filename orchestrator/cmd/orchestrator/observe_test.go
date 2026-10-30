package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/anovimous/automata-worker/orchestrator/internal/job"
	"github.com/anovimous/automata-worker/orchestrator/internal/telemetry"
	"github.com/anovimous/automata-worker/orchestrator/internal/wordlist"
)

// recordedResult is one call of DependencyResult.
type recordedResult struct {
	dep telemetry.Dependency
	err error
}

type fakeRecorder struct {
	results []recordedResult
}

func (f *fakeRecorder) DependencyResult(d telemetry.Dependency, err error) {
	f.results = append(f.results, recordedResult{d, err})
}

func TestObservedInformerRecordsTheResultAsTheCore(t *testing.T) {
	failure := errors.New("core is down")
	for _, informErr := range []error{nil, failure} {
		inner := &fakeInformer{err: informErr}
		rec := &fakeRecorder{}

		err := observedInformer{informer: inner, recorder: rec}.Inform(4, job.StateRunning, "", "")

		if err != informErr || inner.called != 1 || inner.jobId != 4 || inner.state != job.StateRunning {
			t.Errorf("Inform returned %v, informer %+v, want the informer's result passed on", err, inner)
		}
		if len(rec.results) != 1 || rec.results[0].dep != telemetry.DepCore || rec.results[0].err != informErr {
			t.Errorf("recorded %+v, want one core result %v", rec.results, informErr)
		}
	}
}

// fakeObjectDownloader writes content or fails.
type fakeObjectDownloader struct {
	content string
	err     error
}

func (f fakeObjectDownloader) DownloadWordlist(ctx context.Context, id int64, w io.Writer) error {
	if f.err != nil {
		return f.err
	}
	_, err := io.WriteString(w, f.content)
	return err
}

func TestObservedDownloaderRecordsTheResultAsTheObjectStorage(t *testing.T) {
	failure := errors.New("connection reset")
	for _, downloadErr := range []error{nil, failure} {
		rec := &fakeRecorder{}
		var out bytes.Buffer

		err := observedDownloader{downloader: fakeObjectDownloader{content: "admin\n", err: downloadErr}, recorder: rec}.
			DownloadWordlist(context.Background(), 7, &out)

		if err != downloadErr {
			t.Errorf("DownloadWordlist returned %v, want %v", err, downloadErr)
		}
		if len(rec.results) != 1 || rec.results[0].dep != telemetry.DepObjectStorage || rec.results[0].err != downloadErr {
			t.Errorf("recorded %+v, want one object storage result %v", rec.results, downloadErr)
		}
	}
}

func TestObservedDownloaderLeavesOutACanceledDownload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := &fakeRecorder{}

	err := observedDownloader{downloader: fakeObjectDownloader{err: context.Canceled}, recorder: rec}.
		DownloadWordlist(ctx, 7, io.Discard)

	if !errors.Is(err, context.Canceled) || len(rec.results) != 0 {
		t.Errorf("err = %v, recorded %+v, want the cancel passed on and nothing recorded", err, rec.results)
	}
}

// a storage error during a canceled context is still one of the storage
func TestObservedDownloaderRecordsAnotherErrorAfterACancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := &fakeRecorder{}

	observedDownloader{downloader: fakeObjectDownloader{err: errors.New("no such key")}, recorder: rec}.
		DownloadWordlist(ctx, 7, io.Discard)

	if len(rec.results) != 1 {
		t.Errorf("recorded %+v, want the storage error", rec.results)
	}
}

type fakeStatser struct {
	stats wordlist.CacheStats
	err   error
}

func (f fakeStatser) Stats() (wordlist.CacheStats, error) { return f.stats, f.err }

func TestWordlistCacheStatsConvertsTheReading(t *testing.T) {
	read := wordlistCacheStats(fakeStatser{stats: wordlist.CacheStats{Dir: "/c", Bytes: 9, Files: 2, MaxBytes: 100, PinnedFiles: 1}})

	got, err := read()
	want := telemetry.WordlistCache{Dir: "/c", Bytes: 9, Files: 2, MaxBytes: 100, PinnedFiles: 1}
	if err != nil || got != want {
		t.Errorf("read() = %+v, %v, want %+v", got, err, want)
	}
}

func TestWordlistCacheStatsPassesTheErrorOn(t *testing.T) {
	failure := errors.New("no such directory")
	if _, err := wordlistCacheStats(fakeStatser{err: failure})(); err != failure {
		t.Errorf("err = %v, want %v", err, failure)
	}
}
