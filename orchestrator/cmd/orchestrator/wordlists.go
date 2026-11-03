package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/anovimous/automata-worker/orchestrator/internal/telemetry"
	"github.com/anovimous/automata-worker/orchestrator/internal/wordlist"
)

// observedDownloader is a wordlist downloader whose results also go to the worker's status, as the results of
// talking to the object storage. A download cut by its context says nothing of the storage, so it is left out.
type observedDownloader struct {
	downloader wordlist.Downloader
	recorder   dependencyRecorder
}

func (o observedDownloader) DownloadWordlist(ctx context.Context, id int64, w io.Writer) error {
	err := o.downloader.DownloadWordlist(ctx, id, w)
	if err == nil || ctx.Err() == nil || !errors.Is(err, ctx.Err()) {
		o.recorder.DependencyResult(telemetry.DepObjectStorage, err)
	}
	return err
}

// cacheStatser is what the status needs of the wordlist cache.
type cacheStatser interface {
	Stats() (wordlist.CacheStats, error)
}

// wordlistCacheStats reads the cache in the form of the status.
func wordlistCacheStats(cache cacheStatser) func() (telemetry.WordlistCache, error) {
	return func() (telemetry.WordlistCache, error) {
		s, err := cache.Stats()
		if err != nil {
			return telemetry.WordlistCache{}, err
		}
		return telemetry.WordlistCache{Dir: s.Dir, Bytes: s.Bytes, Files: s.Files, MaxBytes: s.MaxBytes, PinnedFiles: s.PinnedFiles}, nil
	}
}

// wordlistEnsurer makes the wordlist files of a job available on the local disk (wordlist.Cache).
type wordlistEnsurer interface {
	Ensure(ctx context.Context, refs []wordlist.Ref) (paths []string, release func(), err error)
}

// prepareWordlists gets the wordlists named by the details file into the cache and rewrites the file so
// that it holds their local paths (the form the connectors read). release lets the cache remove the files
// again when it needs room, call it when the job is over. On an error release is nil.
func prepareWordlists(ctx context.Context, cache wordlistEnsurer, detailsPath string) (release func(), err error) {
	refs, err := wordlist.RefsFromDetails(detailsPath)
	if err != nil {
		return nil, err
	}

	paths, release, err := cache.Ensure(ctx, refs)
	if err != nil {
		return nil, err
	}

	if err := wordlist.ResolveDetails(detailsPath, paths); err != nil {
		release()
		return nil, fmt.Errorf("failed to give the connector the wordlist paths: %w", err)
	}

	return release, nil
}
