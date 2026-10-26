package main

import (
	"context"
	"fmt"

	"github.com/anovimous/automata-worker/orchestrator/internal/wordlist"
)

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
