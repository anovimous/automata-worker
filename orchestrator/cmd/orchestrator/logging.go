package main

import (
	"io"
	log "log/slog"
)

// newLogger returns the logger of the orchestrator: text or JSON (LOG_FORMAT), every record carrying the
// worker id, so that the logs of several workers can be told apart.
func newLogger(format, workerID string, w io.Writer) *log.Logger {
	var handler log.Handler = log.NewTextHandler(w, nil)
	if format == "json" {
		handler = log.NewJSONHandler(w, nil)
	}
	return log.New(handler).With("workerId", workerID)
}
