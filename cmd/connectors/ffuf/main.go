package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/anovimous/sdkconnector/internal/parser"
	"github.com/anovimous/sdkconnector/internal/runner"

	"github.com/anovimous/automatasdk/sdk"
)

func main() {
	s, err := sdk.New()
	if err != nil {
		slog.Error("failed to initialize sdk", "error", err)
		os.Exit(1)
	}

	jobDetails, err := parser.Parse(s.DetailsPath())
	if err != nil {
		slog.Error("failed to parse job details", "error", err)
		s.Fail(fmt.Sprintf("failed to parse job details: %v", err))
		os.Exit(1)
	}

	tmpDir := s.TmpDirectory()

	command, outputFilePath, err := BuildCommand(jobDetails, tmpDir)

	if err := runner.Run(s, command, outputFilePath); err != nil {
		slog.Error("manager run failed", "jobId", s.JobId(), "error", err)
		os.Exit(1)
	}
}
