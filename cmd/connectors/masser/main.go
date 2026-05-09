package main

import (
	"fmt"
	log "log/slog"
	"os"
	"strings"

	"github.com/anovimous/sdkconnector/internal/parser"
	"github.com/anovimous/sdkconnector/internal/runner"

	"github.com/anovimous/automatasdk/sdk"
)

func main() {

	log.Info("MASSER CONNECTOR STARTED")

	s, err := sdk.New()
	if err != nil {
		log.Error("failed to initialize sdk", "error", err)
		os.Exit(1)
	}
	log.Info("SDK INITIALIZED")
	jobDetails, err := parser.Parse(s.DetailsPath())
	if err != nil {
		log.Error("failed to parse job details", "error", err)
		s.Fail(fmt.Sprintf("failed to parse job details: %v", err))
		os.Exit(1)
	}
	log.Info("JOB DETAILS PARSED")
	tmpDir := s.TmpDirectory()

	command, args, outputFilePath, err := BuildCommand(jobDetails, s.DataPath(), tmpDir)
	log.Info("GOING TO EXECUTE CONNECTOR RUNNER.RUN WITH THE COMMAND", "command", command, "args", strings.Join(args, " "), "outputFilePath", outputFilePath)
	if err := runner.Run(s, command, args, outputFilePath); err != nil {
		log.Error("manager run failed", "jobId", s.JobId(), "error", err)
		os.Exit(1)
	}
}
