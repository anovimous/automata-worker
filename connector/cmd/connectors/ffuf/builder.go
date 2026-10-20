package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	log "log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/anovimous/automata-worker/connector/internal/parser"
	"github.com/google/uuid"
)

var allowedFlags = []string{"-cc", "-ck", "-d", "-http2", "-ignore-body", "-r", "-raw", "-recursion", "-recursion-depth", "-recursion-strategy", "-replay-proxy", "-sni", "-timeout", "-u", "-x", "-V", "-ac", "-acc", "-ach", "-ack", "-acs", "-c", "-json", "-maxtime", "-maxtime-job", "-p", "-s", "-sa", "-scrapers", "-se", "-search", "-sf", "-t", "-v", "-mc", "-ml", "-mmode", "-mr", "-ms", "-mt", "-mw", "-fc", "-fl", "-fmode", "-fr", "-fs", "-ft", "-fw", "-D", "-e", "-enc", "-ic", "-input-cmd", "-input-num", "-input-shell", "-mode", "-request-proto"}

type ffufCustomConfig struct {
	RawRequestBase64 string        `json:"rawRequestBase64"`
	Flags            []parser.Flag `json:"flags"`
}

func BuildCommand(j *parser.JobDetails, tmpDir string) (command string, args []string, outputFilePath string, e error) {

	var customConfig ffufCustomConfig

	err := json.Unmarshal(j.CustomConfig, &customConfig)
	if err != nil {
		return "", nil, "", fmt.Errorf("JobDetails.CustomConfig is not of allowed form for this routine: %w", err)
	}

	// ffuf takes one wordlist; the core sends a set, so the first one is used
	if len(j.WordListsPaths) == 0 {
		return "", nil, "", fmt.Errorf("The job has no wordlist, ffuf needs one")
	}

	parts := []string{}

	outputFileName := fmt.Sprintf("%s.result", uuid.New().String())

	outputFilePath = filepath.Join(tmpDir, outputFileName)

	parts = append(parts, "-o", outputFilePath)

	parts = append(parts, "-w", j.WordListsPaths[0])

	parts = append(parts, "-rate", strconv.Itoa(j.Rate))

	parts = append(parts, "-noninteractive")

	if len(j.Auth.Data.Cookies) > 0 {
		parts = append(parts, "-b", buildCookieString(j.Auth.Data.Cookies))
	}

	for _, header := range j.Auth.Data.Headers {
		parts = append(parts, "-H", buildHeader(header))
	}

	for _, mar := range j.GenericConfig.MatchAndReplace {
		if mar.TargetType == "HEADER" && mar.Rule != "DELETE" {
			parts = append(parts, "-H", fmt.Sprintf("%s: %s", mar.TargetKey, mar.Value))
		}
	}

	for _, flag := range customConfig.Flags {
		if !slices.Contains(allowedFlags, flag.Key) {
			return "", nil, "", fmt.Errorf("Unknown flag provided in custom config")
		}
		parts = append(parts, flag.Key, flag.Value)
	}

	rawRequestBytes, err := base64.StdEncoding.DecodeString(customConfig.RawRequestBase64)
	if err != nil {
		return "", nil, "", fmt.Errorf("Corrupted base64 format provided in custom config")
	}
	log.Info("CONNECTOR.BUILDER:", "rawRequestbase64", customConfig.RawRequestBase64)
	requestFileName := fmt.Sprintf("%s.raw", uuid.New().String())

	requestFilePath := filepath.Join(tmpDir, requestFileName)

	err = os.WriteFile(requestFilePath, rawRequestBytes, 0666)
	if err != nil {
		return "", nil, "", fmt.Errorf("Failed to store the request temporarily on the FS")
	}
	parts = append(parts, "-request", requestFilePath)

	return "ffuf", parts, outputFilePath, nil

}

func buildCookieString(cookies []parser.AuthNameValuePair) string {
	parts := make([]string, len(cookies))

	for i, cookie := range cookies {
		parts[i] = fmt.Sprintf("%s=%s", cookie.Key, cookie.Value)
	}

	return strings.Join(parts, "; ")
}

func buildHeader(header parser.AuthNameValuePair) string {
	return fmt.Sprintf("%s: %s", header.Key, header.Value)
}
