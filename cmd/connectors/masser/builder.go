package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"

	"github.com/anovimous/sdkconnector/internal/parser"
	"github.com/google/uuid"
)

var allowedFlags = []string{"-n", "-value", "-log", "-request-proto", "-proxy", "-replay-proxy", "-mc", "-ml", "-mr", "-ms", "-mw", "-mmode", "-fc", "-fl", "-fr", "-fs", "-fw", "-fmode", "-hc", "-hs", "-hl", "-hr", "-hrf", "-hmode"}

type masserCustomConfig struct {
	Flags []parser.Flag `json:"flags"`
}

func BuildCommand(j *parser.JobDetails, dataFilePath string, tmpDir string) (command string, args []string, outputFilePath string, e error) {

	var customConfig masserCustomConfig

	err := json.Unmarshal(j.CustomConfig, &customConfig)
	if err != nil {
		return "", nil, "", fmt.Errorf("JobDetails.CustomConfig is not of allowed form for this routine: %w", err)
	}

	parts := []string{}

	outputFileName := fmt.Sprintf("%s.json", uuid.New().String())

	outputFilePath = filepath.Join(tmpDir, outputFileName)

	parts = append(parts, "-requests", dataFilePath)

	parts = append(parts, "-output", outputFilePath)

	parts = append(parts, "-rate", strconv.Itoa(j.Rate))

	if j.Verbosity != "" {
		parts = append(parts, "-verbosity", j.Verbosity)
	}

	for _, wordlist := range j.WordListsPaths {
		parts = append(parts, "-wordlist", wordlist)
	}

	for _, cookie := range j.Auth.Data.Cookies {
		parts = append(parts, "-C", buildCookie(cookie))
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

	return "masser", parts, outputFilePath, nil

}

func buildHeader(header parser.AuthNameValuePair) string {
	return fmt.Sprintf("%s: %s", header.Key, header.Value)
}

func buildCookie(cookie parser.AuthNameValuePair) string {
	return fmt.Sprintf("%s=%s", cookie.Key, cookie.Value)
}
