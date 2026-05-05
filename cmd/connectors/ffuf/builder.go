package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/anovimous/sdkconnector/internal/parser"
	"github.com/google/uuid"
)

var allowedFlags = []string{"-H", "-X", "-b", "-cc", "-ck", "-d", "-http2", "-ignore-body", "-r", "-raw", "-recursion", "-recursion-depth", "-recursion-strategy", "-replay-proxy", "-sni", "-timeout", "-u", "-x", "-V", "-ac", "-acc", "-ach", "-ack", "-acs", "-c", "-config", "-json", "-maxtime", "-maxtime-job", "-noninteractive", "-p", "-rate", "-s", "-sa", "-scraperfile", "-scrapers", "-se", "-search", "-sf", "-t", "-v", "-mc", "-ml", "-mmode", "-mr", "-ms", "-mt", "-mw", "-fc", "-fl", "-fmode", "-fr", "-fs", "-ft", "-fw", "-D", "-e", "-enc", "-ic", "-input-cmd", "-input-num", "-input-shell", "-mode", "-request", "-request-proto", "-w", "-debug-log", "-o", "-od", "-of", "-or"}

type ffufCustomConfig struct {
	rawRequestBase64 string
	flags            []parser.Flag
}

func BuildCommand(j *parser.JobDetails, tmpDir string) (command string, outputFilePath string, e error) {

	var customConfig ffufCustomConfig

	err := json.Unmarshal(j.CustomConfig, &customConfig)
	if err != nil {
		return "", "", fmt.Errorf("JobDetails.CustomConfig is not of allowed form for this routine: %w", err)
	}

	parts := []string{"ffuf"}

	outputFileName := fmt.Sprintf("%s.json", uuid.New().String())

	outputFilePath = filepath.Join(tmpDir, outputFileName)

	parts = append(parts, "-o", outputFilePath)

	parts = append(parts, "-w", j.WordListPath)

	parts = append(parts, "-b", buildCookieString(j.Auth.Data.Cookies))

	for _, header := range j.Auth.Data.Headers {
		parts = append(parts, "-H", buildHeader(header))
	}

	for _, mar := range j.GenericConfig.MatchAndReplace {
		if mar.TargetType == "HEADER" && mar.Rule != "DELETE" {
			parts = append(parts, "-H", fmt.Sprintf("%s: %s", mar.TargetKey, mar.Value))
		}
	}

	for _, flag := range customConfig.flags {
		if !slices.Contains(allowedFlags, flag.Key) {
			return "", "", fmt.Errorf("Unknown flag provided in custom config")
		}
		parts = append(parts, flag.Key, flag.Value)
	}

	rawRequest, err := base64.StdEncoding.DecodeString(customConfig.rawRequestBase64)
	if err != nil {
		return "", "", fmt.Errorf("Corrupted base64 format provided in custom config")
	}

	requestFileName := fmt.Sprintf("%s.raw", uuid.New().String())

	requestFilePath := filepath.Join(tmpDir, requestFileName)

	err = os.WriteFile(requestFilePath, []byte(rawRequest), 0666)
	if err != nil {
		return "", "", fmt.Errorf("Failed to store the request temporarily on the FS")
	}
	parts = append(parts, "-request", requestFilePath)

	return strings.Join(parts, " "), outputFilePath, nil

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
