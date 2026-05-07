package parser

import (
	"encoding/json"
	"fmt"
	"os"
)

// MODEL START:
// Main struct

type JobDetails struct {
	JobId         int             `json:"jobId"`
	RoutineKey    string          `json:"routineKey"`
	Rate          int             `json:"rate"`
	Verbosity     string          `json:"verbosity"`
	Auth          AuthDataWrapper `json:"auth"`
	TargetType    string          `json:"targetType"`
	GenericConfig GenericConfig   `json:"genericConfig"`
	CustomConfig  json.RawMessage `json:"customConfig"`
	WordListPath  string          `json:"wordlistPath"`
}

// Authentication Details

type AuthDataWrapper struct {
	Data              StaticAuthData `json:"data"`
	LifetimeInSeconds string         `json:"lifeTimeInSeconds"`
	PopulatedAt       string         `json:"populatedAt"`
}

type StaticAuthData struct {
	Headers         []AuthNameValuePair `json:"headers"`
	Cookies         []AuthNameValuePair `json:"cookies"`
	QueryParameters []AuthNameValuePair `json:"queryParameters"`
}

type AuthNameValuePair struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Generic Config including matchandreplace

type GenericConfig struct {
	MatchAndReplace []MatchAndReplace `json:"matchAndReplace"`
}

type MatchAndReplace struct {
	TargetType MatchAndReplaceTargetType `json:"targetType"`
	Rule       MatchAndReplaceRule       `json:"rule"`
	TargetKey  string                    `json:"targetKey"`
	Value      string                    `json:"value"`
}

type MatchAndReplaceTargetType string

const (
	TargetTypeHeader MatchAndReplaceTargetType = "HEADER"
)

type MatchAndReplaceRule string

const (
	RuleAdd     MatchAndReplaceRule = "ADD"
	RuleReplace MatchAndReplaceRule = "REPLACE"
	RuleDelete  MatchAndReplaceRule = "DELETE"
)

// Custom config contents

type Flag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// MODEL END

// Parses the file into JobDetails object

func Parse(localPath string) (*JobDetails, error) {
	data, err := os.ReadFile(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read job details file: %w", err)
	}

	var details JobDetails
	if err := json.Unmarshal(data, &details); err != nil {
		return nil, fmt.Errorf("failed to parse job details file: %w", err)
	}

	return &details, nil
}
