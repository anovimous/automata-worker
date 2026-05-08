package config

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

//go:embed routine-worker-mapping.json
var mappingsData []byte

type Mapper struct {
	mappings map[string]string
}

type KeyPath struct {
	Key  string `json:"key"`
	Path string `json:"path"`
}

func Load() (*Mapper, error) {

	mappings := map[string]string{}

	var allKeyPath []KeyPath

	if err := json.Unmarshal(mappingsData, &allKeyPath); err != nil {
		return nil, fmt.Errorf("failed to parse mappings file: %w", err)
	}

	for _, keyPath := range allKeyPath {
		mappings[keyPath.Key] = keyPath.Path
	}

	return &Mapper{mappings: mappings}, nil
}

func (m *Mapper) Lookup(routineKey string) (string, error) {

	path, ok := m.mappings[routineKey]
	if !ok {
		return "", fmt.Errorf("no mapping found for routineKey: %s", routineKey)
	}
	return path, nil
}
