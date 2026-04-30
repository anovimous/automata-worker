package config

import (
	"encoding/json"
	"fmt"
	"os"
)

var mappings = map[string]string{}

type KeyPath struct {
	Key  string `json:"key"`
	Path string `json:"path"`
}

func Load(mappingFileLocation string) error {

	data, err := os.ReadFile(mappingFileLocation)
	if err != nil {
		return err
	}

	var allKeyPath []KeyPath

	if err := json.Unmarshal(data, &allKeyPath); err != nil {
		return fmt.Errorf("failed to parse mappings file: %w", err)
	}

	for _, keyPath := range allKeyPath {
		mappings[keyPath.Key] = keyPath.Path
	}

	return nil
}

func Lookup(routineKey string) (string, error) {
	path, ok := mappings[routineKey]
	if !ok {
		return "", fmt.Errorf("no mapping found for routineKey: %s", routineKey)
	}
	return path, nil
}
