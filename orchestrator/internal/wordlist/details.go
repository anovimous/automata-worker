package wordlist

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// RefsFromDetails reads the wordlists the core put in the details file. A file without any gives none.
func RefsFromDetails(detailsPath string) ([]Ref, error) {
	fields, err := readFields(detailsPath)
	if err != nil {
		return nil, err
	}

	raw, ok := fields["wordlists"]
	if !ok {
		return nil, nil
	}

	var refs []Ref
	if err := json.Unmarshal(raw, &refs); err != nil {
		return nil, fmt.Errorf("failed to read the wordlists of the details file: %w", err)
	}
	return refs, nil
}

// ResolveDetails rewrites the details file for the connectors: "wordlists" is replaced by
// "wordlistsPaths", the local paths in the same order. Every other field is kept as it is. The file is
// replaced in one step and keeps its restrictive mode (it holds credentials).
func ResolveDetails(detailsPath string, paths []string) error {
	fields, err := readFields(detailsPath)
	if err != nil {
		return err
	}

	if paths == nil {
		paths = []string{}
	}
	encoded, err := json.Marshal(paths)
	if err != nil {
		return fmt.Errorf("failed to encode the wordlist paths: %w", err)
	}

	delete(fields, "wordlists")
	fields["wordlistsPaths"] = encoded

	out, err := json.Marshal(fields)
	if err != nil {
		return fmt.Errorf("failed to encode the details file: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(detailsPath), "details-resolved-")
	if err != nil {
		return fmt.Errorf("failed to create the new details file: %w", err)
	}
	defer os.Remove(tmp.Name())

	if _, err := tmp.Write(out); err != nil {
		tmp.Close()
		return fmt.Errorf("failed to write the new details file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to write the new details file: %w", err)
	}
	// CreateTemp makes the file 0600
	if err := os.Rename(tmp.Name(), detailsPath); err != nil {
		return fmt.Errorf("failed to replace the details file: %w", err)
	}
	return nil
}

func readFields(detailsPath string) (map[string]json.RawMessage, error) {
	data, err := os.ReadFile(detailsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read the details file: %w", err)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("failed to parse the details file: %w", err)
	}
	if fields == nil {
		return nil, fmt.Errorf("the details file is not a JSON object")
	}
	return fields, nil
}
