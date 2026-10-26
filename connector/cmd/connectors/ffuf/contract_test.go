package main

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/anovimous/automata-worker/connector/internal/parser"
)

// the details file as the orchestrator hands it over: the core's file with the local wordlist paths
func TestBuildCommandFromTheDetailsFileOfTheCore(t *testing.T) {
	details, err := parser.Parse("../../../../contract/samples/details-file-resolved.json")
	if err != nil {
		t.Fatalf("Parse returned an error: %v", err)
	}
	tmp := t.TempDir()

	command, args, output, err := BuildCommand(details, tmp)
	if err != nil {
		t.Fatalf("BuildCommand returned an error: %v", err)
	}

	if command != "ffuf" {
		t.Errorf("command = %s", command)
	}
	joined := strings.Join(args, "\x00")
	for _, pair := range [][]string{{"-w", "/var/cache/automata/wordlists/ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}, {"-rate", "50"}, {"-b", "session=abc"}, {"-H", "Authorization: Bearer token"}, {"-H", "X-Test: 1"}, {"-mc", "200"}, {"-o", output}} {
		if !strings.Contains(joined, pair[0]+"\x00"+pair[1]) {
			t.Errorf("args %v miss %v", args, pair)
		}
	}

	i := slices.Index(args, "-request")
	if i < 0 {
		t.Fatalf("args %v have no -request", args)
	}
	if filepath.Dir(args[i+1]) != tmp {
		t.Errorf("the request file %s is not in the temp directory %s", args[i+1], tmp)
	}
	raw, err := os.ReadFile(args[i+1])
	if err != nil || string(raw) != "GET /FUZZ HTTP/1.1" {
		t.Errorf("request file = %q, %v", raw, err)
	}
}
