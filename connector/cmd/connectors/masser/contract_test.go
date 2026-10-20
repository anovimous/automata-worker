package main

import (
	"slices"
	"strings"
	"testing"

	"github.com/anovimous/automata-worker/connector/internal/parser"
)

func TestBuildCommandFromTheDetailsFileOfTheCore(t *testing.T) {
	details, err := parser.Parse("../../../../contract/samples/details-file-without-auth.json")
	if err != nil {
		t.Fatalf("Parse returned an error: %v", err)
	}

	command, args, output, err := BuildCommand(details, "/data/43.jsonl", "/tmp/job-43")
	if err != nil {
		t.Fatalf("BuildCommand returned an error: %v", err)
	}

	if command != "masser" {
		t.Errorf("command = %s", command)
	}
	joined := strings.Join(args, "\x00")
	for _, pair := range [][]string{{"-requests", "/data/43.jsonl"}, {"-output", output}, {"-rate", "10"}, {"-verbosity", "SUMMARIZED"}} {
		if !strings.Contains(joined, pair[0]+"\x00"+pair[1]) {
			t.Errorf("args %v miss %v", args, pair)
		}
	}
	if slices.Contains(args, "-wordlist") || slices.Contains(args, "-H") || slices.Contains(args, "-C") {
		t.Errorf("a job without wordlists and authentication must not have those flags: %v", args)
	}
}
