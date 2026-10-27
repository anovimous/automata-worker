package main

import (
	"os"
	"strings"
	"testing"

	"github.com/anovimous/automata-worker/connector/internal/parser"
)

func details(wordlists []string, customConfig string) *parser.JobDetails {
	return &parser.JobDetails{
		Rate:           10,
		WordListsPaths: wordlists,
		CustomConfig:   []byte(customConfig),
	}
}

const validConfig = `{"rawRequestBase64":"R0VUIC9GVVpaIEhUVFAvMS4x","flags":[]}`

func TestBuildCommandUsesTheWordlist(t *testing.T) {
	_, args, _, err := BuildCommand(details([]string{"/w/a.txt"}, validConfig), t.TempDir())
	if err != nil {
		t.Fatalf("BuildCommand returned an error: %v", err)
	}

	if !strings.Contains(strings.Join(args, "\x00"), "-w\x00/w/a.txt") {
		t.Errorf("args %v do not use the wordlist", args)
	}
}

func TestBuildCommandFailsForAJobWithMoreThanOneWordlist(t *testing.T) {
	tmp := t.TempDir()

	_, _, _, err := BuildCommand(details([]string{"/w/a.txt", "/w/b.txt"}, validConfig), tmp)

	if err == nil || !strings.Contains(err.Error(), "2 wordlists") {
		t.Errorf("error = %v, want one about the two wordlists", err)
	}
	if files, _ := os.ReadDir(tmp); len(files) != 0 {
		t.Errorf("a failed build must not leave files behind: %v", files)
	}
}

func TestBuildCommandFailsForAJobWithoutAWordlist(t *testing.T) {
	tmp := t.TempDir()

	_, _, _, err := BuildCommand(details([]string{}, validConfig), tmp)

	if err == nil || !strings.Contains(err.Error(), "no wordlist") {
		t.Errorf("error = %v, want one about the missing wordlist", err)
	}
	if files, _ := os.ReadDir(tmp); len(files) != 0 {
		t.Errorf("a failed build must not leave files behind: %v", files)
	}
}

func TestBuildCommandFailsForACustomConfigOfTheWrongForm(t *testing.T) {
	_, _, _, err := BuildCommand(details([]string{"/w/a.txt"}, `{"flags":"not a list"}`), t.TempDir())

	if err == nil {
		t.Error("expected an error, got none")
	}
}

func TestBuildCommandFailsForAFlagThatIsNotAllowed(t *testing.T) {
	config := `{"rawRequestBase64":"R0VU","flags":[{"key":"-o","value":"/etc/passwd"}]}`

	_, _, _, err := BuildCommand(details([]string{"/w/a.txt"}, config), t.TempDir())

	if err == nil || !strings.Contains(err.Error(), "Unknown flag") {
		t.Errorf("error = %v, want one about the unknown flag", err)
	}
}

func TestBuildCommandFailsForARequestThatIsNotBase64(t *testing.T) {
	tmp := t.TempDir()

	_, _, _, err := BuildCommand(details([]string{"/w/a.txt"}, `{"rawRequestBase64":"%%%","flags":[]}`), tmp)

	if err == nil || !strings.Contains(err.Error(), "base64") {
		t.Errorf("error = %v, want one about the base64 request", err)
	}
	if files, _ := os.ReadDir(tmp); len(files) != 0 {
		t.Errorf("a failed build must not leave files behind: %v", files)
	}
}
