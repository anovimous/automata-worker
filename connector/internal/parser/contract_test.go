package parser

import (
	"path/filepath"
	"reflect"
	"testing"
)

// The details files are the samples which the core's tests write with its own
// code (contract/samples, a copy of src/test/resources/contract in the core), and
// details-file-resolved.json, the worker's own sample of what the orchestrator hands
// the connector: the file of the core with the paths of the downloaded wordlists.
const samples = "../../../contract/samples"

func TestParseDetailsFileWithAuthentication(t *testing.T) {
	got, err := Parse(filepath.Join(samples, "details-file-resolved.json"))
	if err != nil {
		t.Fatalf("Parse returned an error: %v", err)
	}

	if got.JobId != 42 || got.RoutineKey != "fuzz" || got.Rate != 50 || got.Verbosity != "NORMAL" || got.TargetType != "REQUEST" {
		t.Errorf("scalars not read: %+v", got)
	}
	if !reflect.DeepEqual(got.WordListsPaths, []string{"/var/cache/automata/wordlists/ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"}) {
		t.Errorf("wordlists = %v", got.WordListsPaths)
	}
	if want := []AuthNameValuePair{{Key: "Authorization", Value: "Bearer token"}}; !reflect.DeepEqual(got.Auth.Data.Headers, want) {
		t.Errorf("headers = %v", got.Auth.Data.Headers)
	}
	if want := []AuthNameValuePair{{Key: "session", Value: "abc"}}; !reflect.DeepEqual(got.Auth.Data.Cookies, want) {
		t.Errorf("cookies = %v", got.Auth.Data.Cookies)
	}
	if got.Auth.LifetimeInSeconds != "2026-10-20T10:00:00Z" || got.Auth.PopulatedAt != "2026-10-20T09:00:00Z" {
		t.Errorf("auth times = %q, %q", got.Auth.LifetimeInSeconds, got.Auth.PopulatedAt)
	}
	if want := []MatchAndReplace{{TargetType: TargetTypeHeader, Rule: RuleAdd, TargetKey: "X-Test", Value: "1"}}; !reflect.DeepEqual(got.GenericConfig.MatchAndReplace, want) {
		t.Errorf("matchAndReplace = %v", got.GenericConfig.MatchAndReplace)
	}
	if len(got.CustomConfig) == 0 {
		t.Error("customConfig is empty")
	}
}

// A connector started on the file of the core, which the orchestrator did not resolve, finds no wordlists
func TestTheDetailsFileOfTheCoreHasNoWordlistPathsYet(t *testing.T) {
	got, err := Parse(filepath.Join(samples, "details-file.json"))
	if err != nil {
		t.Fatalf("Parse returned an error: %v", err)
	}

	if got.WordListsPaths == nil || len(got.WordListsPaths) != 0 {
		t.Errorf("wordlists = %#v, want an empty list", got.WordListsPaths)
	}
}

func TestParseDetailsFileWithoutAuthentication(t *testing.T) {
	got, err := Parse(filepath.Join(samples, "details-file-without-auth.json"))
	if err != nil {
		t.Fatalf("Parse returned an error: %v", err)
	}

	if got.JobId != 43 || got.RoutineKey != "masser" || got.TargetType != "HOST" || got.Verbosity != "SUMMARIZED" {
		t.Errorf("scalars not read: %+v", got)
	}
	if got.WordListsPaths == nil || len(got.WordListsPaths) != 0 {
		t.Errorf("wordlists = %#v, want an empty list", got.WordListsPaths)
	}
	if got.Auth.Data.Headers == nil || got.Auth.Data.Cookies == nil || got.Auth.Data.QueryParameters == nil {
		t.Errorf("a null auth must give empty lists: %+v", got.Auth)
	}
}
