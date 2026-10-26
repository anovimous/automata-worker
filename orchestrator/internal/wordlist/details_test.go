package wordlist

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// The details files are the samples which the core's tests write with its own code, and the worker's
// own sample of the form the connectors get (contract/samples).
const samples = "../../../contract/samples"

func copyOf(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(samples, name))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "details-42.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func decode(t *testing.T, data []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRefsAreReadFromTheSampleOfTheCore(t *testing.T) {
	refs, err := RefsFromDetails(copyOf(t, "details-file.json"))
	if err != nil {
		t.Fatalf("RefsFromDetails returned an error: %v", err)
	}

	want := []Ref{{Id: 7, Name: "common", Sha256: "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", SizeBytes: 1234}}
	if !reflect.DeepEqual(refs, want) {
		t.Errorf("refs = %+v, want %+v", refs, want)
	}
}

func TestAJobWithoutWordlistsHasNoRefs(t *testing.T) {
	refs, err := RefsFromDetails(copyOf(t, "details-file-without-auth.json"))
	if err != nil {
		t.Fatalf("RefsFromDetails returned an error: %v", err)
	}

	if len(refs) != 0 {
		t.Errorf("refs = %+v, want none", refs)
	}
}

func TestAWordlistWithoutANameIsRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.json")
	os.WriteFile(path, []byte(`{"wordlists":[{"id":3,"name":null,"sha256":"aa","sizeBytes":5}]}`), 0o600)

	refs, err := RefsFromDetails(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(refs) != 1 || refs[0].Name != "" || refs[0].Id != 3 {
		t.Errorf("refs = %+v", refs)
	}
}

func TestRefsFromAnUnreadableFileFail(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	os.WriteFile(bad, []byte("not json"), 0o600)
	null := filepath.Join(dir, "null.json")
	os.WriteFile(null, []byte("null"), 0o600)
	wrongType := filepath.Join(dir, "wrong.json")
	os.WriteFile(wrongType, []byte(`{"wordlists":"x"}`), 0o600)

	for _, path := range []string{filepath.Join(dir, "missing.json"), bad, null, wrongType} {
		if _, err := RefsFromDetails(path); err == nil {
			t.Errorf("RefsFromDetails(%s) returned no error", filepath.Base(path))
		}
	}
}

func TestResolvingTheSampleOfTheCoreGivesTheSampleOfTheConnectors(t *testing.T) {
	path := copyOf(t, "details-file.json")
	cached := "/var/cache/automata/wordlists/ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"

	if err := ResolveDetails(path, []string{cached}); err != nil {
		t.Fatalf("ResolveDetails returned an error: %v", err)
	}

	got, _ := os.ReadFile(path)
	want, _ := os.ReadFile(filepath.Join(samples, "details-file-resolved.json"))
	if !reflect.DeepEqual(decode(t, got), decode(t, want)) {
		t.Errorf("resolved file = %s\nwant %s", got, want)
	}
}

func TestResolveKeepsEveryOtherFieldAsItWas(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.json")
	original := `{"jobId":1,"customConfig":{"flags":[{"key":"-mc","value":"200"}],"n":1.50},"auth":null,"wordlists":[{"id":1}]}`
	os.WriteFile(path, []byte(original), 0o600)

	if err := ResolveDetails(path, []string{"/a", "/b"}); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(path)
	m := map[string]json.RawMessage{}
	json.Unmarshal(data, &m)
	if string(m["customConfig"]) != `{"flags":[{"key":"-mc","value":"200"}],"n":1.50}` {
		t.Errorf("customConfig = %s", m["customConfig"])
	}
	if string(m["auth"]) != "null" || string(m["jobId"]) != "1" {
		t.Errorf("fields = %s", data)
	}
	if string(m["wordlistsPaths"]) != `["/a","/b"]` {
		t.Errorf("wordlistsPaths = %s", m["wordlistsPaths"])
	}
	if _, ok := m["wordlists"]; ok {
		t.Errorf("wordlists is still there: %s", data)
	}
}

func TestResolveWithoutPathsWritesAnEmptyList(t *testing.T) {
	path := copyOf(t, "details-file-without-auth.json")

	if err := ResolveDetails(path, nil); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(path)
	if got := decode(t, data)["wordlistsPaths"]; !reflect.DeepEqual(got, []any{}) {
		t.Errorf("wordlistsPaths = %#v, want an empty list", got)
	}
}

func TestResolveKeepsTheFileClosedToOthers(t *testing.T) {
	path := copyOf(t, "details-file.json")

	if err := ResolveDetails(path, []string{"/a"}); err != nil {
		t.Fatal(err)
	}

	if info, _ := os.Stat(path); info.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v, want 0600", info.Mode().Perm())
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 1 {
		t.Errorf("directory has %d entries, want only the details file", len(entries))
	}
}

func TestResolveOfAFileThatIsNotAnObjectFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "d.json")
	os.WriteFile(path, []byte("[1]"), 0o600)

	if err := ResolveDetails(path, []string{"/a"}); err == nil {
		t.Error("ResolveDetails returned no error")
	}
	if err := ResolveDetails(filepath.Join(t.TempDir(), "missing.json"), nil); err == nil {
		t.Error("ResolveDetails of a missing file returned no error")
	}
}
