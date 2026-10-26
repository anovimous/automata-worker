package wordlist

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/retry"
)

// fakeDownloader serves wordlists from memory and counts the downloads.
type fakeDownloader struct {
	objects map[int64][]byte
	calls   int
	// failures is how many of the first calls fail with a network-like error
	failures int
}

func (f *fakeDownloader) DownloadWordlist(ctx context.Context, id int64, w io.Writer) error {
	f.calls++
	if f.calls <= f.failures {
		return errors.New("connection reset")
	}
	data, ok := f.objects[id]
	if !ok {
		return errors.New("no such key")
	}
	_, err := w.Write(data)
	return err
}

func refOf(id int64, content string) Ref {
	sum := sha256.Sum256([]byte(content))
	return Ref{Id: id, Name: "list", Sha256: hex.EncodeToString(sum[:]), SizeBytes: int64(len(content))}
}

var noSleep = retry.Policy{Attempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond, Sleep: func(time.Duration) {}}

func newCache(t *testing.T, maxBytes int64, d *fakeDownloader) (*Cache, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "cache")
	c, err := New(dir, maxBytes, d, noSleep)
	if err != nil {
		t.Fatalf("New returned an error: %v", err)
	}
	return c, dir
}

func filesIn(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestEnsureDownloadsTheFileAndKeepsItUnderItsHash(t *testing.T) {
	ref := refOf(1, "admin\nlogin\n")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("admin\nlogin\n")}}
	c, dir := newCache(t, 1<<20, d)

	paths, release, err := c.Ensure(context.Background(), []Ref{ref})
	if err != nil {
		t.Fatalf("Ensure returned an error: %v", err)
	}
	defer release()

	if want := filepath.Join(dir, ref.Sha256); len(paths) != 1 || paths[0] != want {
		t.Fatalf("paths = %v, want [%s]", paths, want)
	}
	got, err := os.ReadFile(paths[0])
	if err != nil || string(got) != "admin\nlogin\n" {
		t.Errorf("content = %q, %v", got, err)
	}
	if info, _ := os.Stat(paths[0]); info.Mode().Perm() != 0o444 {
		t.Errorf("mode = %v, want 0444", info.Mode().Perm())
	}
	if names := filesIn(t, dir); len(names) != 1 {
		t.Errorf("cache directory = %v, want only the wordlist", names)
	}
}

func TestEnsureReturnsThePathsInTheOrderOfTheRefs(t *testing.T) {
	a, b := refOf(2, "bbb\n"), refOf(1, "aaa\n")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("aaa\n"), 2: []byte("bbb\n")}}
	c, dir := newCache(t, 1<<20, d)

	paths, release, err := c.Ensure(context.Background(), []Ref{a, b})
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if want := []string{filepath.Join(dir, a.Sha256), filepath.Join(dir, b.Sha256)}; paths[0] != want[0] || paths[1] != want[1] {
		t.Errorf("paths = %v, want %v", paths, want)
	}
}

func TestEnsureWithoutWordlistsGivesNoPaths(t *testing.T) {
	c, _ := newCache(t, 1<<20, &fakeDownloader{})

	paths, release, err := c.Ensure(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	release()

	if len(paths) != 0 {
		t.Errorf("paths = %v, want none", paths)
	}
}

func TestEnsureUsesTheCachedFileWithoutDownloading(t *testing.T) {
	ref := refOf(1, "admin\n")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("admin\n")}}
	c, _ := newCache(t, 1<<20, d)

	_, release, err := c.Ensure(context.Background(), []Ref{ref})
	if err != nil {
		t.Fatal(err)
	}
	release()
	_, release, err = c.Ensure(context.Background(), []Ref{ref})
	if err != nil {
		t.Fatal(err)
	}
	release()

	if d.calls != 1 {
		t.Errorf("downloads = %d, want 1", d.calls)
	}
}

func TestEnsureMarksACachedFileAsUsed(t *testing.T) {
	ref := refOf(1, "admin\n")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("admin\n")}}
	c, dir := newCache(t, 1<<20, d)
	_, release, _ := c.Ensure(context.Background(), []Ref{ref})
	release()
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, ref.Sha256), old, old); err != nil {
		t.Fatal(err)
	}

	_, release, err := c.Ensure(context.Background(), []Ref{ref})
	if err != nil {
		t.Fatal(err)
	}
	release()

	info, _ := os.Stat(filepath.Join(dir, ref.Sha256))
	if time.Since(info.ModTime()) > time.Hour {
		t.Errorf("modTime = %v, want a recent one", info.ModTime())
	}
}

func TestEnsureReplacesACachedFileOfTheWrongSize(t *testing.T) {
	ref := refOf(1, "admin\nlogin\n")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("admin\nlogin\n")}}
	c, dir := newCache(t, 1<<20, d)
	broken := filepath.Join(dir, ref.Sha256)
	if err := os.WriteFile(broken, []byte("admin"), 0o644); err != nil {
		t.Fatal(err)
	}

	paths, release, err := c.Ensure(context.Background(), []Ref{ref})
	if err != nil {
		t.Fatalf("Ensure returned an error: %v", err)
	}
	defer release()

	got, _ := os.ReadFile(paths[0])
	if string(got) != "admin\nlogin\n" || d.calls != 1 {
		t.Errorf("content = %q after %d downloads", got, d.calls)
	}
}

func TestEnsureFailsForAFileWithAnotherHashAndLeavesNothingBehind(t *testing.T) {
	ref := refOf(1, "admin\n")
	// same size, other content
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("other\n")}}
	c, dir := newCache(t, 1<<20, d)

	paths, release, err := c.Ensure(context.Background(), []Ref{ref})

	if err == nil || !strings.Contains(err.Error(), "sha256") {
		t.Fatalf("err = %v, want one about the sha256", err)
	}
	if paths != nil || release != nil {
		t.Errorf("paths = %v, release set = %v, want neither", paths, release != nil)
	}
	if d.calls != 3 {
		t.Errorf("downloads = %d, want 3 attempts", d.calls)
	}
	if names := filesIn(t, dir); len(names) != 0 {
		t.Errorf("cache directory = %v, want it empty", names)
	}
}

func TestEnsureFailsForAFileOfAnotherSize(t *testing.T) {
	ref := refOf(1, "admin\nlogin\n")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("admin\n")}}
	c, dir := newCache(t, 1<<20, d)

	_, _, err := c.Ensure(context.Background(), []Ref{ref})

	if err == nil || !strings.Contains(err.Error(), "expected 12") {
		t.Fatalf("err = %v, want one about the size", err)
	}
	if names := filesIn(t, dir); len(names) != 0 {
		t.Errorf("cache directory = %v, want it empty", names)
	}
}

func TestEnsureStopsAnObjectBiggerThanTheSizeOfTheCore(t *testing.T) {
	ref := refOf(1, "ab")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("abcdefgh")}}
	c, dir := newCache(t, 1<<20, d)

	_, _, err := c.Ensure(context.Background(), []Ref{ref})

	if err == nil || !strings.Contains(err.Error(), "bigger than the expected 2 bytes") {
		t.Fatalf("err = %v, want one about a too big object", err)
	}
	if names := filesIn(t, dir); len(names) != 0 {
		t.Errorf("cache directory = %v, want it empty", names)
	}
}

func TestEnsureRetriesATransientFailure(t *testing.T) {
	ref := refOf(1, "admin\n")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("admin\n")}, failures: 2}
	c, _ := newCache(t, 1<<20, d)

	paths, release, err := c.Ensure(context.Background(), []Ref{ref})
	if err != nil {
		t.Fatalf("Ensure returned an error: %v", err)
	}
	defer release()

	if d.calls != 3 || len(paths) != 1 {
		t.Errorf("downloads = %d, paths = %v", d.calls, paths)
	}
}

func TestEnsureGivesUpAfterTheAttemptsAndNamesTheWordlist(t *testing.T) {
	ref := refOf(7, "admin\n")
	ref.Name = "common"
	d := &fakeDownloader{objects: map[int64][]byte{7: []byte("admin\n")}, failures: 10}
	c, _ := newCache(t, 1<<20, d)

	_, _, err := c.Ensure(context.Background(), []Ref{ref})

	if err == nil || !strings.Contains(err.Error(), "wordlist 7 (common)") || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("err = %v, want one naming the wordlist and the cause", err)
	}
	if d.calls != 3 {
		t.Errorf("downloads = %d, want 3", d.calls)
	}
}

func TestEnsureDoesNotRetryACanceledContext(t *testing.T) {
	ref := refOf(1, "admin\n")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("admin\n")}}
	c, _ := newCache(t, 1<<20, d)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := c.Ensure(ctx, []Ref{ref})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the context error", err)
	}
	if d.calls != 0 {
		t.Errorf("downloads = %d, want none", d.calls)
	}
}

func TestEnsureRefusesAWordlistWithoutValidFacts(t *testing.T) {
	good := refOf(1, "admin\n")
	cases := map[string]Ref{
		"no id":          {Sha256: good.Sha256, SizeBytes: 6},
		"upper case":     {Id: 1, Sha256: strings.ToUpper(good.Sha256), SizeBytes: 6},
		"short hash":     {Id: 1, Sha256: "abc", SizeBytes: 6},
		"a path as hash": {Id: 1, Sha256: "../../etc/passwd", SizeBytes: 6},
		"no size":        {Id: 1, Sha256: good.Sha256},
	}
	for name, ref := range cases {
		t.Run(name, func(t *testing.T) {
			d := &fakeDownloader{objects: map[int64][]byte{1: []byte("admin\n")}}
			c, _ := newCache(t, 1<<20, d)

			if _, _, err := c.Ensure(context.Background(), []Ref{ref}); err == nil {
				t.Error("Ensure returned no error")
			}
			if d.calls != 0 {
				t.Errorf("downloads = %d, want none", d.calls)
			}
		})
	}
}

func TestEnsureFailureUnpinsTheFilesOfTheCall(t *testing.T) {
	a := refOf(1, "aaaa")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("aaaa")}}
	c, _ := newCache(t, 1<<20, d)

	// the second wordlist does not exist
	if _, _, err := c.Ensure(context.Background(), []Ref{a, refOf(2, "bbbb")}); err == nil {
		t.Fatal("Ensure returned no error")
	}

	if len(c.pinned) != 0 {
		t.Errorf("pinned = %v, want none", c.pinned)
	}
}

func setModTime(t *testing.T, path string, age time.Duration) {
	t.Helper()
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureRemovesTheLeastRecentlyUsedFilesToMakeRoom(t *testing.T) {
	oldest, middle, newest := refOf(1, "1111"), refOf(2, "2222"), refOf(3, "3333")
	incoming := refOf(4, "4444")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("1111"), 2: []byte("2222"), 3: []byte("3333"), 4: []byte("4444")}}
	c, dir := newCache(t, 12, d)
	for _, ref := range []Ref{oldest, middle, newest} {
		_, release, err := c.Ensure(context.Background(), []Ref{ref})
		if err != nil {
			t.Fatal(err)
		}
		release()
	}
	setModTime(t, filepath.Join(dir, oldest.Sha256), 3*time.Hour)
	setModTime(t, filepath.Join(dir, middle.Sha256), 2*time.Hour)
	setModTime(t, filepath.Join(dir, newest.Sha256), time.Hour)

	_, release, err := c.Ensure(context.Background(), []Ref{incoming})
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if _, err := os.Stat(filepath.Join(dir, oldest.Sha256)); !os.IsNotExist(err) {
		t.Errorf("the least recently used file is still there: %v", err)
	}
	for _, ref := range []Ref{middle, newest, incoming} {
		if _, err := os.Stat(filepath.Join(dir, ref.Sha256)); err != nil {
			t.Errorf("%s was removed: %v", ref, err)
		}
	}
}

func TestEnsureKeepsTheFilesOfTheJobEvenWhenTheyAreOverTheSize(t *testing.T) {
	a, b := refOf(1, "aaaaaaaa"), refOf(2, "bbbbbbbb")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("aaaaaaaa"), 2: []byte("bbbbbbbb")}}
	c, dir := newCache(t, 10, d)

	paths, release, err := c.Ensure(context.Background(), []Ref{a, b})
	if err != nil {
		t.Fatalf("Ensure returned an error: %v", err)
	}
	defer release()

	for _, path := range paths {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("%s is missing: %v", path, err)
		}
	}
	if names := filesIn(t, dir); len(names) != 2 {
		t.Errorf("cache directory = %v, want both files", names)
	}
}

func TestReleasedFilesCanBeRemovedByTheNextJob(t *testing.T) {
	old, next := refOf(1, "aaaaaaaa"), refOf(2, "bbbbbbbb")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("aaaaaaaa"), 2: []byte("bbbbbbbb")}}
	c, dir := newCache(t, 10, d)
	_, release, err := c.Ensure(context.Background(), []Ref{old})
	if err != nil {
		t.Fatal(err)
	}
	release()

	_, release, err = c.Ensure(context.Background(), []Ref{next})
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	if names := filesIn(t, dir); len(names) != 1 || names[0] != next.Sha256 {
		t.Errorf("cache directory = %v, want only the new file", names)
	}
}

func TestFilesOfTheRunningJobAreNotRemovedWhileItIsNotReleased(t *testing.T) {
	running, other := refOf(1, "aaaaaaaa"), refOf(2, "bbbbbbbb")
	d := &fakeDownloader{objects: map[int64][]byte{1: []byte("aaaaaaaa"), 2: []byte("bbbbbbbb")}}
	c, dir := newCache(t, 10, d)
	_, release, err := c.Ensure(context.Background(), []Ref{running})
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	// a second Ensure while the first is not released
	if _, release2, err := c.Ensure(context.Background(), []Ref{other}); err != nil {
		t.Fatal(err)
	} else {
		defer release2()
	}

	if _, err := os.Stat(filepath.Join(dir, running.Sha256)); err != nil {
		t.Errorf("a pinned file was removed: %v", err)
	}
}

func TestNewRemovesOnlyOldHalfDownloads(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("a", 64)
	oldPart := filepath.Join(dir, sha+".part-111")
	freshPart := filepath.Join(dir, sha+".part-222")
	kept := filepath.Join(dir, sha)
	for _, p := range []string{oldPart, freshPart, kept} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	setModTime(t, oldPart, 2*time.Hour)
	setModTime(t, kept, 2*time.Hour)

	if _, err := New(dir, 100, &fakeDownloader{}, noSleep); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(oldPart); !os.IsNotExist(err) {
		t.Errorf("the old half download is still there: %v", err)
	}
	for _, p := range []string{freshPart, kept} {
		if _, err := os.Stat(p); err != nil {
			t.Errorf("%s was removed: %v", filepath.Base(p), err)
		}
	}
}

func TestNewRefusesASizeBelowOne(t *testing.T) {
	if _, err := New(t.TempDir(), 0, &fakeDownloader{}, noSleep); err == nil {
		t.Error("New returned no error")
	}
}

func TestNewCreatesTheDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "a", "b")

	if _, err := New(dir, 10, &fakeDownloader{}, noSleep); err != nil {
		t.Fatalf("New returned an error: %v", err)
	}

	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Errorf("directory missing: %v", err)
	}
}

func TestRefNamesTheWordlistForAMessage(t *testing.T) {
	if got := (Ref{Id: 4}).String(); got != "wordlist 4" {
		t.Errorf("String = %q", got)
	}
	if got := (Ref{Id: 4, Name: "common"}).String(); got != "wordlist 4 (common)" {
		t.Errorf("String = %q", got)
	}
}
