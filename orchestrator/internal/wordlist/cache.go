// Package wordlist gives the connectors the wordlist files of a job on the local disk.
//
// The core stores every wordlist as an object in the wordlists bucket, which never changes after it is
// created, and puts its SHA-256 and size in the details file. The files are kept in a cache directory
// under their SHA-256, so a cached file is never stale and a job needs no network when it has them all.
// There is no time limit on a cached file: the least recently used ones are removed only when the
// cache is over its size.
//
// Use one cache directory per orchestrator process. A file removed to make room could otherwise be one
// which another process has handed to its tool but not yet opened.
package wordlist

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	log "log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/anovimous/automata-worker/orchestrator/internal/retry"
)

// Ref is a wordlist as the core writes it in the details file.
type Ref struct {
	Id        int64  `json:"id"`
	Name      string `json:"name"`
	Sha256    string `json:"sha256"`
	SizeBytes int64  `json:"sizeBytes"`
}

// String names the wordlist for a message.
func (r Ref) String() string {
	if r.Name == "" {
		return fmt.Sprintf("wordlist %d", r.Id)
	}
	return fmt.Sprintf("wordlist %d (%s)", r.Id, r.Name)
}

// Downloader writes the file of a wordlist (the object with its id) to w.
type Downloader interface {
	DownloadWordlist(ctx context.Context, id int64, w io.Writer) error
}

// leftover download files older than this are removed at startup
const staleDownloadAge = time.Hour

var sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// Cache is the directory of the wordlist files.
type Cache struct {
	dir        string
	maxBytes   int64
	downloader Downloader
	retry      retry.Policy

	mu     sync.Mutex
	pinned map[string]int
}

// New makes the cache directory if it is missing and removes the half downloaded files an earlier run
// left behind. retryPolicy is used around each download.
func New(dir string, maxBytes int64, downloader Downloader, retryPolicy retry.Policy) (*Cache, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("the wordlist cache size must be above 0, got %d", maxBytes)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("failed to create the wordlist cache directory %s: %w", dir, err)
	}

	c := &Cache{dir: dir, maxBytes: maxBytes, downloader: downloader, retry: retryPolicy, pinned: map[string]int{}}
	c.removeStaleDownloads(time.Now())
	return c, nil
}

// Ensure makes every wordlist available in the cache and returns their paths, in the order of refs.
// The files stay pinned (never removed to make room) until release is called, so call it when the job is
// over. On an error nothing stays pinned and release is nil.
func (c *Cache) Ensure(ctx context.Context, refs []Ref) (paths []string, release func(), err error) {
	for _, ref := range refs {
		if err := validate(ref); err != nil {
			return nil, nil, err
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.pin(refs)
	unpin := func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.unpin(refs)
	}

	paths = make([]string, 0, len(refs))
	for _, ref := range refs {
		path, err := c.ensureOne(ctx, ref)
		if err != nil {
			c.unpin(refs)
			return nil, nil, err
		}
		paths = append(paths, path)
	}

	return paths, unpin, nil
}

func validate(ref Ref) error {
	if ref.Id <= 0 {
		return fmt.Errorf("%s has no valid id", ref)
	}
	if !sha256Pattern.MatchString(ref.Sha256) {
		return fmt.Errorf("%s has no valid sha256", ref)
	}
	if ref.SizeBytes <= 0 {
		return fmt.Errorf("%s has no valid size", ref)
	}
	return nil
}

func (c *Cache) pin(refs []Ref) {
	for _, ref := range refs {
		c.pinned[ref.Sha256]++
	}
}

func (c *Cache) unpin(refs []Ref) {
	for _, ref := range refs {
		if c.pinned[ref.Sha256] <= 1 {
			delete(c.pinned, ref.Sha256)
		} else {
			c.pinned[ref.Sha256]--
		}
	}
}

func (c *Cache) pathOf(sha string) string {
	return filepath.Join(c.dir, sha)
}

func (c *Cache) ensureOne(ctx context.Context, ref Ref) (string, error) {
	path := c.pathOf(ref.Sha256)

	if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() && info.Size() == ref.SizeBytes {
		// the use is what makes it recent for the removal order
		now := time.Now()
		if err := os.Chtimes(path, now, now); err != nil {
			log.Warn("failed to mark a cached wordlist as used", "path", path, "error", err)
		}
		return path, nil
	}

	c.makeRoomFor(ref.SizeBytes)

	err := c.retry.Do(func() error {
		if err := ctx.Err(); err != nil {
			return retry.Permanent(err)
		}
		return c.download(ctx, ref)
	})
	if err != nil {
		return "", fmt.Errorf("failed to get %s: %w", ref, err)
	}

	return path, nil
}

// download writes the object to a temporary file next to the final one, checks it and renames it, so
// the final name is never a half file.
func (c *Cache) download(ctx context.Context, ref Ref) error {
	part, err := os.CreateTemp(c.dir, ref.Sha256+".part-")
	if err != nil {
		return fmt.Errorf("failed to create the download file: %w", err)
	}
	defer os.Remove(part.Name())
	defer part.Close()

	hash := sha256.New()
	written, err := c.copyObject(ctx, ref, io.MultiWriter(part, hash))
	if err != nil {
		return err
	}
	if written != ref.SizeBytes {
		return fmt.Errorf("downloaded %d bytes, expected %d", written, ref.SizeBytes)
	}
	if got := hex.EncodeToString(hash.Sum(nil)); got != ref.Sha256 {
		return fmt.Errorf("the downloaded file has sha256 %s, expected %s", got, ref.Sha256)
	}
	if err := part.Sync(); err != nil {
		return fmt.Errorf("failed to write the download file: %w", err)
	}
	if err := part.Chmod(0o444); err != nil {
		return fmt.Errorf("failed to set the mode of the download file: %w", err)
	}
	if err := part.Close(); err != nil {
		return fmt.Errorf("failed to close the download file: %w", err)
	}

	if err := os.Rename(part.Name(), c.pathOf(ref.Sha256)); err != nil {
		return fmt.Errorf("failed to move the download into the cache: %w", err)
	}
	return nil
}

func (c *Cache) copyObject(ctx context.Context, ref Ref, w io.Writer) (int64, error) {
	counter := &countingWriter{w: w, limit: ref.SizeBytes}
	if err := c.downloader.DownloadWordlist(ctx, ref.Id, counter); err != nil {
		return counter.n, err
	}
	return counter.n, nil
}

// countingWriter counts the bytes and stops a download which is bigger than the size the core gave, so a
// wrong object can not fill the disk.
type countingWriter struct {
	w     io.Writer
	n     int64
	limit int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	if c.n+int64(len(p)) > c.limit {
		return 0, fmt.Errorf("the object is bigger than the expected %d bytes", c.limit)
	}
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

type cachedFile struct {
	path    string
	sha     string
	size    int64
	modTime time.Time
}

func (c *Cache) cachedFiles() []cachedFile {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		log.Error("failed to read the wordlist cache directory", "dir", c.dir, "error", err)
		return nil
	}

	var files []cachedFile
	for _, entry := range entries {
		if !sha256Pattern.MatchString(entry.Name()) {
			continue
		}
		info, err := entry.Info()
		if err != nil || !info.Mode().IsRegular() {
			continue
		}
		files = append(files, cachedFile{path: filepath.Join(c.dir, entry.Name()), sha: entry.Name(), size: info.Size(), modTime: info.ModTime()})
	}
	return files
}

// makeRoomFor removes the least recently used files, never a pinned one, until a file of need bytes fits
// in the cache. When only pinned files are left it stops: a job whose wordlists alone are bigger than the
// cache still runs.
func (c *Cache) makeRoomFor(need int64) {
	files := c.cachedFiles()

	var total int64
	for _, f := range files {
		total += f.size
	}
	if total+need <= c.maxBytes {
		return
	}

	sort.Slice(files, func(i, j int) bool { return files[i].modTime.Before(files[j].modTime) })

	for _, f := range files {
		if total+need <= c.maxBytes {
			return
		}
		if c.pinned[f.sha] > 0 {
			continue
		}
		if err := os.Remove(f.path); err != nil {
			log.Error("failed to remove a cached wordlist", "path", f.path, "error", err)
			continue
		}
		log.Info("removed a cached wordlist to make room", "sha256", f.sha, "bytes", f.size)
		total -= f.size
	}

	if total+need > c.maxBytes {
		log.Warn("the wordlist cache is over its size, only files of the running job are left", "bytes", total+need, "max", c.maxBytes)
	}
}

func (c *Cache) removeStaleDownloads(now time.Time) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !strings.Contains(entry.Name(), ".part-") {
			continue
		}
		info, err := entry.Info()
		if err != nil || now.Sub(info.ModTime()) < staleDownloadAge {
			continue
		}
		if err := os.Remove(filepath.Join(c.dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Error("failed to remove a stale download", "name", entry.Name(), "error", err)
		}
	}
}
