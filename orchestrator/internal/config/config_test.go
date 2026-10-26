package config

import (
	"strings"
	"testing"
)

// setRequired sets the variables Load can not do without
func setRequired(t *testing.T) {
	t.Helper()
	t.Setenv("RUSTFS_WORDLISTS_BUCKET", "wordlists")
	t.Setenv("WORDLIST_CACHE_DIR", "/var/cache/automata/wordlists")
}

func TestLoadReadsTheEnvironment(t *testing.T) {
	setRequired(t)
	t.Setenv("RABBITMQ_QUEUE", "jobs.http.queue")
	t.Setenv("CENTRAL_POLL_INTERVAL_SECONDS", "7")
	t.Setenv("TEMP_DIR", "/var/tmp/x")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.RabbitMQ.Queue != "jobs.http.queue" || cfg.Central.PollInterval != 7 || cfg.TempDir != "/var/tmp/x" {
		t.Errorf("unexpected config %+v", cfg)
	}
}

func TestLoadReadsTheWordlistSettings(t *testing.T) {
	setRequired(t)
	t.Setenv("WORDLIST_CACHE_MAX_BYTES", "2048")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.RustFS.WordlistsBucket != "wordlists" || cfg.WordlistCache.Dir != "/var/cache/automata/wordlists" || cfg.WordlistCache.MaxBytes != 2048 {
		t.Errorf("unexpected config %+v", cfg)
	}
}

func TestTheWordlistCacheIsOnePointFiveGigabytesByDefault(t *testing.T) {
	setRequired(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.WordlistCache.MaxBytes != 1610612736 {
		t.Errorf("MaxBytes = %d, want 1610612736", cfg.WordlistCache.MaxBytes)
	}
}

func TestLoadFailsWithoutTheWordlistsBucket(t *testing.T) {
	setRequired(t)
	t.Setenv("RUSTFS_WORDLISTS_BUCKET", "")

	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RUSTFS_WORDLISTS_BUCKET") {
		t.Errorf("err = %v, want one naming RUSTFS_WORDLISTS_BUCKET", err)
	}
}

func TestLoadFailsWithoutAnAbsoluteCacheDirectory(t *testing.T) {
	for name, dir := range map[string]string{"missing": "", "relative": "cache/wordlists"} {
		t.Run(name, func(t *testing.T) {
			setRequired(t)
			t.Setenv("WORDLIST_CACHE_DIR", dir)

			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "WORDLIST_CACHE_DIR") {
				t.Errorf("err = %v, want one naming WORDLIST_CACHE_DIR", err)
			}
		})
	}
}

func TestLoadFailsForACacheSizeThatIsNotAPositiveNumber(t *testing.T) {
	for _, value := range []string{"0", "-5", "abc", "1.5GB"} {
		t.Run(value, func(t *testing.T) {
			setRequired(t)
			t.Setenv("WORDLIST_CACHE_MAX_BYTES", value)

			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "WORDLIST_CACHE_MAX_BYTES") {
				t.Errorf("err = %v, want one naming WORDLIST_CACHE_MAX_BYTES", err)
			}
		})
	}
}
