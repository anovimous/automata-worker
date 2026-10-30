package config

import (
	"errors"
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

func TestLoadReadsTheTelemetrySettings(t *testing.T) {
	setRequired(t)
	t.Setenv("WORKER_ID", "w-host1-k3f9qa")
	t.Setenv("TELEMETRY_ADDR", "0.0.0.0:9464")
	t.Setenv("LOG_FORMAT", "json")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := TelemetryConfig{WorkerID: "w-host1-k3f9qa", Addr: "0.0.0.0:9464", LogFormat: "json"}
	if cfg.Telemetry != want {
		t.Errorf("Telemetry = %+v, want %+v", cfg.Telemetry, want)
	}
}

func TestLoadFailsForAWrongTelemetrySetting(t *testing.T) {
	for key, value := range map[string]string{"WORKER_ID": "-x", "TELEMETRY_ADDR": "9464", "LOG_FORMAT": "xml"} {
		t.Run(key, func(t *testing.T) {
			setRequired(t)
			t.Setenv(key, value)

			if _, err := Load(); err == nil || !strings.Contains(err.Error(), key) {
				t.Errorf("err = %v, want one naming %s", err, key)
			}
		})
	}
}

func TestLoadWorkerID(t *testing.T) {
	hostname := func() (string, error) { return "6f1c2b9a0d3e", nil }

	tests := []struct {
		name, env, want string
		wantErr         bool
	}{
		{"the hostname by default", "", "6f1c2b9a0d3e", false},
		{"the monitor's id", "w-host1-k3f9qa", "w-host1-k3f9qa", false},
		{"dots and underscores", "compose_worker.1", "compose_worker.1", false},
		{"63 characters", strings.Repeat("a", 63), strings.Repeat("a", 63), false},
		{"64 characters", strings.Repeat("a", 64), "", true},
		{"a leading dash", "-w", "", true},
		{"a space", "w 1", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("WORKER_ID", tt.env)

			got, err := loadWorkerID(hostname)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("loadWorkerID() = %q, %v, want %q, error %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestLoadWorkerIDFailsWithoutAHostname(t *testing.T) {
	t.Setenv("WORKER_ID", "")

	if _, err := loadWorkerID(func() (string, error) { return "", errors.New("no hostname") }); err == nil {
		t.Error("expected an error")
	}
}

func TestParseTelemetryAddr(t *testing.T) {
	tests := []struct {
		value, want string
		wantErr     bool
	}{
		{"", "127.0.0.1:9464", false},
		{"off", "", false},
		{"0.0.0.0:9464", "0.0.0.0:9464", false},
		{"127.0.0.1:19100", "127.0.0.1:19100", false},
		{":9464", ":9464", false},
		{"[::1]:9464", "[::1]:9464", false},
		{"9464", "", true},
		{"127.0.0.1:0", "", true},
		{"127.0.0.1:65536", "", true},
		{"127.0.0.1:http", "", true},
		{"OFF", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			got, err := ParseTelemetryAddr(tt.value)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("ParseTelemetryAddr(%q) = %q, %v, want %q, error %v", tt.value, got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestLoadTelemetryAddrReadsTheEnvironment(t *testing.T) {
	t.Setenv("TELEMETRY_ADDR", "off")

	if got, err := loadTelemetryAddr(); err != nil || got != "" {
		t.Errorf("loadTelemetryAddr() = %q, %v, want disabled", got, err)
	}
}

func TestLoadLogFormat(t *testing.T) {
	tests := []struct {
		env, want string
		wantErr   bool
	}{
		{"", "text", false},
		{"text", "text", false},
		{"json", "json", false},
		{"JSON", "", true},
		{"logfmt", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.env, func(t *testing.T) {
			t.Setenv("LOG_FORMAT", tt.env)

			got, err := loadLogFormat()
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("loadLogFormat() = %q, %v, want %q, error %v", got, err, tt.want, tt.wantErr)
			}
		})
	}
}
