package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/joho/godotenv"
)

// DefaultWordlistCacheMaxBytes is the size of the wordlist cache when WORDLIST_CACHE_MAX_BYTES is not set (1.5 GB).
const DefaultWordlistCacheMaxBytes int64 = 1610612736

// DefaultTelemetryAddr is where the telemetry server listens when TELEMETRY_ADDR is not set.
const DefaultTelemetryAddr = "127.0.0.1:9464"

// the worker id names the worker in its status and logs; the monitor sets w-<host>-<6 chars>
var workerIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}$`)

func Load() (*Config, error) {

	godotenv.Load()

	cache, err := loadWordlistCache()
	if err != nil {
		return nil, err
	}

	wordlistsBucket := os.Getenv("RUSTFS_WORDLISTS_BUCKET")
	if wordlistsBucket == "" {
		return nil, fmt.Errorf("RUSTFS_WORDLISTS_BUCKET is not set")
	}

	workerID, err := loadWorkerID(os.Hostname)
	if err != nil {
		return nil, err
	}
	telemetryAddr, err := loadTelemetryAddr()
	if err != nil {
		return nil, err
	}
	logFormat, err := loadLogFormat()
	if err != nil {
		return nil, err
	}

	return &Config{
		RabbitMQ: RabbitMQConfig{
			URL:   os.Getenv("RABBITMQ_URL"),
			Queue: os.Getenv("RABBITMQ_QUEUE"),
		},
		RustFS: RustFSConfig{
			Endpoint:        os.Getenv("RUSTFS_ENDPOINT"),
			AccessKey:       os.Getenv("RUSTFS_ACCESS_KEY"),
			SecretKey:       os.Getenv("RUSTFS_SECRET_KEY"),
			JobsBucket:      os.Getenv("RUSTFS_JOBS_BUCKET"),
			ResultsBucket:   os.Getenv("RUSTFS_RESULTS_BUCKET"),
			WordlistsBucket: wordlistsBucket,
		},
		Central: CentralConfig{
			BaseURL:         os.Getenv("CENTRAL_BASE_URL"),
			PollInterval:    toInt(os.Getenv("CENTRAL_POLL_INTERVAL_SECONDS")),
			StateEndpoint:   os.Getenv("CENTRAL_STATE_ENDPOINT"),
			OutcomeEndpoint: os.Getenv("CENTRAL_OUTCOME_ENDPOINT"),
		},
		Worker: WorkerConfig{
			HeartbeatTimeout:  toInt(os.Getenv("WORKER_HEARTBEAT_TIMEOUT_SECONDS")),
			HeartbeatInterval: toInt(os.Getenv("WORKER_HEARTBEAT_INTERVAL_SECONDS")),
			SocketDir:         os.Getenv("WORKER_SOCKET_DIR"),
		},
		TempDir:       os.Getenv("TEMP_DIR"),
		WordlistCache: cache,
		Telemetry: TelemetryConfig{
			WorkerID:  workerID,
			Addr:      telemetryAddr,
			LogFormat: logFormat,
		},
	}, nil
}

// loadWorkerID reads WORKER_ID, the hostname when it is not set (a container's id by default). Like the
// wordlist cache settings a wrong value stops the start.
func loadWorkerID(hostname func() (string, error)) (string, error) {
	id := os.Getenv("WORKER_ID")
	if id == "" {
		h, err := hostname()
		if err != nil {
			return "", fmt.Errorf("WORKER_ID is not set and the hostname is unknown: %w", err)
		}
		id = h
	}
	if !workerIDPattern.MatchString(id) {
		return "", fmt.Errorf("WORKER_ID must match %s, got %q", workerIDPattern, id)
	}
	return id, nil
}

// loadTelemetryAddr reads TELEMETRY_ADDR, see ParseTelemetryAddr.
func loadTelemetryAddr() (string, error) {
	return ParseTelemetryAddr(os.Getenv("TELEMETRY_ADDR"))
}

// ParseTelemetryAddr reads a TELEMETRY_ADDR value: empty is DefaultTelemetryAddr, "off" is "" (no telemetry
// server), anything else must be host:port with a port from 1 to 65535.
func ParseTelemetryAddr(value string) (string, error) {
	switch value {
	case "":
		return DefaultTelemetryAddr, nil
	case "off":
		return "", nil
	}
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("TELEMETRY_ADDR must be host:port or off, got %q: %w", value, err)
	}
	if p, err := strconv.Atoi(port); err != nil || p < 1 || p > 65535 {
		return "", fmt.Errorf("TELEMETRY_ADDR must have a port from 1 to 65535, got %q", value)
	}
	return value, nil
}

// loadLogFormat reads LOG_FORMAT: text (the default) or json.
func loadLogFormat() (string, error) {
	switch format := os.Getenv("LOG_FORMAT"); format {
	case "", "text":
		return "text", nil
	case "json":
		return "json", nil
	default:
		return "", fmt.Errorf("LOG_FORMAT must be text or json, got %q", format)
	}
}

// loadWordlistCache reads WORDLIST_CACHE_DIR (an absolute directory) and WORDLIST_CACHE_MAX_BYTES (above 0,
// 1.5 GB when not set). Unlike the other numbers a wrong value is an error, not 0.
func loadWordlistCache() (WordlistCacheConfig, error) {
	dir := os.Getenv("WORDLIST_CACHE_DIR")
	if dir == "" {
		return WordlistCacheConfig{}, fmt.Errorf("WORDLIST_CACHE_DIR is not set")
	}
	if !filepath.IsAbs(dir) {
		return WordlistCacheConfig{}, fmt.Errorf("WORDLIST_CACHE_DIR must be an absolute path, got %q", dir)
	}

	maxBytes := DefaultWordlistCacheMaxBytes
	if raw := os.Getenv("WORDLIST_CACHE_MAX_BYTES"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed <= 0 {
			return WordlistCacheConfig{}, fmt.Errorf("WORDLIST_CACHE_MAX_BYTES must be a number above 0, got %q", raw)
		}
		maxBytes = parsed
	}

	return WordlistCacheConfig{Dir: dir, MaxBytes: maxBytes}, nil
}

func toInt(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

type Config struct {
	RabbitMQ RabbitMQConfig
	RustFS   RustFSConfig
	Central  CentralConfig
	Worker   WorkerConfig
	TempDir  string

	WordlistCache WordlistCacheConfig
	Telemetry     TelemetryConfig
}

// TelemetryConfig is what the worker reports about itself (docs/telemetry-contract.md).
type TelemetryConfig struct {
	WorkerID  string `json:"workerId"`
	Addr      string `json:"addr"`      // "" when disabled ("off")
	LogFormat string `json:"logFormat"` // "text" | "json"
}

type WordlistCacheConfig struct {
	Dir      string `json:"dir"`
	MaxBytes int64  `json:"maxBytes"`
}

type RabbitMQConfig struct {
	URL   string `json:"url"`
	Queue string `json:"queue"`
}

type RustFSConfig struct {
	Endpoint      string `json:"endpoint"`
	AccessKey     string `json:"accessKey"`
	SecretKey     string `json:"secretKey"`
	JobsBucket    string `json:"jobsBucket"`
	ResultsBucket string `json:"resultsBucket"`
	// WordlistsBucket holds the files of the wordlists, one object per wordlist named by its id
	WordlistsBucket string `json:"wordlistsBucket"`
}

type CentralConfig struct {
	BaseURL         string `json:"baseUrl"`
	PollInterval    int    `json:"pollIntervalSeconds"`
	StateEndpoint   string `json:"stateEndpoint"`
	OutcomeEndpoint string `json:"outcomeEndpoint"`
}

type WorkerConfig struct {
	HeartbeatTimeout  int    `json:"heartbeatTimeoutSeconds"`
	HeartbeatInterval int    `json:"heartbeatIntervalSeconds"`
	SocketDir         string `json:"socketDir"`
}
