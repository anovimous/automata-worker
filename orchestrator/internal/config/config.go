package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/joho/godotenv"
)

// DefaultWordlistCacheMaxBytes is the size of the wordlist cache when WORDLIST_CACHE_MAX_BYTES is not set (1.5 GB).
const DefaultWordlistCacheMaxBytes int64 = 1610612736

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
	}, nil
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
