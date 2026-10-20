package config

import (
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

func Load() (config *Config, error error) {

	godotenv.Load()

	return &Config{
		RabbitMQ: RabbitMQConfig{
			URL:   os.Getenv("RABBITMQ_URL"),
			Queue: os.Getenv("RABBITMQ_QUEUE"),
		},
		RustFS: RustFSConfig{
			Endpoint:      os.Getenv("RUSTFS_ENDPOINT"),
			AccessKey:     os.Getenv("RUSTFS_ACCESS_KEY"),
			SecretKey:     os.Getenv("RUSTFS_SECRET_KEY"),
			JobsBucket:    os.Getenv("RUSTFS_JOBS_BUCKET"),
			ResultsBucket: os.Getenv("RUSTFS_RESULTS_BUCKET"),
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
		TempDir: os.Getenv("TEMP_DIR"),
	}, nil
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
