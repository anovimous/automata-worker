package config

import "testing"

func TestLoadReadsTheEnvironment(t *testing.T) {
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
