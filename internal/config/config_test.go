package config

import (
	"os"
	"testing"
)

func unsetenvWithCleanup(t *testing.T, key string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	os.Unsetenv(key)
	t.Cleanup(func() {
		if had {
			os.Setenv(key, prev)
		} else {
			os.Unsetenv(key)
		}
	})
}

func TestGetEnv_WhenVarSet_ReturnsValue(t *testing.T) {
	t.Setenv("_CONFIG_TEST_VAR", "hello")
	got := getEnv("_CONFIG_TEST_VAR", "default")
	if got != "hello" {
		t.Errorf("expected %q, got %q", "hello", got)
	}
}

func TestGetEnv_WhenVarNotSet_ReturnsDefault(t *testing.T) {
	unsetenvWithCleanup(t, "_CONFIG_TEST_MISSING")
	got := getEnv("_CONFIG_TEST_MISSING", "fallback")
	if got != "fallback" {
		t.Errorf("expected %q, got %q", "fallback", got)
	}
}

func TestGetEnv_WhenVarEmpty_ReturnsDefault(t *testing.T) {
	t.Setenv("_CONFIG_TEST_EMPTY", "")
	got := getEnv("_CONFIG_TEST_EMPTY", "default")
	if got != "default" {
		t.Errorf("expected %q when var is empty, got %q", "default", got)
	}
}

func TestRequireEnv_WhenVarSet_ReturnsValue(t *testing.T) {
	t.Setenv("_CONFIG_TEST_REQUIRED", "myvalue")
	got := requireEnv("_CONFIG_TEST_REQUIRED")
	if got != "myvalue" {
		t.Errorf("expected %q, got %q", "myvalue", got)
	}
}

func TestRequireEnv_WhenVarNotSet_Panics(t *testing.T) {
	unsetenvWithCleanup(t, "_CONFIG_TEST_MISSING_REQUIRED")
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic, but did not panic")
		}
	}()
	requireEnv("_CONFIG_TEST_MISSING_REQUIRED")
}

func setRequiredEnvs(t *testing.T) {
	t.Helper()
	t.Setenv("POSTGRES_DSN", "postgres://user:pass@localhost/db")
	t.Setenv("MINIO_ENDPOINT", "localhost:9000")
	t.Setenv("MINIO_ACCESS_KEY", "accesskey")
	t.Setenv("MINIO_SECRET_KEY", "secretkey")
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
}

func TestLoad_WithAllEnvVars_ReturnsPopulatedConfig(t *testing.T) {
	setRequiredEnvs(t)
	t.Setenv("PORT", "9090")
	t.Setenv("MINIO_BUCKET", "mybucket")
	t.Setenv("MINIO_USE_SSL", "true")
	t.Setenv("PROCESS_QUEUE", "my.queue")
	t.Setenv("PROCESSING_TOPIC", "my.topic")
	t.Setenv("REPORT_TOPIC", "my.report")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	checks := []struct {
		field    string
		got      string
		expected string
	}{
		{"Port", cfg.Port, "9090"},
		{"PostgresDSN", cfg.PostgresDSN, "postgres://user:pass@localhost/db"},
		{"MinioEndpoint", cfg.MinioEndpoint, "localhost:9000"},
		{"MinioAccessKey", cfg.MinioAccessKey, "accesskey"},
		{"MinioSecretKey", cfg.MinioSecretKey, "secretkey"},
		{"MinioBucket", cfg.MinioBucket, "mybucket"},
		{"RabbitMQURL", cfg.RabbitMQURL, "amqp://guest:guest@localhost:5672/"},
		{"ProcessQueue", cfg.ProcessQueue, "my.queue"},
		{"ProcessingTopic", cfg.ProcessingTopic, "my.topic"},
		{"ReportTopic", cfg.ReportTopic, "my.report"},
	}
	for _, c := range checks {
		if c.got != c.expected {
			t.Errorf("%s: expected %q, got %q", c.field, c.expected, c.got)
		}
	}
	if !cfg.MinioUseSSL {
		t.Error("MinioUseSSL: expected true, got false")
	}
}

func TestLoad_WithOnlyRequiredEnvVars_UsesDefaults(t *testing.T) {
	setRequiredEnvs(t)
	for _, k := range []string{"PORT", "MINIO_BUCKET", "MINIO_USE_SSL", "PROCESS_QUEUE", "PROCESSING_TOPIC", "REPORT_TOPIC"} {
		unsetenvWithCleanup(t, k)
	}

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	defaults := []struct {
		field    string
		got      string
		expected string
	}{
		{"Port", cfg.Port, "8081"},
		{"MinioBucket", cfg.MinioBucket, "diagrams"},
		{"ProcessQueue", cfg.ProcessQueue, "process.queue"},
		{"ProcessingTopic", cfg.ProcessingTopic, "processing.topic"},
		{"ReportTopic", cfg.ReportTopic, "report.topic"},
	}
	for _, c := range defaults {
		if c.got != c.expected {
			t.Errorf("default %s: expected %q, got %q", c.field, c.expected, c.got)
		}
	}
	if cfg.MinioUseSSL {
		t.Error("default MinioUseSSL: expected false, got true")
	}
}

func TestLoad_MinioUseSSL_ParsesInvalidValue_AsFalse(t *testing.T) {
	setRequiredEnvs(t)
	t.Setenv("MINIO_USE_SSL", "not-a-bool")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.MinioUseSSL {
		t.Error("invalid MINIO_USE_SSL should default to false")
	}
}

func TestLoad_WhenRequiredEnvMissing_Panics(t *testing.T) {
	unsetenvWithCleanup(t, "POSTGRES_DSN")
	unsetenvWithCleanup(t, "MINIO_ENDPOINT")
	unsetenvWithCleanup(t, "MINIO_ACCESS_KEY")
	unsetenvWithCleanup(t, "MINIO_SECRET_KEY")
	unsetenvWithCleanup(t, "RABBITMQ_URL")

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic when required env var is missing")
		}
	}()
	Load()
}
