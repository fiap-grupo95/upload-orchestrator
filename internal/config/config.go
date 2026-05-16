package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port            string
	PostgresDSN     string
	MinioEndpoint   string
	MinioAccessKey  string
	MinioSecretKey  string
	MinioBucket     string
	MinioUseSSL     bool
	RabbitMQURL     string
	ProcessQueue    string
	ProcessingTopic string
	ReportTopic     string
}

func Load() (*Config, error) {
	ssl, _ := strconv.ParseBool(getEnv("MINIO_USE_SSL", "false"))

	cfg := &Config{
		Port:            getEnv("PORT", "8081"),
		PostgresDSN:     requireEnv("POSTGRES_DSN"),
		MinioEndpoint:   requireEnv("MINIO_ENDPOINT"),
		MinioAccessKey:  requireEnv("MINIO_ACCESS_KEY"),
		MinioSecretKey:  requireEnv("MINIO_SECRET_KEY"),
		MinioBucket:     getEnv("MINIO_BUCKET", "diagrams"),
		MinioUseSSL:     ssl,
		RabbitMQURL:     requireEnv("RABBITMQ_URL"),
		ProcessQueue:    getEnv("PROCESS_QUEUE", "process.queue"),
		ProcessingTopic: getEnv("PROCESSING_TOPIC", "processing.topic"),
		ReportTopic:     getEnv("REPORT_TOPIC", "report.topic"),
	}

	return cfg, nil
}

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("required env var %s is not set", key))
	}
	return v
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
