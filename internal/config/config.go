package config

import (
	"flag"
	"os"
)

type Config struct {
	NATSURL     string
	DatabaseURL string
	LogLevel    string
	MetricsAddr string
}

func Load() (*Config, error) {
	cfg := &Config{}
	flag.StringVar(&cfg.NATSURL, "nats-url", os.Getenv("NATS_URL"), "NATS server URL")
	flag.StringVar(&cfg.DatabaseURL, "db-url", os.Getenv("DATABASE_URL"), "Database connection URL")
	flag.StringVar(&cfg.LogLevel, "log-level", "info", "Log level")
	flag.StringVar(&cfg.MetricsAddr, "metrics-addr", ":9091", "Metrics listen address")
	flag.Parse()
	return cfg, nil
}
