package config

import (
	"fmt"
	"strings"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	NATSURL     string `mapstructure:"nats-url"`
	LogLevel    string `mapstructure:"log-level"`
	MetricsAddr string `mapstructure:"metrics-addr"`
}

func Load() (*Config, error) {
	// Optional flag for config file
	pflag.String("config", "config.yaml", "Path to config file")

	// CLI flags (override everything)
	pflag.String("nats-url", "", "NATS server URL")
	pflag.String("database-url", "", "Database connection URL")
	pflag.String("log-level", "", "Log level")
	pflag.String("metrics-addr", "", "Metrics listen address")
	pflag.Parse()

	// Bind flags
	if err := viper.BindPFlags(pflag.CommandLine); err != nil {
		return nil, err
	}

	// Support ENV like NATS_URL
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()

	// Load from YAML file (if it exists)
	viper.SetConfigFile(viper.GetString("config"))
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("No config file found, continuing with flags/env: %v\n", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	// Validate
	if cfg.NATSURL == "" {
		return nil, fmt.Errorf("nats-url must be set")
	}

	return &cfg, nil
}
