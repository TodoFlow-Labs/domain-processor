// main.go
package main

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/todoflow-labs/domain-processor/internal/config"
	"github.com/todoflow-labs/domain-processor/internal/processor"
	"github.com/todoflow-labs/domain-processor/internal/subscriber"
	"github.com/todoflow-labs/shared-dtos/logging"
	"github.com/todoflow-labs/shared-dtos/metrics"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}
	// Initialize logger
	logger := logging.New(cfg.LogLevel)
	logger.Info().Msg("domain-processor starting")

	// Initialize metrics
	metrics.Init(cfg.MetricsAddr)

	// Connect to PostgreSQL
	db, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("db connect failed")
	}
	defer db.Close()

	// Initialize database schema
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("nats connect failed")
	}
	defer nc.Close()
	// Initialize JetStream
	js, err := nc.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("jetstream init failed")
	}
	// Ensure JetStream stream exists
	for _, stream := range []struct {
		Name     string
		Subjects []string
	}{
		{"todo_commands", []string{"todo.commands"}},
		{"todo_events", []string{"todo.events"}},
	} {
		if _, err := js.AddStream(&nats.StreamConfig{
			Name: stream.Name, Subjects: stream.Subjects,
		}); err != nil && !strings.Contains(err.Error(), "file already in use") {
			logger.Fatal().Err(err).Msgf("failed to create stream %s", stream.Name)
		}
	}

	// initialize subscribers
	h := processor.NewProcessor(js, db, logger)
	subscriber.SubscribeToCommands(js, h, logger)

	select {}
}
