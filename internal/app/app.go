// internal/app/app.go
package app

import (
	"strings"

	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/todoflow-labs/domain-processor/internal/config"
	"github.com/todoflow-labs/domain-processor/internal/processor"
	"github.com/todoflow-labs/domain-processor/internal/subscriber"
	"github.com/todoflow-labs/shared-dtos/logging"
	"github.com/todoflow-labs/shared-dtos/metrics"
)

func Run() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	logger := logging.New(cfg.LogLevel).With().Str("service", "domain-processor").Logger()
	logger.Info().Msg("domain-processor starting")

	metrics.Init(cfg.MetricsAddr)
	logger.Debug().Msgf("metrics server listening on %s", cfg.MetricsAddr)

	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("nats connect failed")
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("jetstream init failed")
	}

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
		logger.Debug().Msgf("stream %s ensured", stream.Name)
	}

	h := processor.NewProcessor(js, logger)
	subscriber.SubscribeToCommands(js, h, logger)

	logger.Info().Msg("domain-processor is running")
	select {}
}
