package logging

import (
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type Logger = zerolog.Logger

// New creates a logger at the specified level ("debug","info","warn","error", etc.).
// If parsing fails, it falls back to InfoLevel.
func New(level string) *zerolog.Logger {
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)

	logger := log.With().Timestamp().Logger()
	return &logger
}