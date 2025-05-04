package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog/log"

	"github.com/todoflow-labs/domain-processor/internal/config"
	"github.com/todoflow-labs/domain-processor/internal/logging"
	"github.com/todoflow-labs/shared-dtos/dto"
)

func main() {
	// Load config
	cfg, err := config.Load()
	if err != nil {
		log.Fatal().Err(err).Msg("failed to load config")
	}

	// Init logger
	logger := logging.New(cfg.LogLevel)
	logger.Info().Msg("domain-processor starting")

	// Connect DB
	db, err := pgxpool.New(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("db connect failed")
	}
	defer db.Close()

	// Connect NATS
	nc, err := nats.Connect(cfg.NATSURL)
	if err != nil {
		logger.Fatal().Err(err).Msg("nats connect failed")
	}
	js, err := nc.JetStream()
	if err != nil {
		logger.Fatal().Err(err).Msg("jetstream init failed")
	}

	// Ensure command and event streams exist
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "todo_commands",
		Subjects: []string{"todo.commands"},
	}); err != nil && !strings.Contains(err.Error(), "file already in use") {
		logger.Fatal().Err(err).Msg("failed to create commands stream")
	}
	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "todo_events",
		Subjects: []string{"todo.events"},
	}); err != nil && !strings.Contains(err.Error(), "file already in use") {
		logger.Fatal().Err(err).Msg("failed to create events stream")
	}

	// Subscribe to commands
	_, err = js.Subscribe("todo.commands", func(m *nats.Msg) {
		var base dto.BaseCommand
		if err := json.Unmarshal(m.Data, &base); err != nil {
			logger.Error().Err(err).Msg("invalid command envelope")
			m.Ack()
			return
		}
		logger.Debug().Msgf("Cmd received: %s", base.Type)

		switch base.Type {
		case dto.CreateTodoCmd:
			var cmd dto.CreateTodoCommand
			_ = json.Unmarshal(m.Data, &cmd)
			handleCreate(cmd, js, db, logger)
		case dto.UpdateTodoCmd:
			var cmd dto.UpdateTodoCommand
			_ = json.Unmarshal(m.Data, &cmd)
			handleUpdate(cmd, js, db, logger)
		case dto.DeleteTodoCmd:
			var cmd dto.DeleteTodoCommand
			_ = json.Unmarshal(m.Data, &cmd)
			handleDelete(cmd, js, db, logger)
		default:
			logger.Warn().Msgf("unknown command type: %s", base.Type)
		}
		m.Ack()
	},
		nats.Durable("domain-processor"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("subscribe failed")
	}

	// Block
	select {}
}

func handleCreate(cmd dto.CreateTodoCommand, js nats.JetStreamContext, db *pgxpool.Pool, log *logging.Logger) {
	r := db.QueryRow(context.Background(),
		`INSERT INTO todos.todo (title) VALUES ($1) RETURNING id, created_at`,
		cmd.Title,
	)
	
	var evt dto.TodoCreatedEvent
	evt.Type = dto.TodoCreatedEvt
	evt.Title = cmd.Title
	r.Scan(&evt.ID, &evt.Timestamp)
	data, _ := json.Marshal(evt)
	log.Debug().Msgf("Todo created: %s", evt.ID)
	if _, err := js.Publish("todo.events", data); err != nil {
		log.Error().Err(err).Msg("publish create event failed")
	}
}

func handleUpdate(cmd dto.UpdateTodoCommand, js nats.JetStreamContext, db *pgxpool.Pool, log *logging.Logger) {
	_, err := db.Exec(context.Background(),
		`UPDATE todos.todo SET title=$1, completed=$2 WHERE id=$3`,
		cmd.Title, cmd.Completed, cmd.ID,
	)
	if err != nil {
		log.Error().Err(err).Msg("db update failed")
		return
	}
	var evt dto.TodoUpdatedEvent
	evt.Type = dto.TodoUpdatedEvt
	evt.ID = cmd.ID
	evt.Title = func(s *string) string { if s!=nil { return *s }; return "" }(cmd.Title)
	evt.Completed = func(b *bool) bool { if b!=nil { return *b }; return false }(cmd.Completed)
	evt.Timestamp = time.Now()
	data, _ := json.Marshal(evt)
	log.Debug().Msgf("Todo updated: %s", evt.ID)
	if _, err := js.Publish("todo.events", data); err != nil {
		log.Error().Err(err).Msg("publish update event failed")
	}
}

func handleDelete(cmd dto.DeleteTodoCommand, js nats.JetStreamContext, db *pgxpool.Pool, log *logging.Logger) {
	_, err := db.Exec(context.Background(),
		`DELETE FROM todos.todo WHERE id=$1`, cmd.ID,
	)
	if err != nil {
		log.Error().Err(err).Msg("db delete failed")
		return
	}
	var evt dto.TodoDeletedEvent
	evt.Type = dto.TodoDeletedEvt
	evt.ID = cmd.ID
	evt.Timestamp = time.Now()
	data, _ := json.Marshal(evt)
	log.Debug().Msgf("Todo deleted: %s", evt.ID)
	if _, err := js.Publish("todo.events", data); err != nil {
		log.Error().Err(err).Msg("publish delete event failed")
	}
}