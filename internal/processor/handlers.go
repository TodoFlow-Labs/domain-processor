// internal/processor/handlers.go
package processor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type CommandHandler interface {
	HandleCreate(dto.CreateTodoCommand)
	HandleUpdate(dto.UpdateTodoCommand)
	HandleDelete(dto.DeleteTodoCommand)
}

type DBExecutor interface {
	Exec(context.Context, string, ...any) (any, error)
	QueryRow(context.Context, string, ...any) RowScanner
}

type RowScanner interface {
	Scan(dest ...any) error
}

type Processor struct {
	js     nats.JetStreamContext
	db     DBExecutor
	logger *logging.Logger
}

func NewProcessor(js nats.JetStreamContext, db DBExecutor, logger *logging.Logger) *Processor {
	return &Processor{js: js, db: db, logger: logger}
}

func (p *Processor) HandleCreate(cmd dto.CreateTodoCommand) {
	r := p.db.QueryRow(context.Background(),
		`INSERT INTO todos.todo (title) VALUES ($1) RETURNING id, created_at`,
		cmd.Title,
	)

	var evt dto.TodoCreatedEvent
	evt.Type = dto.TodoCreatedEvt
	evt.Title = cmd.Title

	if err := r.Scan(&evt.ID, &evt.Timestamp); err != nil {
		p.logger.Error().Err(err).Msg("db insert failed")
		return
	}
	dat, _ := json.Marshal(evt)
	p.logger.Debug().Msgf("Todo created: %s", evt.ID)
	if _, err := p.js.Publish("todo.events", dat); err != nil {
		p.logger.Error().Err(err).Msg("publish create event failed")
	}
}

func (p *Processor) HandleUpdate(cmd dto.UpdateTodoCommand) {
	_, err := p.db.Exec(context.Background(),
		`UPDATE todos.todo SET title=$1, completed=$2 WHERE id=$3`,
		cmd.Title, cmd.Completed, cmd.ID,
	)
	if err != nil {
		p.logger.Error().Err(err).Msg("db update failed")
		return
	}

	evt := dto.TodoUpdatedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoUpdatedEvt,
			ID:        cmd.ID,
			Timestamp: time.Now(),
		},
		Title:     derefString(cmd.Title),
		Completed: derefBool(cmd.Completed),
	}
	dat, _ := json.Marshal(evt)
	p.logger.Debug().Msgf("Todo updated: %s", evt.ID)
	if _, err := p.js.Publish("todo.events", dat); err != nil {
		p.logger.Error().Err(err).Msg("publish update event failed")
	}
}

func (p *Processor) HandleDelete(cmd dto.DeleteTodoCommand) {
	_, err := p.db.Exec(context.Background(),
		`DELETE FROM todos.todo WHERE id=$1`, cmd.ID,
	)
	if err != nil {
		p.logger.Error().Err(err).Msg("db delete failed")
		return
	}
	evt := dto.TodoDeletedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoDeletedEvt,
			ID:        cmd.ID,
			Timestamp: time.Now(),
		},
	}
	dat, _ := json.Marshal(evt)
	p.logger.Debug().Msgf("Todo deleted: %s", evt.ID)
	if _, err := p.js.Publish("todo.events", dat); err != nil {
		p.logger.Error().Err(err).Msg("publish delete event failed")
	}
}

func derefString(s *string) string {
	if s != nil {
		return *s
	}
	return ""
}

func derefBool(b *bool) bool {
	if b != nil {
		return *b
	}
	return false
}
