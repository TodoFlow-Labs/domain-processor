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
	logger logging.Logger
}

func NewProcessor(js nats.JetStreamContext, db DBExecutor, logger logging.Logger) *Processor {
	return &Processor{js: js, db: db, logger: logger}
}

func (p *Processor) HandleCreate(cmd dto.CreateTodoCommand) {
	r := p.db.QueryRow(context.Background(),
		`INSERT INTO todos.todo (user_id, title, description, due_date, priority, tags)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING id, created_at`,
		cmd.UserID, cmd.Title, cmd.Description, cmd.DueDate, cmd.Priority, cmd.Tags,
	)

	var evt dto.TodoCreatedEvent
	evt.Type = dto.TodoCreatedEvt
	evt.UserID = cmd.UserID
	evt.Title = cmd.Title
	evt.Description = cmd.Description
	evt.DueDate = cmd.DueDate
	evt.Priority = cmd.Priority
	evt.Tags = cmd.Tags

	if err := r.Scan(&evt.ID, &evt.Timestamp); err != nil {
		p.logger.Error().Err(err).Msg("db insert failed")
		return
	}

	p.logger.Debug().Msgf("Todo created: %s", evt.ID)
	p.publishEvent("todo.events", evt)
}

func (p *Processor) HandleUpdate(cmd dto.UpdateTodoCommand) {
	_, err := p.db.Exec(context.Background(), `
		UPDATE todos.todo
		SET
			title = COALESCE($1, title),
			description = COALESCE($2, description),
			completed = COALESCE($3, completed),
			due_date = COALESCE($4, due_date),
			priority = COALESCE($5, priority),
			tags = COALESCE($6, tags),
			updated_at = now()
		WHERE id = $7 AND user_id = $8
	`, cmd.Title, cmd.Description, cmd.Completed, cmd.DueDate, cmd.Priority, cmd.Tags, cmd.ID, cmd.UserID)
	if err != nil {
		p.logger.Error().Err(err).Msg("db update failed")
		return
	}

	evt := dto.TodoUpdatedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoUpdatedEvt,
			ID:        cmd.ID,
			UserID:    cmd.UserID,
			Timestamp: time.Now(),
		},
		Title:       derefString(cmd.Title),
		Description: derefString(cmd.Description),
		Completed:   derefBool(cmd.Completed),
		DueDate:     cmd.DueDate,
		Priority:    cmd.Priority,
		Tags:        derefStringSlice(cmd.Tags),
	}

	p.logger.Debug().Msgf("Todo updated: %s", evt.ID)
	p.publishEvent("todo.events", evt)
}

func (p *Processor) HandleDelete(cmd dto.DeleteTodoCommand) {
	_, err := p.db.Exec(context.Background(),
		`DELETE FROM todos.todo WHERE id = $1 AND user_id = $2`, cmd.ID, cmd.UserID,
	)
	if err != nil {
		p.logger.Error().Err(err).Msg("db delete failed")
		return
	}

	evt := dto.TodoDeletedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoDeletedEvt,
			ID:        cmd.ID,
			UserID:    cmd.UserID,
			Timestamp: time.Now(),
		},
	}

	p.logger.Debug().Msgf("Todo deleted: %s", evt.ID)
	p.publishEvent("todo.events", evt)
}

func (p *Processor) publishEvent(subject string, evt any) {
	data, err := json.Marshal(evt)
	if err != nil {
		p.logger.Error().Err(err).Msg("failed to serialize event")
		return
	}
	if _, err := p.js.Publish(subject, data); err != nil {
		p.logger.Error().Err(err).Str("subject", subject).Msg("failed to publish event")
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

func derefStringSlice(s *[]string) []string {
	if s != nil {
		return *s
	}
	return nil
}
