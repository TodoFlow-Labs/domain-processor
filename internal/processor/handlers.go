package processor

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nats-io/nats.go"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type CommandHandler interface {
	HandleCreate(dto.CreateTodoCommand) error
	HandleUpdate(dto.UpdateTodoCommand) error
	HandleDelete(dto.DeleteTodoCommand) error
}

type DBExecutor interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
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

func (p *Processor) HandleCreate(cmd dto.CreateTodoCommand) error {
	r := p.db.QueryRow(context.Background(),
		`INSERT INTO todo (user_id, title, description, due_date, priority, tags)
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
		p.logger.Error().Str("user_id", cmd.UserID).Msg("Todo creation failed")
		evt.Type = dto.TodoCreateFailedEvt
		if err := p.publishEvent("todo.events", evt); err != nil {
			p.logger.Error().Err(err).Msg("failed to publish event")
			return err
		}

		return err
	}

	p.logger.Debug().Msgf("Todo created: %s", evt.ID)
	if err := p.publishEvent("todo.events", evt); err != nil {
		p.logger.Error().Err(err).Msg("failed to publish event")
		return err
	}
	return nil
}

func (p *Processor) HandleUpdate(cmd dto.UpdateTodoCommand) error {
	tag, err := p.db.Exec(context.Background(), `
		UPDATE todo
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

	if err != nil || tag.RowsAffected() == 0 {
		if err != nil {
			p.logger.Error().Err(err).Msg("db update failed")
		} else {
			p.logger.Warn().Str("id", cmd.ID).Msg("no todo updated (not found or no changes)")
		}
		evt.Type = dto.TodoUpdateFailedEvt
		if err := p.publishEvent("todo.events", evt); err != nil {
			p.logger.Error().Err(err).Msg("failed to publish event")
			return err
		}
		if err != nil {
			return err
		}
		return nil // still soft-success if 0 rows affected
	}

	p.logger.Debug().Msgf("Todo updated: %s", evt.ID)
	if err := p.publishEvent("todo.events", evt); err != nil {
		p.logger.Error().Err(err).Msg("failed to publish event")
		return err
	}
	return nil
}

func (p *Processor) HandleDelete(cmd dto.DeleteTodoCommand) error {
	tag, err := p.db.Exec(context.Background(),
		`DELETE FROM todo WHERE id = $1 AND user_id = $2`, cmd.ID, cmd.UserID,
	)

	evt := dto.TodoDeletedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoDeletedEvt,
			ID:        cmd.ID,
			UserID:    cmd.UserID,
			Timestamp: time.Now(),
		},
	}
	p.logger.Debug().Msgf("Todo deleted ERROR: %s", err)
	if err != nil || tag.RowsAffected() == 0 {
		p.logger.Error().Err(err).Msg("db delete failed")
		evt.Type = dto.TodoDeleteFailedEvt
		if err := p.publishEvent("todo.events", evt); err != nil {
			p.logger.Error().Err(err).Msg("failed to publish event")
			return err
		}
		return err
	}

	p.logger.Debug().Msgf("Todo deleted: %s", evt.ID)
	if err := p.publishEvent("todo.events", evt); err != nil {
		p.logger.Error().Err(err).Msg("failed to publish event")
		return err
	}
	return nil
}

func (p *Processor) publishEvent(subject string, evt any) error {
	data, err := json.Marshal(evt)
	if err != nil {
		p.logger.Error().Err(err).Msg("failed to serialize event")
		return err
	}
	if _, err := p.js.Publish(subject, data); err != nil {
		p.logger.Error().Err(err).Str("subject", subject).Msg("failed to publish event")
		return err
	}
	return nil
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
