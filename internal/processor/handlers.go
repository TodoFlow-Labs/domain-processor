package processor

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type CommandHandler interface {
	HandleCreate(dto.CreateTodoCommand) error
	HandleUpdate(dto.UpdateTodoCommand) error
	HandleDelete(dto.DeleteTodoCommand) error
}

type Processor struct {
	js     nats.JetStreamContext
	logger logging.Logger
}

func NewProcessor(js nats.JetStreamContext, logger logging.Logger) *Processor {
	return &Processor{js: js, logger: logger}
}

func (p *Processor) HandleCreate(cmd dto.CreateTodoCommand) error {
	evt := dto.TodoCreatedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoCreatedEvt,
			UserID:    cmd.UserID,
			Timestamp: time.Now(),
			ID:        uuid.NewString(),
		},
		Title:       cmd.Title,
		Description: cmd.Description,
		DueDate:     cmd.DueDate,
		Priority:    cmd.Priority,
		Tags:        cmd.Tags,
	}

	p.logger.Debug().Msgf("Emitting TodoCreatedEvent for user %s", cmd.UserID)
	return p.publishEvent("todo.events", evt)
}

func (p *Processor) HandleUpdate(cmd dto.UpdateTodoCommand) error {
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

	p.logger.Debug().Msgf("Emitting TodoUpdatedEvent for todo %s", cmd.ID)
	return p.publishEvent("todo.events", evt)
}

func (p *Processor) HandleDelete(cmd dto.DeleteTodoCommand) error {
	evt := dto.TodoDeletedEvent{
		BaseEvent: dto.BaseEvent{
			Type:      dto.TodoDeletedEvt,
			ID:        cmd.ID,
			UserID:    cmd.UserID,
			Timestamp: time.Now(),
		},
	}

	p.logger.Debug().Msgf("Emitting TodoDeletedEvent for todo %s", cmd.ID)
	return p.publishEvent("todo.events", evt)
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
