// internal/subscriber/subscriber.go
package subscriber

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/todoflow-labs/domain-processor/internal/processor"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

func SubscribeToCommands(js nats.JetStreamContext, handler processor.CommandHandler, logger logging.Logger) {
	_, err := js.Subscribe("todo.commands", func(m *nats.Msg) {
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
			handler.HandleCreate(cmd)
		case dto.UpdateTodoCmd:
			var cmd dto.UpdateTodoCommand
			_ = json.Unmarshal(m.Data, &cmd)
			handler.HandleUpdate(cmd)
		case dto.DeleteTodoCmd:
			var cmd dto.DeleteTodoCommand
			_ = json.Unmarshal(m.Data, &cmd)
			handler.HandleDelete(cmd)
		default:
			logger.Warn().Msgf("unknown command type: %s", base.Type)
		}
		m.Ack()
	},
		nats.Durable("domain-processor"),
		nats.Bind("todo_commands", "domain-processor"),
		nats.ManualAck(),
		nats.AckWait(30*time.Second),
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("subscribe failed")
	}
}
