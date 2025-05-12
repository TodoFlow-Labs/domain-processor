package processor_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/todoflow-labs/domain-processor/internal/processor"
	"github.com/todoflow-labs/shared-dtos/dto"
	"github.com/todoflow-labs/shared-dtos/logging"
)

type mockDB struct {
	mock.Mock
}

func (m *mockDB) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	callArgs := m.Called(ctx, query, args)
	return callArgs.Get(0).(pgconn.CommandTag), callArgs.Error(1)
}

func (m *mockDB) QueryRow(ctx context.Context, query string, args ...any) processor.RowScanner {
	return m.Called(ctx, query, args).Get(0).(processor.RowScanner)
}

type mockRow struct {
	mock.Mock
}

func (r *mockRow) Scan(dest ...any) error {
	args := r.Called(dest)
	if len(dest) == 2 {
		if id, ok := dest[0].(*string); ok {
			*id = "todo-id"
		}
		if ts, ok := dest[1].(*time.Time); ok {
			*ts = time.Now()
		}
	}
	return args.Error(0)
}

func setupEmbeddedNATSServer(t *testing.T) (*server.Server, nats.JetStreamContext, *nats.Conn) {
	t.Helper()
	opts := &server.Options{
		JetStream: true,
		StoreDir:  t.TempDir(),
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
	}
	srv, err := server.NewServer(opts)
	assert.NoError(t, err)

	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("NATS server not ready in time")
	}

	nc, err := nats.Connect(srv.ClientURL())
	assert.NoError(t, err)

	js, err := nc.JetStream()
	assert.NoError(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "todo_events",
		Subjects: []string{"todo.events"},
	})
	assert.NoError(t, err)

	return srv, js, nc
}

func TestHandleCreate_Success(t *testing.T) {
	db := new(mockDB)
	row := new(mockRow)
	logger := logging.New("debug")
	srv, js, nc := setupEmbeddedNATSServer(t)
	defer srv.Shutdown()
	defer nc.Close()

	db.On("QueryRow", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(row)
	row.On("Scan", mock.Anything).Return(nil)

	h := processor.NewProcessor(js, db, logger)
	cmd := dto.CreateTodoCommand{
		BaseCommand: dto.BaseCommand{UserID: "user-1"},
		Title:       "test todo",
	}

	err := h.HandleCreate(cmd)
	assert.NoError(t, err)

	sub, err := js.PullSubscribe("todo.events", "test-create")
	assert.NoError(t, err)
	msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)

	var evt dto.TodoCreatedEvent
	err = json.Unmarshal(msgs[0].Data, &evt)
	assert.NoError(t, err)
	assert.Equal(t, cmd.Title, evt.Title)
	assert.Equal(t, cmd.UserID, evt.UserID)

	db.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestHandleUpdate_Success(t *testing.T) {
	db := new(mockDB)
	logger := logging.New("debug")
	srv, js, nc := setupEmbeddedNATSServer(t)
	defer srv.Shutdown()
	defer nc.Close()

	db.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything).Return(pgconn.NewCommandTag("UPDATE 1"), nil)

	h := processor.NewProcessor(js, db, logger)
	title := "Updated Title"
	completed := true
	cmd := dto.UpdateTodoCommand{
		BaseCommand: dto.BaseCommand{
			ID:     "todo-id",
			UserID: "user-1",
		},
		Title:     &title,
		Completed: &completed,
	}

	err := h.HandleUpdate(cmd)
	assert.NoError(t, err)

	sub, err := js.PullSubscribe("todo.events", "test-update")
	assert.NoError(t, err)
	msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)

	var evt dto.TodoUpdatedEvent
	err = json.Unmarshal(msgs[0].Data, &evt)
	assert.NoError(t, err)
	assert.Equal(t, cmd.ID, evt.ID)
	assert.Equal(t, cmd.UserID, evt.UserID)

	db.AssertExpectations(t)
}

func TestHandleDelete_Success(t *testing.T) {
	db := new(mockDB)
	logger := logging.New("debug")
	srv, js, nc := setupEmbeddedNATSServer(t)
	defer srv.Shutdown()
	defer nc.Close()

	db.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(pgconn.NewCommandTag("DELETE 1"), nil)

	h := processor.NewProcessor(js, db, logger)
	cmd := dto.DeleteTodoCommand{
		BaseCommand: dto.BaseCommand{
			ID:     "todo-id",
			UserID: "user-1",
		},
	}

	err := h.HandleDelete(cmd)
	assert.NoError(t, err)

	sub, err := js.PullSubscribe("todo.events", "test-delete")
	assert.NoError(t, err)
	msgs, err := sub.Fetch(1, nats.MaxWait(time.Second))
	assert.NoError(t, err)
	assert.Len(t, msgs, 1)

	var evt dto.TodoDeletedEvent
	err = json.Unmarshal(msgs[0].Data, &evt)
	assert.NoError(t, err)
	assert.Equal(t, cmd.ID, evt.ID)
	assert.Equal(t, cmd.UserID, evt.UserID)

	db.AssertExpectations(t)
}
