// internal/processor/handler_test.go
package processor_test

import (
	"context"
	"testing"
	"time"

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

func (m *mockDB) Exec(ctx context.Context, query string, args ...any) (any, error) {
	argsList := m.Called(ctx, query, args)
	return argsList.Get(0), argsList.Error(1)
}

func (m *mockDB) QueryRow(ctx context.Context, query string, args ...any) processor.RowScanner {
	argsList := m.Called(ctx, query, args)
	return argsList.Get(0).(processor.RowScanner)
}

type mockRow struct {
	mock.Mock
}

func (r *mockRow) Scan(dest ...any) error {
	args := r.Called(dest)
	if len(dest) == 2 {
		idPtr := dest[0].(*string)
		tsPtr := dest[1].(*time.Time)
		*idPtr = "todo-id"
		*tsPtr = time.Now()
	}
	return args.Error(0)
}

func setupEmbeddedNATSServer(t *testing.T) (*server.Server, nats.JetStreamContext, *nats.Conn) {
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

func TestHandleUpdate_Success(t *testing.T) {
	db := new(mockDB)
	logger := logging.New("debug")
	srv, js, nc := setupEmbeddedNATSServer(t)
	defer srv.Shutdown()
	defer nc.Close()

	db.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(nil, nil)

	h := processor.NewProcessor(js, db, logger)
	title := "Updated Title"
	completed := true
	cmd := dto.UpdateTodoCommand{
		BaseCommand: dto.BaseCommand{
			ID: "todo-id",
		},
		Title:     &title,
		Completed: &completed,
	}

	h.HandleUpdate(cmd)

	db.AssertExpectations(t)
}

func TestHandleCreate_Success(t *testing.T) {
	db := new(mockDB)
	row := new(mockRow)
	logger := logging.New("debug")
	srv, js, nc := setupEmbeddedNATSServer(t)
	defer srv.Shutdown()
	defer nc.Close()

	db.On("QueryRow", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(row)
	row.On("Scan", mock.Anything).
		Return(nil)

	h := processor.NewProcessor(js, db, logger)
	cmd := dto.CreateTodoCommand{Title: "test todo"}

	h.HandleCreate(cmd)

	db.AssertExpectations(t)
	row.AssertExpectations(t)
}

func TestHandleDelete_Success(t *testing.T) {
	db := new(mockDB)
	logger := logging.New("debug")
	srv, js, nc := setupEmbeddedNATSServer(t)
	defer srv.Shutdown()
	defer nc.Close()

	db.On("Exec", mock.Anything, mock.AnythingOfType("string"), mock.Anything).
		Return(nil, nil)

	h := processor.NewProcessor(js, db, logger)
	cmd := dto.DeleteTodoCommand{
		BaseCommand: dto.BaseCommand{
			ID: "todo-id",
		},
	}

	h.HandleDelete(cmd)

	db.AssertExpectations(t)
}
