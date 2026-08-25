package postgres

import (
	"context"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"testing"
	"time"
)

func TestHandoffStoreScopeAndReserve(t *testing.T) {
	if _, err := NewHandoffStore(nil, ""); !errors.Is(err, audit.ErrInvalid) {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	mock.ExpectBegin()
	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{"tenant_id", "handoff_id", "request_id", "trace_id", "event_id", "state", "result", "error_type", "latency_ms", "created_at", "updated_at"})
	rows.AddRow("t", "h", "r", "tr", "e", "pending", nil, nil, nil, now, now)
	mock.ExpectQuery("SELECT tenant_id,handoff_id,request_id").WillReturnRows(rows)
	mock.ExpectCommit()
	store, _ := NewHandoffStore(db, "t")
	got, err := store.Reserve(context.Background(), audit.ExecutionHandoff{TenantID: "t", HandoffID: "h", RequestID: "r", State: audit.HandoffPending})
	if err != nil || got.State != audit.HandoffPending {
		t.Fatalf("reserve=%+v err=%v", got, err)
	}
	if _, err := store.Reserve(context.Background(), audit.ExecutionHandoff{TenantID: "other", HandoffID: "h", RequestID: "r", State: audit.HandoffPending}); !errors.Is(err, audit.ErrTenantScope) {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
