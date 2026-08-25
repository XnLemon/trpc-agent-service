package postgres

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

func testEvent(tenant string) audit.Event {
	input, output := int64(12), int64(8)
	return audit.Event{SchemaVersion: 1, EventID: "evt_01ARZ3NDEKTSV4RRFFQ69G5FAV", EventType: audit.EventExecutionCompleted, TenantID: tenant, Channel: "api", AgentAppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV", OccurredAt: time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC), Cost: &audit.Usage{InputTokens: &input, OutputTokens: &output, Currency: "USD", Provider: "provider", Model: "model", ExecutionResult: audit.ResultSuccess}}
}

func TestNewBindsTenantAndRejectsInvalidScope(t *testing.T) {
	if _, err := New(nil, ""); !errors.Is(err, audit.ErrInvalid) {
		t.Fatalf("empty scope error = %v", err)
	}
	store, err := New(nil, "tenant-a")
	if err != nil || store.tenantID != "tenant-a" {
		t.Fatalf("store = %+v, err=%v", store, err)
	}
}

func TestAppendBindsDatabaseScopeAndMapsDuplicateConflict(t *testing.T) {
	event := testEvent("tenant-a")
	digest, err := event.Digest()
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SELECT set_config").WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT \\* FROM public\\.audit_append_event").WillReturnRows(sqlmock.NewRows([]string{"stored_digest", "duplicate", "conflict"}).AddRow(digest, false, false))
	mock.ExpectCommit()
	store, err := NewRepository(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	result, err := store.Append(context.Background(), event)
	if err != nil || result.Duplicate || result.Digest != digest {
		t.Fatalf("append = %+v, err=%v", result, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SELECT set_config").WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT \\* FROM public\\.audit_append_event").WillReturnRows(sqlmock.NewRows([]string{"stored_digest", "duplicate", "conflict"}).AddRow("other", false, true))
	mock.ExpectRollback()
	store, err = NewRepository(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), event); !errors.Is(err, audit.ErrConflict) {
		t.Fatalf("conflict append error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendRejectsCrossTenantBeforeDatabase(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store, err := New(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Append(context.Background(), testEvent("tenant-b")); !errors.Is(err, audit.ErrTenantScope) {
		t.Fatalf("cross-tenant append error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetAndListDecodeValidatedEvents(t *testing.T) {
	event := testEvent("tenant-a")
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SELECT set_config").WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id,event_id,schema_version,event_type").WithArgs("tenant-a", event.EventID).WillReturnRows(testEventRow(event))
	mock.ExpectCommit()
	store, err := New(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	got, err := store.Get(context.Background(), event.EventID)
	if err != nil || got.EventID != event.EventID || got.Cost == nil || got.Cost.InputTokens == nil || *got.Cost.InputTokens != 12 {
		t.Fatalf("get = %+v, err=%v", got, err)
	}
	mock.ExpectBegin()
	mock.ExpectExec("SELECT set_config").WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id,event_id,schema_version,event_type.*ORDER BY occurred_at, event_id").WithArgs("tenant-a").WillReturnRows(testEventRow(event))
	mock.ExpectCommit()
	list, err := store.List(context.Background(), audit.Query{})
	if err != nil || len(list) != 1 || list[0].TenantID != "tenant-a" {
		t.Fatalf("list = %+v, err=%v", list, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAggregateUsageSeparatesCurrencyAndGroup(t *testing.T) {
	event := testEvent("tenant-a")
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec("SELECT set_config").WithArgs("tenant-a").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id,event_id,schema_version,event_type.*ORDER BY occurred_at, event_id").WithArgs("tenant-a").WillReturnRows(testEventRow(event))
	mock.ExpectCommit()
	store, err := New(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	totals, err := store.AggregateUsage(context.Background(), audit.UsageQuery{GroupBy: []audit.GroupBy{audit.GroupProvider}})
	if err != nil || len(totals) != 1 || totals[0].InputTokens != 12 || totals[0].Currency != "USD" {
		t.Fatalf("totals = %+v, err=%v", totals, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func testEventRow(event audit.Event) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"tenant_id", "event_id", "schema_version", "event_type", "channel", "user_id", "session_id", "agent_app_id", "revision", "model_profile_id", "tool_name", "decision", "latency_ms", "error_type", "input_tokens", "output_tokens", "model_cost_minor", "tool_cost_minor", "currency", "budget_used_tokens", "budget_used_minor", "execution_result", "provider", "model", "request_id", "trace_id", "correlation_id", "actor_type", "actor_id", "reason", "previous_version", "next_version", "occurred_at", "digest",
	}).AddRow(event.TenantID, event.EventID, event.SchemaVersion, string(event.EventType), event.Channel, nil, nil, event.AgentAppID, nil, nil, nil, nil, nil, nil, int64(12), int64(8), nil, nil, event.Cost.Currency, nil, nil, string(event.Cost.ExecutionResult), event.Cost.Provider, event.Cost.Model, nil, nil, nil, nil, nil, nil, nil, nil, event.OccurredAt, "0000000000000000000000000000000000000000000000000000000000000000")
}
