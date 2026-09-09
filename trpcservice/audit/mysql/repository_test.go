package mysql

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

func TestAppendIsTenantBoundAndDigestIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, err := New(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	event := audit.Event{SchemaVersion: audit.SchemaVersion, EventID: "event-a", EventType: audit.EventToolExecuted, TenantID: "tenant-a", ToolName: "read", RequestID: "request-a", TraceID: "trace-a", Decision: audit.DecisionAccepted, OccurredAt: time.Now().UTC()}
	digest, err := event.Digest()
	if err != nil {
		t.Fatal(err)
	}
	insert := regexp.QuoteMeta("INSERT INTO audit_event (")
	mock.ExpectExec(insert).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT digest FROM audit_event WHERE tenant_id=? AND event_id=?")).WithArgs("tenant-a", "event-a").WillReturnRows(sqlmock.NewRows([]string{"digest"}).AddRow(digest))
	result, err := store.Append(context.Background(), event)
	if err != nil || result.Duplicate || result.Digest != digest {
		t.Fatalf("first append = %+v, err = %v", result, err)
	}
	mock.ExpectExec(insert).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT digest FROM audit_event WHERE tenant_id=? AND event_id=?")).WithArgs("tenant-a", "event-a").WillReturnRows(sqlmock.NewRows([]string{"digest"}).AddRow(digest))
	result, err = store.Append(context.Background(), event)
	if err != nil || !result.Duplicate {
		t.Fatalf("duplicate append = %+v, err = %v", result, err)
	}
	other := event
	other.TenantID = "tenant-b"
	if _, err := store.Append(context.Background(), other); !errors.Is(err, audit.ErrTenantScope) {
		t.Fatalf("cross-tenant append = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAppendRejectsDigestConflict(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, err := New(db, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	event := audit.Event{SchemaVersion: audit.SchemaVersion, EventID: "event-a", EventType: audit.EventToolExecuted, TenantID: "tenant-a", ToolName: "read", RequestID: "request-a", TraceID: "trace-a", Decision: audit.DecisionAccepted, OccurredAt: time.Now().UTC()}
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO audit_event (")).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT digest FROM audit_event WHERE tenant_id=? AND event_id=?")).WithArgs("tenant-a", "event-a").WillReturnRows(sqlmock.NewRows([]string{"digest"}).AddRow("different"))
	if _, err := store.Append(context.Background(), event); !errors.Is(err, audit.ErrConflict) {
		t.Fatalf("digest conflict = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
