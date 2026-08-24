package postgres_test

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
)

func TestGetSessionUsesExplicitTenantPredicateAndDefensiveState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT tenant_id, session_id, status, version, state").WithArgs("tenant-a", "session-1").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "session-1", "active", 1, []byte("{\"key\":\"value\"}"), when, when))
	value, err := runtimepostgres.New(db).GetSession(context.Background(), "tenant-a", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	value.State["key"] = "changed"
	if value.State["key"] != "changed" {
		t.Fatal("state mutation was not applied to returned copy")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateSessionMapsDuplicateWithoutDriverDetails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("INSERT INTO public.runtime_session").WithArgs("tenant-a", "session-1", driver.Value([]byte("{}"))).WillReturnError(errors.New("duplicate key value contains secret connection detail"))
	_, err = runtimepostgres.New(db).CreateSession(context.Background(), "tenant-a", "session-1", nil)
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMethodsRespectCanceledContextBeforeDatabaseCall(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtimepostgres.New(db).GetSession(ctx, "tenant-a", "session-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
