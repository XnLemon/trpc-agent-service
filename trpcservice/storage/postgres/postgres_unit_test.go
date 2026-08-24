package postgres

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgreSQLStoragePrimitives(t *testing.T) {
	if got := normalizeDSN(" PostgreSQL+PSYCOPG://db.example/test "); got != "postgresql://db.example/test" {
		t.Fatalf("normalized psycopg DSN = %q", got)
	}
	if _, err := Open(context.Background(), "", Options{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("empty DSN error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Open(canceled, "postgres://unused", Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Open error = %v", err)
	}
	if err := Ping(context.Background(), nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("nil Ping error = %v", err)
	}

	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectPing().WillReturnError(errors.New("ping failure"))
	if err := Ping(context.Background(), db); !errors.Is(err, ErrStorage) {
		t.Fatalf("failed ping error = %v", err)
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
	tx, err := Begin(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectCommit()
	if err := Commit(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	notFound := errors.New("not found")
	duplicate := errors.New("duplicate")
	conflict := errors.New("conflict")
	invalid := errors.New("invalid")
	if got := MapError(context.Background(), sql.ErrNoRows, notFound, duplicate, conflict, invalid); !errors.Is(got, notFound) {
		t.Fatalf("not-found mapping = %v", got)
	}
	if got := MapError(context.Background(), &pgconn.PgError{Code: "23505"}, notFound, duplicate, conflict, invalid); !errors.Is(got, duplicate) {
		t.Fatalf("duplicate mapping = %v", got)
	}
	if got := MapError(context.Background(), errors.New("driver failure"), notFound, duplicate, conflict, invalid); !errors.Is(got, ErrStorage) {
		t.Fatalf("fallback mapping = %v", got)
	}
	if NullableInt(sql.NullInt64{}) != nil || NullableString(sql.NullString{}) != nil || NullableText("") != nil {
		t.Fatal("invalid nullable values became non-nil")
	}
	if value := NullableInt(sql.NullInt64{Int64: 4, Valid: true}); value == nil || *value != 4 {
		t.Fatalf("nullable integer = %v", value)
	}
	future := time.Now().UTC().Add(time.Hour)
	if got := MonotonicNow(future); !got.Equal(future) {
		t.Fatalf("monotonic timestamp regressed: %s", got)
	}
}

func TestPostgreSQLStorageJSONBoundary(t *testing.T) {
	if _, err := EncodeJSON(math.NaN()); err == nil {
		t.Fatal("non-finite number encoded as JSON")
	}
	var decoded map[string]string
	if err := DecodeJSON([]byte(`{"mode":"safe"}`), &decoded); err != nil || decoded["mode"] != "safe" {
		t.Fatalf("JSON decode = %#v, err=%v", decoded, err)
	}
	for _, malformed := range [][]byte{[]byte(`{"unknown":true}`), []byte(`{} {}`), []byte(`not-json`)} {
		if err := DecodeJSON(malformed, &decoded); !errors.Is(err, ErrStorage) {
			t.Fatalf("DecodeJSON(%q) error = %v", malformed, err)
		}
	}
}
