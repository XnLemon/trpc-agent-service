package postgres

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestPostgreSQLDBLifecycleErrorBoundaries(t *testing.T) {
	t.Run("ping", func(t *testing.T) {
		if err := Ping(context.Background(), openPostgresErrorDB(t)); err != nil {
			t.Fatalf("successful ping error = %v", err)
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
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := Ping(canceled, db); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled ping error = %v", err)
		}
	})

	t.Run("transaction", func(t *testing.T) {
		db, mock := newSQLMock(t)
		mock.ExpectBegin()
		tx, err := begin(context.Background(), db)
		if err != nil {
			t.Fatalf("begin error = %v", err)
		}
		mock.ExpectCommit()
		if err := commit(context.Background(), tx); err != nil {
			t.Fatalf("commit error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("begin and commit failures", func(t *testing.T) {
		db, mock := newSQLMock(t)
		mock.ExpectBegin().WillReturnError(errors.New("begin failure"))
		if _, err := begin(context.Background(), db); !errors.Is(err, ErrStorage) {
			t.Fatalf("begin failure error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}

		db, mock = newSQLMock(t)
		mock.ExpectBegin()
		tx, err := begin(context.Background(), db)
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectCommit().WillReturnError(errors.New("commit failure"))
		if err := commit(context.Background(), tx); !errors.Is(err, ErrStorage) {
			t.Fatalf("commit failure error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}

		db, mock = newSQLMock(t)
		mock.ExpectBegin()
		tx, err = begin(context.Background(), db)
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectRollback()
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		if err := commit(canceled, tx); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled commit error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
	})

	if got := normalizeDSN(" postgres+psycopg://db.example/test "); got != "postgresql://db.example/test" {
		t.Fatalf("alternate normalized DSN = %q", got)
	}
	if got := normalizeDSN("  "); got != "" {
		t.Fatalf("blank normalized DSN = %q", got)
	}
	if got := mapDBError(context.TODO(), nil, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid); got != nil {
		t.Fatalf("nil database error = %v", got)
	}
	if got := mapDBError(context.Background(), &pgconn.PgError{Code: "99999"}, tenant.ErrNotFound, tenant.ErrDuplicateKey, tenant.ErrConflict, tenant.ErrInvalid); !errors.Is(got, ErrStorage) {
		t.Fatalf("unknown PostgreSQL error = %v", got)
	}
}

func TestPostgreSQLCodecLayeredDecodeErrors(t *testing.T) {
	if _, err := encodeJSON(math.NaN()); err == nil {
		t.Fatal("non-finite number encoded as JSON")
	}
	if err := decodeModelJSON([]byte("not-json"), []byte("{}"), &model.Configuration{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("model options error = %v", err)
	}
	if err := decodeModelJSON([]byte("{}"), []byte("not-json"), &model.Configuration{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("model generation error = %v", err)
	}
	if err := decodeAgentRevisionParts([]byte("not-json"), []byte("{}"), &agent.Revision{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent generation error = %v", err)
	}
	if err := decodeAgentRevisionParts([]byte("{}"), []byte("not-json"), &agent.Revision{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("agent runtime error = %v", err)
	}
	if err := decodeProtocol([]byte("not-json"), &channels.ProtocolConfiguration{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("protocol error = %v", err)
	}
}
