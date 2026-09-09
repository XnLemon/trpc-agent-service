package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

func TestBackendRepositoryRejectsNonCanonicalScopeBeforeSQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository := NewRepository(db, nil)
	validProfileID := "bp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if _, err := repository.Get(context.Background(), "tenant", validProfileID); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("invalid tenant Get() = %v", err)
	}
	if _, _, err := repository.List(context.Background(), "tenant", "", "", "", 1); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("invalid tenant List() = %v", err)
	}
	if _, err := repository.Get(context.Background(), "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", "profile"); !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("invalid profile Get() = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
