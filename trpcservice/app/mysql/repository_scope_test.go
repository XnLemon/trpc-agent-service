package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
)

func TestAppRepositoryRejectsNonCanonicalScopeBeforeSQL(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository := NewAppRepository(db)
	validAppID := "app_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if _, err := repository.Get(context.Background(), "tenant", validAppID); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("invalid tenant Get() = %v", err)
	}
	if _, _, err := repository.ListRevisions(context.Background(), "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", "app", "", "", "", 1); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("invalid app ListRevisions() = %v", err)
	}
	if _, err := repository.GetRevision(context.Background(), "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", validAppID, 0); !errors.Is(err, appmodel.ErrInvalid) {
		t.Fatalf("invalid revision GetRevision() = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
