package migrations

import (
	"context"
	"errors"
	"os"
	"testing"

	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

func TestOrderedFilesAreContiguousAndDigestable(t *testing.T) {
	files, err := orderedFiles()
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].version != 1 || files[1].version != 2 {
		t.Fatalf("migration order = %+v", files)
	}
	for _, migration := range files {
		if migration.name == "" || len(migration.digest) != 64 || migration.sql == "" {
			t.Fatalf("invalid migration metadata = %+v", migration)
		}
	}
}

func TestApplyAndVerifyAgainstDisposablePostgres(t *testing.T) {
	dsn := os.Getenv("POSTGRES_MIGRATION_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_MIGRATION_TEST_DSN is not set")
	}
	db, err := storagepostgres.Open(context.Background(), dsn, storagepostgres.Options{MaxOpenConns: 2, MaxIdleConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if err := Apply(context.Background(), db); err != nil {
		t.Fatalf("Apply error = %v", err)
	}
	if err := Verify(context.Background(), db); err != nil {
		t.Fatalf("Verify error = %v", err)
	}
}

func TestApplyRejectsNilInputsWithoutDatabaseDetails(t *testing.T) {
	if !errors.Is(Apply(context.Background(), nil), ErrMigration) {
		t.Fatal("nil database was not rejected")
	}
	if !errors.Is(Verify(context.Background(), nil), ErrMigration) {
		t.Fatal("nil database was not rejected by Verify")
	}
}

func TestValidateHistoryRejectsFutureVersions(t *testing.T) {
	files, err := orderedFiles()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateHistory(map[int]string{1: files[0].digest, 2: files[1].digest, 3: "future"}, files); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("future migration history error = %v", err)
	}
}
