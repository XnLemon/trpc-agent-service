package migrations

import (
	"context"
	"errors"
	"testing"
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
