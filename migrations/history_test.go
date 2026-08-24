package migrations

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestExecMigrationRemovesEmbeddedTransactionWrappers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	mock.ExpectBegin()
	mock.ExpectExec("CREATE TABLE test_migration").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectCommit()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := execMigration(context.Background(), tx, "BEGIN;\nCREATE TABLE test_migration;\nCOMMIT;"); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationHelpersAndSQLMockApplyVerify(t *testing.T) {
	files, err := orderedFiles()
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectExec(`SELECT pg_advisory_lock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS public.schema_migrations`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnRows(sqlmock.NewRows([]string{"version", "sha256"}))
	for _, migration := range files {
		mock.ExpectBegin()
		mock.ExpectExec(`(?s).*`).WillReturnResult(sqlmock.NewResult(0, 0))
		mock.ExpectExec(`INSERT INTO public.schema_migrations`).WithArgs(migration.version, migration.name, migration.digest, sqlmock.AnyArg()).WillReturnResult(sqlmock.NewResult(0, 1))
		mock.ExpectCommit()
	}
	mock.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
	if err := Apply(context.Background(), db); err != nil {
		t.Fatalf("Apply error = %v", err)
	}
	if got := nextVersion(map[int]string{1: files[0].digest, 2: files[1].digest}); got != 3 {
		t.Fatalf("nextVersion = %d", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	verifyDB, verifyMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = verifyDB.Close() }()
	verifyMock.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnRows(sqlmock.NewRows([]string{"version", "sha256"}).AddRow(files[0].version, files[0].digest).AddRow(files[1].version, files[1].digest))
	if err := Verify(context.Background(), verifyDB); err != nil {
		t.Fatalf("Verify error = %v", err)
	}
	if err := verifyMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMigrationApplyAndVerifyFailures(t *testing.T) {
	tests := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{"lock", func(m sqlmock.Sqlmock) {
			m.ExpectExec(`SELECT pg_advisory_lock`).WithArgs(lockKey).WillReturnError(errors.New("lock"))
		}, ErrMigration},
		{"history table", func(m sqlmock.Sqlmock) {
			m.ExpectExec(`SELECT pg_advisory_lock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec(`CREATE TABLE IF NOT EXISTS public.schema_migrations`).WillReturnError(errors.New("table"))
			m.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
		}, ErrMigration},
		{"history read", func(m sqlmock.Sqlmock) {
			m.ExpectExec(`SELECT pg_advisory_lock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec(`CREATE TABLE IF NOT EXISTS public.schema_migrations`).WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnError(errors.New("read"))
			m.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
		}, ErrInvalidHistory},
		{"migration execution", func(m sqlmock.Sqlmock) {
			m.ExpectExec(`SELECT pg_advisory_lock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec(`CREATE TABLE IF NOT EXISTS public.schema_migrations`).WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnRows(sqlmock.NewRows([]string{"version", "sha256"}))
			m.ExpectBegin()
			m.ExpectExec(`(?s).*`).WillReturnError(errors.New("migration SQL"))
			m.ExpectRollback()
			m.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
		}, ErrMigration},
		{"migration record", func(m sqlmock.Sqlmock) {
			files, err := orderedFiles()
			if err != nil {
				panic(err)
			}
			m.ExpectExec(`SELECT pg_advisory_lock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec(`CREATE TABLE IF NOT EXISTS public.schema_migrations`).WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnRows(sqlmock.NewRows([]string{"version", "sha256"}))
			m.ExpectBegin()
			m.ExpectExec(`(?s).*`).WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectExec(`INSERT INTO public.schema_migrations`).WithArgs(files[0].version, files[0].name, files[0].digest, sqlmock.AnyArg()).WillReturnError(errors.New("record"))
			m.ExpectRollback()
			m.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
		}, ErrMigration},
		{"digest mismatch", func(m sqlmock.Sqlmock) {
			files, err := orderedFiles()
			if err != nil {
				panic(err)
			}
			m.ExpectExec(`SELECT pg_advisory_lock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec(`CREATE TABLE IF NOT EXISTS public.schema_migrations`).WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnRows(sqlmock.NewRows([]string{"version", "sha256"}).AddRow(files[0].version, "wrong"))
			m.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
		}, ErrInvalidHistory},
		{"future history", func(m sqlmock.Sqlmock) {
			files, err := orderedFiles()
			if err != nil {
				panic(err)
			}
			m.ExpectExec(`SELECT pg_advisory_lock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec(`CREATE TABLE IF NOT EXISTS public.schema_migrations`).WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnRows(sqlmock.NewRows([]string{"version", "sha256"}).AddRow(files[0].version, files[0].digest).AddRow(9, "future"))
			m.ExpectExec(`SELECT pg_advisory_unlock`).WithArgs(lockKey).WillReturnResult(sqlmock.NewResult(0, 1))
		}, ErrInvalidHistory},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			tc.setup(mock)
			if err := Apply(context.Background(), db); !errors.Is(err, tc.want) {
				t.Fatalf("Apply error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnError(errors.New("read"))
	if err := Verify(context.Background(), db); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("Verify error = %v", err)
	}
}

func TestMigrationContextAndVerificationMismatchErrors(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	if err := Apply(canceled, db); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Apply error = %v", err)
	}
	_ = db.Close()
	files, err := orderedFiles()
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range []*sqlmock.Rows{
		sqlmock.NewRows([]string{"version", "sha256"}).AddRow(files[0].version, files[0].digest),
		sqlmock.NewRows([]string{"version", "sha256"}).AddRow(files[0].version, files[0].digest).AddRow(files[1].version, "wrong"),
	} {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		mock.ExpectQuery(`SELECT version, sha256 FROM public.schema_migrations`).WillReturnRows(rows)
		if err := Verify(context.Background(), db); !errors.Is(err, ErrInvalidHistory) {
			t.Fatalf("mismatch Verify error = %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatal(err)
		}
		_ = db.Close()
	}
}

func TestValidateHistoryRejectsEmptyAndNextVersionHandlesGaps(t *testing.T) {
	if err := validateHistory(nil, nil); !errors.Is(err, ErrInvalidHistory) {
		t.Fatalf("empty history error = %v", err)
	}
	if got := nextVersion(map[int]string{4: "future", 2: "older"}); got != 5 {
		t.Fatalf("nextVersion with gap = %d", got)
	}
}

func TestZZApplyAndVerifyAgainstMigratedPostgres(t *testing.T) {
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
