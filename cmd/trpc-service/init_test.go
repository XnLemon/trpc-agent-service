package main

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/bootstrap"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

func TestParseInitOptionsUsesExplicitMetadata(t *testing.T) {
	t.Setenv(envInitTenantKey, "from-environment")
	t.Setenv(envInitTenantName, "Environment Tenant")
	t.Setenv(envInitAppKey, "environment-app")
	t.Setenv(envInitAppName, "Environment App")
	t.Setenv(envInitAppDescription, "description")

	options, help, err := parseInitOptions([]string{"--confirm", "--tenant-name", "Command Tenant"}, io.Discard)
	if err != nil || help || !options.confirm {
		t.Fatalf("init options = %+v help=%v err=%v", options, help, err)
	}
	if options.config.TenantKey != "from-environment" || options.config.TenantDisplayName != "Command Tenant" || options.config.AppKey != "environment-app" || options.config.AppDisplayName != "Environment App" || options.config.AppDescription != "description" {
		t.Fatalf("init metadata = %+v", options.config)
	}
	if _, help, err := parseInitOptions([]string{"--help"}, io.Discard); err != nil || !help {
		t.Fatalf("init help = help:%v err:%v", help, err)
	}
}

func TestRunInitRequiresConfirmationAndValidConfiguration(t *testing.T) {
	previousOpen := openInitDatabase
	openInitDatabase = func(context.Context, string, postgres.Options) (*sql.DB, error) {
		t.Fatal("database was opened before init confirmation/configuration")
		return nil, nil
	}
	defer func() { openInitDatabase = previousOpen }()

	t.Setenv(bootstrapPostgresDSN, "postgres://init-user@example.test/db")
	if err := runMain(context.Background(), []string{"init"}, io.Discard, io.Discard, nil); err == nil || !errors.Is(err, bootstrap.ErrInitializationAuthorization) || !strings.Contains(err.Error(), "--confirm") {
		t.Fatalf("missing confirmation error = %v", err)
	}

	for _, name := range []string{envInitTenantKey, envInitTenantName, envInitAppKey, envInitAppName} {
		t.Setenv(name, "")
	}
	err := runMain(context.Background(), []string{"init", "--confirm"}, io.Discard, io.Discard, nil)
	if !errors.Is(err, bootstrap.ErrInvalidConfig) {
		t.Fatalf("invalid initialization configuration = %v", err)
	}
}

func TestRunInitAppliesAndVerifiesMigrationsAndPrintsOnlyIDs(t *testing.T) {
	t.Setenv(bootstrapPostgresDSN, "postgres://init-user@example.test/db")
	t.Setenv(envInitTenantKey, "acme")
	t.Setenv(envInitTenantName, "Acme")
	t.Setenv(envInitAppKey, "assistant")
	t.Setenv(envInitAppName, "Assistant")
	t.Setenv(envInitAppDescription, "Initial app")
	t.Setenv("TRPC_MODEL_API_KEY", "model-secret")

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	previousOpen := openInitDatabase
	previousApply := applyInitMigrations
	previousVerify := verifyInitMigrations
	var applyCalls, verifyCalls int
	openInitDatabase = func(_ context.Context, dsn string, _ postgres.Options) (*sql.DB, error) {
		if dsn != "postgres://init-user@example.test/db" {
			t.Fatalf("init DSN = %q", dsn)
		}
		return db, nil
	}
	applyInitMigrations = func(context.Context, *sql.DB) error {
		applyCalls++
		return nil
	}
	verifyInitMigrations = func(context.Context, *sql.DB) error {
		verifyCalls++
		return nil
	}
	defer func() {
		openInitDatabase = previousOpen
		applyInitMigrations = previousApply
		verifyInitMigrations = previousVerify
	}()

	mock.ExpectBegin()
	mock.ExpectExec("SELECT pg_advisory_xact_lock").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT tenant_id FROM public\\.tenant").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}))
	mock.ExpectQuery("SELECT tenant_id, app_id FROM public\\.agent_app").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "app_id"}))
	mock.ExpectExec("SELECT public\\.control_plane_create_tenant").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("SELECT public\\.control_plane_create_agent_app").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("SELECT 1").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{"present"}).AddRow(1))
	mock.ExpectCommit()
	mock.ExpectClose()

	var output strings.Builder
	if err := runMain(context.Background(), []string{"init", "--confirm"}, &output, io.Discard, nil); err != nil {
		t.Fatal(err)
	}
	if applyCalls != 1 || verifyCalls != 1 {
		t.Fatalf("migration calls = apply:%d verify:%d", applyCalls, verifyCalls)
	}
	value := output.String()
	if !strings.Contains(value, "TRPC_TENANT_ID='t_") || !strings.Contains(value, "TRPC_APP_ID='app_") {
		t.Fatalf("init output = %q", value)
	}
	for _, secret := range []string{"postgres://", "password", "model-secret", "TRPC_MODEL_API_KEY"} {
		if strings.Contains(value, secret) {
			t.Fatalf("init output contains %q: %q", secret, value)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRunInitPreservesCancellation(t *testing.T) {
	t.Setenv(bootstrapPostgresDSN, "postgres://init-user@example.test/db")
	t.Setenv(envInitTenantKey, "acme")
	t.Setenv(envInitTenantName, "Acme")
	t.Setenv(envInitAppKey, "assistant")
	t.Setenv(envInitAppName, "Assistant")
	previousOpen := openInitDatabase
	openInitDatabase = func(ctx context.Context, _ string, _ postgres.Options) (*sql.DB, error) {
		return nil, ctx.Err()
	}
	defer func() { openInitDatabase = previousOpen }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runMain(ctx, []string{"init", "--confirm"}, io.Discard, io.Discard, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled init error = %v", err)
	}
}
