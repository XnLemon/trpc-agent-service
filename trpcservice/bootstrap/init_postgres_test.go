package bootstrap

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

type initAttempt struct {
	result InitResult
	err    error
}

// TestPostgreSQLInitializeConcurrent uses a separately provisioned disposable
// database to prove that the initialization advisory lock spans real
// transactions. It intentionally skips when the dedicated test DSN is absent.
func TestPostgreSQLInitializeConcurrent(t *testing.T) {
	ctx, db := openPostgresInitTestDB(t)

	const attempts = 16
	config := InitConfig{
		TenantKey: "concurrent-init", TenantDisplayName: "Concurrent Init",
		AppKey: "assistant", AppDisplayName: "Assistant",
	}
	start := make(chan struct{})
	results := make(chan initAttempt, attempts)
	var group sync.WaitGroup
	group.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer group.Done()
			<-start
			result, initErr := Initialize(ctx, db, config)
			results <- initAttempt{result: result, err: initErr}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	first := assertConcurrentInitResults(t, results)
	assertInitPairCounts(t, ctx, db)

	repeat, err := Initialize(ctx, db, InitConfig{
		TenantKey: "different", TenantDisplayName: "Ignored", AppKey: "different", AppDisplayName: "Ignored",
	})
	if err != nil || repeat.Created || repeat.TenantID != first.TenantID || repeat.AppID != first.AppID {
		t.Fatalf("repeat initialization = %+v, err=%v", repeat, err)
	}
}

func openPostgresInitTestDB(t *testing.T) (context.Context, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("POSTGRES_INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_INIT_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	db, err := storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 16, MaxIdleConns: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Verify(ctx, db); err != nil {
		t.Fatal(err)
	}
	if !isEmptyInitDatabase(t, ctx, db) {
		t.Skip("POSTGRES_INIT_TEST_DSN must point to an empty disposable database")
	}
	return ctx, db
}

func isEmptyInitDatabase(t *testing.T, ctx context.Context, db *sql.DB) bool {
	t.Helper()
	var tenantCount, appCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.tenant").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.agent_app").Scan(&appCount); err != nil {
		t.Fatal(err)
	}
	return tenantCount == 0 && appCount == 0
}

func assertConcurrentInitResults(t *testing.T, results <-chan initAttempt) InitResult {
	t.Helper()
	var first InitResult
	created := 0
	for value := range results {
		if value.err != nil {
			t.Fatalf("concurrent initialization error = %v", value.err)
		}
		if first.TenantID == "" {
			first = value.result
		}
		if value.result.TenantID != first.TenantID || value.result.AppID != first.AppID {
			t.Fatalf("concurrent initialization result = %+v, first = %+v", value.result, first)
		}
		if value.result.Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("concurrent initialization creators = %d, want 1", created)
	}
	return first
}

func assertInitPairCounts(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var tenantTotal, appTotal int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.tenant").Scan(&tenantTotal); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.agent_app").Scan(&appTotal); err != nil {
		t.Fatal(err)
	}
	if tenantTotal != 1 || appTotal != 1 {
		t.Fatalf("concurrent initialization totals = tenant:%d app:%d", tenantTotal, appTotal)
	}
}
