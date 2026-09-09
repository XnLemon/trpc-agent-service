package postgres_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	apppostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestoragepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
)

func TestPostgresToolInvocationLiveConformance(t *testing.T) {
	if os.Getenv("TRPC_LIVE_INTEGRATION") != "1" {
		t.Skip("TRPC_LIVE_INTEGRATION=1 is required")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TOOL_INVOCATION_LIVE_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TOOL_INVOCATION_LIVE_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	migrationDSN := os.Getenv("TRPC_POSTGRES_TOOL_INVOCATION_LIVE_MIGRATION_DSN")
	if migrationDSN == "" {
		migrationDSN = dsn
	}
	migrationDB, err := storagepostgres.Open(ctx, migrationDSN, storagepostgres.Options{MaxOpenConns: 2, MaxIdleConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, migrationDB); err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	if err := migrations.Verify(ctx, migrationDB); err != nil {
		_ = migrationDB.Close()
		t.Fatal(err)
	}
	if err := migrationDB.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tenantRepo := tenantpostgres.NewRepository(db)
	value, err := tenantRepo.Create(ctx, tenant.CreateInput{TenantKey: "tool-invocation-" + strconv.FormatInt(time.Now().UnixNano(), 10), DisplayName: "Tool invocation live"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM public.tenant WHERE tenant_id=$1", value.TenantID)
	}()
	appRepo := apppostgres.NewAppRepository(db)
	appValue, err := appRepo.Create(ctx, appmodel.CreateInput{TenantID: value.TenantID, AppKey: "tool-live", DisplayName: "Tool invocation live app"})
	if err != nil {
		t.Fatal(err)
	}
	store := runtimestoragepostgres.New(db)
	input := storage.ToolInvocationInput{TenantID: value.TenantID, AppID: appValue.AppID, EventID: "live-event", RequestID: "live-request", TraceID: "live-trace", ToolCallID: "live-call", ToolName: "mcp_live__write", ArgsSHA256: "", Owner: "live-request"}
	input.ArgsSHA256 = argsDigestForLive(`{"value":1}`)
	input.InvocationID = storage.DeriveToolInvocationIDForApp(input.TenantID, input.AppID, input.EventID, input.ToolCallID, input.ToolName, input.ArgsSHA256)
	prepared, err := store.PrepareToolInvocation(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := store.PrepareToolInvocation(ctx, input); err != nil || duplicate.FencingToken != prepared.FencingToken {
		t.Fatalf("idempotent prepare = %#v err=%v", duplicate, err)
	}
	var group sync.WaitGroup
	accepted := make(chan storage.ToolInvocation, 1)
	conflicts := make(chan error, 4)
	for i := 0; i < 4; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			value, err := store.TransitionToolInvocation(ctx, storage.ToolInvocationTransition{TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: prepared.Status, To: storage.ToolInvocationDispatching, Owner: input.Owner, FencingToken: prepared.FencingToken})
			if err == nil {
				accepted <- value
				return
			}
			conflicts <- err
		}()
	}
	group.Wait()
	close(accepted)
	close(conflicts)
	var dispatching storage.ToolInvocation
	for value := range accepted {
		dispatching = value
	}
	if dispatching.Status != storage.ToolInvocationDispatching {
		t.Fatalf("concurrent transition = %#v", dispatching)
	}
	if _, err := store.TransitionToolInvocation(ctx, storage.ToolInvocationTransition{TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: dispatching.Status, To: storage.ToolInvocationAccepted, Owner: input.Owner, FencingToken: dispatching.FencingToken}); err != nil {
		t.Fatal(err)
	}
	for err := range conflicts {
		if !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("concurrent transition error = %v", err)
		}
	}
	recovered, err := store.RecoverStaleToolInvocations(ctx, time.Now().UTC().Add(time.Second))
	if err != nil || len(recovered) != 1 || recovered[0].Status != storage.ToolInvocationUnknown {
		t.Fatalf("live recovery = %#v err=%v", recovered, err)
	}
	if _, err := store.TransitionToolInvocation(ctx, storage.ToolInvocationTransition{TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: storage.ToolInvocationAccepted, To: storage.ToolInvocationSucceeded, Owner: input.Owner, FencingToken: recovered[0].FencingToken - 1}); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("stale completion = %v", err)
	}
}

func argsDigestForLive(raw string) string {
	// The fixture is intentionally fixed and has no sensitive material. The
	// production plugin canonicalizes JSON before calculating this digest.
	return sha256Hex([]byte(raw))
}

func sha256Hex(value []byte) string {
	// Keep the live fixture independent from the tool plugin package.
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
