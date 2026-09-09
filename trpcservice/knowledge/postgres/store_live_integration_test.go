package postgres_test

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	apppostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	knowledgepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/searchfilter"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestPostgresKnowledgeLiveConformance(t *testing.T) {
	if os.Getenv("TRPC_LIVE_INTEGRATION") != "1" {
		t.Skip("TRPC_LIVE_INTEGRATION=1 is required")
	}
	dsn := os.Getenv("TRPC_POSTGRES_KNOWLEDGE_LIVE_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_KNOWLEDGE_LIVE_DSN is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if migrationDSN := os.Getenv("TRPC_POSTGRES_KNOWLEDGE_LIVE_MIGRATION_DSN"); migrationDSN != "" {
		migrationDB, err := postgres.Open(ctx, migrationDSN, postgres.Options{MaxOpenConns: 2, MaxIdleConns: 2})
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
	}
	db, err := postgres.Open(ctx, dsn, postgres.Options{MaxOpenConns: 8, MaxIdleConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tenantRepo := tenantpostgres.NewRepository(db)
	value, err := tenantRepo.Create(ctx, tenant.CreateInput{TenantKey: "knowledge-" + strconv.FormatInt(time.Now().UnixNano(), 10), DisplayName: "Knowledge live"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_, _ = db.ExecContext(context.Background(), "DELETE FROM public.tenant WHERE tenant_id=$1", value.TenantID)
	}()
	appRepo := apppostgres.NewAppRepository(db)
	appValue, err := appRepo.Create(ctx, appmodel.CreateInput{TenantID: value.TenantID, AppKey: "knowledge-live", DisplayName: "Knowledge live App"})
	if err != nil {
		t.Fatal(err)
	}

	store, err := knowledgepostgres.New(db, value.TenantID, knowledgepostgres.WithAppID(appValue.AppID), knowledgepostgres.WithDimension(3), knowledgepostgres.WithMaxResults(20))
	if err != nil {
		t.Fatal(err)
	}
	first := &document.Document{ID: "live-first", Name: "First", Content: "alpha policy", Metadata: map[string]any{"kind": "policy", "rank": 1, "_trpc_app_id": appValue.AppID}}
	second := &document.Document{ID: "live-second", Name: "Second", Content: "beta note", Metadata: map[string]any{"kind": "note", "rank": 2, "_trpc_app_id": appValue.AppID}}
	if err := store.Add(ctx, first, []float64{1, 0, 0}); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(ctx, second, []float64{0, 1, 0}); err != nil {
		t.Fatal(err)
	}
	loaded, vector, err := store.Get(ctx, first.ID)
	if err != nil || loaded == nil || loaded.Content != first.Content || len(vector) != 3 {
		t.Fatalf("live get = %#v vector=%#v err=%v", loaded, vector, err)
	}
	result, err := store.Search(ctx, &vectorstore.SearchQuery{Vector: []float64{1, 0, 0}, Limit: 10, MinScore: 0.9, Filter: &vectorstore.SearchFilter{Metadata: map[string]any{"kind": "policy"}}})
	if err != nil || len(result.Results) != 1 || result.Results[0].Document.ID != first.ID {
		t.Fatalf("live search = %#v err=%v", result, err)
	}
	count, err := store.Count(ctx, vectorstore.WithCountFilter(map[string]any{"kind": "note"}))
	if err != nil || count != 1 {
		t.Fatalf("live count = %d err=%v", count, err)
	}
	if _, err := store.UpdateByFilter(ctx, vectorstore.WithUpdateByFilterCondition(&searchfilter.UniversalFilterCondition{Field: "metadata.kind", Operator: searchfilter.OperatorEqual, Value: "note"}), vectorstore.WithUpdateByFilterUpdates(map[string]any{"metadata.kind": "memo"})); err != nil {
		t.Fatal(err)
	}
	metadata, err := store.GetMetadata(ctx, vectorstore.WithGetMetadataIDs([]string{second.ID}))
	if err != nil || metadata[second.ID].Metadata["kind"] != "memo" {
		t.Fatalf("live metadata = %#v err=%v", metadata, err)
	}

	var group sync.WaitGroup
	errs := make(chan error, 4)
	for index := 0; index < 4; index++ {
		index := index
		group.Add(1)
		go func() {
			defer group.Done()
			doc := &document.Document{ID: fmt.Sprintf("live-concurrent-%d", index), Content: "concurrent", Metadata: map[string]any{"_trpc_app_id": appValue.AppID}}
			if err := store.Add(ctx, doc, []float64{0, 0, 1}); err != nil {
				errs <- err
			}
		}()
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	restarted, err := knowledgepostgres.New(db, value.TenantID, knowledgepostgres.WithAppID(appValue.AppID), knowledgepostgres.WithDimension(3))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	if _, _, err := restarted.Get(ctx, first.ID); err == nil {
		t.Fatal("deleted document survived restart")
	}
	if err := restarted.DeleteByFilter(ctx, vectorstore.WithDeleteAll(true)); err != nil {
		t.Fatal(err)
	}
}
