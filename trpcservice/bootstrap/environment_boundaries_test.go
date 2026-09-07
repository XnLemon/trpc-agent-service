package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestEnvironmentPingAndCatalogBoundaryBranches(t *testing.T) {
	if err := environmentPing(nil, ControlPlaneDriverPostgres, nil, nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("nil ping context error = %v", err)
	}
	canceled := canceledContext()
	if err := environmentPing(canceled, ControlPlaneDriverPostgres, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ping error = %v", err)
	}
	registerBootstrapPingDriver.Do(func() { sql.Register("trpc-service-bootstrap-ping", bootstrapPingDriver{}) })
	db, err := sql.Open("trpc-service-bootstrap-ping", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := environmentPing(context.Background(), ControlPlaneDriverMySQL, db, nil); err != nil {
		t.Fatalf("MySQL ping error = %v", err)
	}
	if err := environmentPing(context.Background(), ControlPlaneDriverPostgres, db, nil); err != nil {
		t.Fatalf("PostgreSQL ping error = %v", err)
	}
	wantErr := errors.New("runtime ping failed")
	if err := environmentPing(context.Background(), ControlPlaneDriverPostgres, db, bootstrapPingRuntimeStore{err: wantErr}); !errors.Is(err, wantErr) {
		t.Fatalf("runtime ping error = %v", err)
	}
	if _, _, err := environmentCatalogs(environmentConfig{demoMode: true, modelProvider: "openai", modelNames: []string{"model"}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("demo catalog provider error = %v", err)
	}
}

func TestEnvironmentComponentSelectionErrorBranches(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, _, _, auditWriter, err := environmentRepositories(environmentConfig{driver: ControlPlaneDriverPostgres, apiIdentities: map[string]gateway.APIIdentity{
		"one": {TenantID: "t_00000000000000000000000000"}, "two": {TenantID: "t_00000000000000000000000001"},
	}}, db)
	if err != nil || auditWriter == nil {
		t.Fatalf("multi-tenant audit writer = %v, %v", auditWriter, err)
	}
	if factory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{}); factory != nil {
		t.Fatal("empty outbox dependencies returned a worker factory")
	}
	store := inmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	missingStoreFactory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{config: environmentConfig{tenantID: "t_00000000000000000000000000"}, aiBotBindings: map[string]struct{}{"binding": {}}})
	if _, err := missingStoreFactory(nil); err == nil {
		t.Fatal("AI Bot worker accepted a runtime store without delivery acknowledgements")
	}
	invalidAdapterFactory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{config: environmentConfig{tenantID: "t_00000000000000000000000000"}, replyStore: store, messageStore: store, deliveryStore: store, aiBotBindings: map[string]struct{}{"binding": {}}})
	if _, err := invalidAdapterFactory([]channels.PollingAdapter{failingPollingAdapter{}}); err == nil {
		t.Fatal("AI Bot worker accepted an adapter with the wrong concrete type")
	}
	countFactory := environmentOutboxWorkerFactory(environmentOutboxWorkerDependencies{config: environmentConfig{tenantID: "t_00000000000000000000000000"}, replyStore: store, messageStore: store, deliveryStore: store, aiBotBindings: map[string]struct{}{"binding": {}}})
	if _, err := countFactory(nil); err == nil {
		t.Fatal("AI Bot worker accepted a mismatched manager count")
	}
	router := environmentReplyProvider{legacy: bootstrapStaticProvider{receipt: "legacy"}, aiBot: bootstrapStaticProvider{receipt: "ai"}, aiBotBindingIDs: map[string]struct{}{"ai": {}}}
	if status, receipt, err := router.Reconcile(context.Background(), runtimestorage.ReplyOutbox{ReplyTarget: runtimestorage.ReplyTarget{BindingID: "legacy"}}); err != nil || status != "accepted" || receipt != "legacy" {
		t.Fatalf("legacy reply reconciliation = %q %q %v", status, receipt, err)
	}
	if _, err := environmentRuntimeProviders(environmentConfig{runtimeStorage: "inmemory"}, environmentRuntimeStores{providers: map[string]environmentStorage{}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing primary runtime provider error = %v", err)
	}
	if _, err := environmentRuntimeProviders(environmentConfig{runtimeStorage: "redis"}, environmentRuntimeStores{providers: map[string]environmentStorage{"redis": store}}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing in-memory runtime fallback error = %v", err)
	}
	registry := storagefactory.NewProviderRegistry()
	if err := registerEnvironmentRuntimeProviders(registry, "invalid", nil, environmentConfig{}, []environmentRuntimeProviderSpec{{name: "inmemory", capabilities: []backend.Capability{backend.CapabilitySession}, store: store}}); !errors.Is(err, storagefactory.ErrInvalid) {
		t.Fatalf("invalid runtime provider scope error = %v", err)
	}
}

type bootstrapPingRuntimeStore struct {
	environmentStorage
	err error
}

func (store bootstrapPingRuntimeStore) Ping(context.Context) error { return store.err }
