package bootstrap

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestEnvironmentRegistersTenantBoundPGVectorProvider(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimestorageinmemory.New()
	defer func() { _ = store.Close() }()
	registry := backend.NewProviderRegistry()
	config := environmentConfig{runtimeStorage: "postgres", dsn: "postgres://user:password@db.example:5432/runtime"}
	providers := []environmentRuntimeProviderSpec{{name: "inmemory", capabilities: environmentRuntimeCapabilities("postgres"), store: store, database: db}}
	if err := registerEnvironmentRuntimeProviders(registry, tenantID, nil, config, providers); err != nil {
		t.Fatal(err)
	}
	provider, err := registry.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: tenantID}, backend.CapabilityBinding{Capability: backend.CapabilityKnowledge, Provider: "pgvector"})
	if err != nil {
		t.Fatal(err)
	}
	value, err := provider.New(context.Background(), backend.StorageFactoryInput{TenantID: tenantID}, backend.CapabilityBinding{Capability: backend.CapabilityKnowledge, Provider: "pgvector", Endpoint: "postgres://db.example:5432"}, modelprofile.SecretValue{})
	if err != nil {
		t.Fatal(err)
	}
	capability, ok := value.(interface {
		runtimestorage.KnowledgeStore
		runtimestorage.VectorStore
		Close() error
	})
	if !ok {
		t.Fatalf("provider value = %T", value)
	}
	if err := capability.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.New(context.Background(), backend.StorageFactoryInput{TenantID: tenantID}, backend.CapabilityBinding{Capability: backend.CapabilityKnowledge, Provider: "pgvector", Endpoint: "postgres://other.example:5432"}, modelprofile.SecretValue{}); err == nil {
		t.Fatal("foreign PostgreSQL endpoint was accepted")
	}
}

func TestEnvironmentPGVectorCatalogValidatesNamespaceAndEndpoint(t *testing.T) {
	catalog, err := newEnvironmentBackendCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	bindings, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{
		Capability: backend.CapabilityKnowledge,
		Provider:   "pgvector",
		Endpoint:   "postgresql://db.example:5432/runtime",
	}})
	if err != nil || len(bindings) != 1 {
		t.Fatalf("pgvector binding = %#v, %v", bindings, err)
	}
	if bindings[0].Options["collection"] != "knowledge" || bindings[0].Options["dimension"] != "32" || bindings[0].Options["max_attempts"] != "3" {
		t.Fatalf("pgvector defaults = %#v", bindings[0].Options)
	}
	for _, endpoint := range []string{"postgres://user:password@db.example:5432/runtime", "https://db.example", "postgres://db.example:5432/runtime?sslmode=disable"} {
		if _, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{Capability: backend.CapabilityKnowledge, Provider: "pgvector", Endpoint: endpoint}}); err == nil {
			t.Fatalf("unsafe pgvector endpoint %q was accepted", endpoint)
		}
	}
	if _, err := newEnvironmentBackendCatalog("inmemory"); err != nil {
		t.Fatal(err)
	}
}
