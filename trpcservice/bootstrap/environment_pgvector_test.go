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

func TestEnvironmentPGVectorOptionParsingIsBounded(t *testing.T) {
	options, err := parseEnvironmentPGVectorOptions(map[string]string{
		"schema": "runtime", "collection": "docs", "embedding_model": "model-v2", "embedding_version": "v3",
		"dimension": "64", "queue_size": "8", "workers": "2", "max_attempts": "5",
	})
	if err != nil || options.schema != "runtime" || options.collection != "docs" || options.dimension != 64 || options.queueSize != 8 || options.workers != 2 || options.maxAttempts != 5 {
		t.Fatalf("parsed pgvector options = %#v, %v", options, err)
	}
	if defaults, err := parseEnvironmentPGVectorOptions(nil); err != nil || defaults.dimension != 32 || defaults.queueSize != 128 {
		t.Fatalf("default pgvector options = %#v, %v", defaults, err)
	}
	for _, raw := range []map[string]string{
		{"unknown": "value"},
		{"schema": "bad value"},
		{"dimension": "0"},
		{"queue_size": "10001"},
		{"workers": "nope"},
		{"max_attempts": "101"},
	} {
		if _, err := parseEnvironmentPGVectorOptions(raw); err == nil {
			t.Fatalf("invalid pgvector options accepted: %#v", raw)
		}
	}
	if valueOr(map[string]string{"key": "  value  "}, "key", "fallback") != "value" || valueOr(nil, "key", "fallback") != "fallback" {
		t.Fatal("valueOr did not apply trimming/default")
	}
	for _, endpoint := range []string{"postgres://db.example:5432", "postgresql://DB.EXAMPLE"} {
		if !validEnvironmentPGVectorEndpoint(endpoint) {
			t.Fatalf("valid endpoint rejected: %s", endpoint)
		}
	}
	for _, endpoint := range []string{"", "https://db.example", "postgres://user@db.example", "postgres://db.example?sslmode=disable", "postgres://db.example#fragment"} {
		if validEnvironmentPGVectorEndpoint(endpoint) {
			t.Fatalf("invalid endpoint accepted: %s", endpoint)
		}
	}
	if !samePostgresEndpoint("postgres://db.example", "postgresql://DB.EXAMPLE:5432") || samePostgresEndpoint("postgres://one", "postgres://two") || samePostgresEndpoint("not a URL", "postgres://db.example") {
		t.Fatal("PostgreSQL endpoint equivalence is incorrect")
	}
}
