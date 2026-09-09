package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	knowledgeadmin "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	knowledgeauto "trpc.group/trpc-go/trpc-agent-go/knowledge/source/auto"
)

func TestEnvironmentKnowledgeProviderIndexesAndSearchesUpstreamSource(t *testing.T) {
	ctx := context.Background()
	value, err := (environmentNativeCapabilityProvider{capability: backend.CapabilityKnowledge}).New(ctx, backend.StorageFactoryInput{}, backend.CapabilityBinding{}, modelprofile.SecretValue{})
	if err != nil {
		t.Fatal(err)
	}
	service, ok := value.(*knowledge.BuiltinKnowledge)
	if !ok {
		t.Fatalf("knowledge provider returned %T", value)
	}
	defer service.Close()

	if err := service.AddSource(ctx, knowledgeauto.New([]string{"The tenant isolation policy requires every execution to use its authenticated tenant identity."})); err != nil {
		t.Fatal(err)
	}
	if err := service.Load(ctx, knowledge.WithShowStats(false)); err != nil {
		t.Fatal(err)
	}
	result, err := service.Search(ctx, &knowledge.SearchRequest{Query: "authenticated tenant identity", MaxResults: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Text == "" {
		t.Fatalf("upstream knowledge search returned no text: %+v", result)
	}
}

func TestEnvironmentKnowledgeManagementProviderBindsScopeAndEmbeddingSecret(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	const tenantID = "t_00000000000000000000000000"
	const appID = "app_00000000000000000000000000"
	const secretRef = "env/knowledge-embedding"
	secrets := modelruntime.NewSecretRegistry()
	if err := secrets.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: secretRef}, "embedding-secret"); err != nil {
		t.Fatal(err)
	}
	provider := &environmentKnowledgeManagementProvider{db: db, secrets: secrets, secretRef: secretRef}
	backend, err := provider.Open(context.Background(), knowledgeadmin.Scope{TenantID: tenantID, AppID: appID})
	if err != nil || backend.Store == nil || backend.Embedder == nil || backend.Close == nil {
		t.Fatalf("scoped management backend = %#v, error = %v", backend, err)
	}
	if err := backend.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Open(context.Background(), knowledgeadmin.Scope{TenantID: tenantID, AppID: "app_00000000000000000000000001"}); err != nil {
		// A different App is a distinct explicit partition and is allowed to
		// open; it must not inherit the first Store instance.
		t.Fatal(err)
	}
	if _, err := provider.Open(context.Background(), knowledgeadmin.Scope{TenantID: "t_00000000000000000000000001", AppID: appID}); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("foreign tenant management backend error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := provider.Open(cancelled, knowledgeadmin.Scope{TenantID: tenantID, AppID: appID}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled management backend error = %v", err)
	}
	if _, err := provider.Open(context.Background(), knowledgeadmin.Scope{TenantID: tenantID, AppID: " app"}); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("invalid app management backend error = %v", err)
	}
}
