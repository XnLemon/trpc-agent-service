package bootstrap

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	agentcontext "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/skill"
)

func TestEnvironmentSkillRepositoryIsTenantAndAppBound(t *testing.T) {
	root := t.TempDir()
	tenantID, appID := "t_00000000000000000000000000", "app_00000000000000000000000000"
	skillDir := filepath.Join(root, "tenants", tenantID, "apps", appID, "demo")
	if err := os.MkdirAll(skillDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(skillDir, skill.SkillFile), []byte("---\nname: demo\ndescription: demo skill\n---\nUse the demo skill.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	provider := environmentSkillRepositoryProvider{root: root}
	ctx := agentcontext.WithExecutionMetadata(context.Background(), agentcontext.ExecutionMetadata{
		TenantID: tenantID, AppID: appID, Revision: 1, UserID: "user", SessionID: "session",
	})
	repository, err := provider.Repository(ctx, skill.SkillScope{AppName: appID})
	if err != nil {
		t.Fatal(err)
	}
	value, err := repository.Get("demo")
	if err != nil || value == nil || value.Summary.Name != "demo" {
		t.Fatalf("scoped skill = %#v, err=%v", value, err)
	}
	if _, err := provider.Repository(ctx, skill.SkillScope{AppName: "other"}); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("cross-app skill scope = %v", err)
	}
	withoutMetadata := context.Background()
	if _, err := provider.Repository(withoutMetadata, skill.SkillScope{AppName: appID}); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("missing metadata skill scope = %v", err)
	}
}

func TestProductionAgentProviderCatalog(t *testing.T) {
	catalog, err := newEnvironmentBackendCatalog("postgres")
	if err != nil {
		t.Fatal(err)
	}
	bindings := []backend.CapabilityBinding{
		{Capability: backend.CapabilityMemory, Provider: "chromadb", Endpoint: "https://chroma.example.test", SecretRef: "vault/chroma", Options: map[string]string{"database": "agents", "collection": "tenant_memories", "dimension": "1536"}},
		{Capability: backend.CapabilityArtifact, Provider: "cos", Endpoint: "https://bucket.cos.ap-guangzhou.myqcloud.com", SecretRef: "vault/cos"},
	}
	if _, err := catalog.NormalizeBindings(bindings); err != nil {
		t.Fatalf("normalize production bindings: %v", err)
	}
	_, err = catalog.NormalizeBindings([]backend.CapabilityBinding{{Capability: backend.CapabilityArtifact, Provider: "s3", Endpoint: "https://s3.example.test", SecretRef: "vault/s3"}})
	if !errors.Is(err, backend.ErrInvalid) {
		t.Fatalf("obsolete S3 Agent Artifact binding error = %v", err)
	}
	if _, err := catalog.NormalizeBindings([]backend.CapabilityBinding{{Capability: backend.CapabilityKnowledge, Provider: "postgres_vector", SecretRef: "vault/embeddings", Options: map[string]string{"dimension": "1536"}}}); err != nil {
		t.Fatalf("PostgreSQL Knowledge binding error = %v", err)
	}
}

func TestCOSArtifactProviderUsesUpstreamService(t *testing.T) {
	secret, err := modelprofile.NewSecretValue("secret-id:secret-key")
	if err != nil {
		t.Fatal(err)
	}
	value, err := (environmentCOSCapabilityProvider{}).New(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000", AppID: "app_00000000000000000000000000"}, backend.CapabilityBinding{
		Capability: backend.CapabilityArtifact,
		Provider:   "cos",
		Endpoint:   "https://bucket.cos.ap-guangzhou.myqcloud.com",
	}, secret)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := value.(artifact.Service); !ok {
		t.Fatalf("COS provider returned %T, want artifact.Service", value)
	}

	for _, invalid := range []modelprofile.SecretValue{{}, mustEnvironmentSecret(t, "missing-separator")} {
		_, err := (environmentCOSCapabilityProvider{}).New(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000", AppID: "app_00000000000000000000000000"}, backend.CapabilityBinding{Capability: backend.CapabilityArtifact, Provider: "cos", Endpoint: "https://bucket.cos.example.test"}, invalid)
		if !errors.Is(err, storagefactory.ErrStorageFactory) {
			t.Fatalf("invalid COS secret error = %v", err)
		}
	}
}

func TestDemoRegistriesMaterializeNativeCapabilitiesWithoutSecrets(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	delegate := sessioninmemory.NewSessionService()
	store := runtimestorageinmemory.New()
	t.Cleanup(func() {
		_ = delegate.Close()
		_ = store.Close()
	})
	config := environmentConfig{
		demoMode: true, modelProvider: demoModelProvider, modelNames: []string{demoModelName}, runtimeStorage: "inmemory",
		apiIdentities: map[string]gateway.APIIdentity{"demo-token": {TenantID: tenantID, AppID: "app_00000000000000000000000000", SubjectID: "demo"}},
	}
	secrets, models, providers, err := environmentRegistries(config, delegate, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := secrets.Resolve(context.Background(), modelprofile.SecretScope{TenantID: tenantID, SecretRef: "env/model"}); err == nil {
		t.Fatal("demo registry unexpectedly stored a model secret")
	}
	model, err := models.New(context.Background(), modelprofile.ModelFactoryInput{TenantID: tenantID, Provider: demoModelProvider, Model: demoModelName}, modelprofile.SecretValue{})
	if err != nil || model == nil || model.Info().Name != demoModelName {
		t.Fatalf("demo model registry = %T, %v", model, err)
	}

	expected := map[backend.Capability]func(any) bool{
		backend.CapabilitySession:   func(v any) bool { _, ok := v.(session.Service); return ok },
		backend.CapabilityMemory:    func(v any) bool { _, ok := v.(memory.Service); return ok },
		backend.CapabilityKnowledge: func(v any) bool { _, ok := v.(knowledge.Knowledge); return ok },
		backend.CapabilityArtifact:  func(v any) bool { _, ok := v.(artifact.Service); return ok },
	}
	for capability, accepts := range expected {
		binding := backend.CapabilityBinding{Capability: capability, Provider: "inmemory"}
		provider, err := providers.Resolve(context.Background(), backend.StorageFactoryInput{TenantID: tenantID, AppID: "app_00000000000000000000000000"}, binding)
		if err != nil {
			t.Fatalf("resolve demo %s: %v", capability, err)
		}
		value, err := provider.New(context.Background(), backend.StorageFactoryInput{TenantID: tenantID, AppID: "app_00000000000000000000000000"}, binding, modelprofile.SecretValue{})
		if err != nil || !accepts(value) {
			t.Fatalf("materialize demo %s = %T, %v", capability, value, err)
		}
	}
}

func TestPostgresVectorKnowledgeProviderUsesUpstreamKnowledgeService(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	secret, err := modelprofile.NewSecretValue("embedding-key")
	if err != nil {
		t.Fatal(err)
	}
	value, err := (environmentPostgresVectorKnowledgeProvider{db: db}).New(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000", AppID: "app_00000000000000000000000000"}, backend.CapabilityBinding{
		Capability: backend.CapabilityKnowledge,
		Provider:   "postgres_vector",
		Options:    map[string]string{"dimension": "32"},
	}, secret)
	if err != nil {
		t.Fatal(err)
	}
	service, ok := value.(knowledge.Knowledge)
	if !ok || service == nil {
		t.Fatalf("Postgres vector provider returned %T", value)
	}
	if closer, ok := service.(interface{ Close() error }); ok {
		if err := closer.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := (environmentPostgresVectorKnowledgeProvider{db: db}).New(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000", AppID: "app_00000000000000000000000000"}, backend.CapabilityBinding{
		Capability: backend.CapabilityKnowledge,
		Provider:   "postgres_vector",
	}, modelprofile.SecretValue{}); !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("missing embedding secret error = %v", err)
	}
}

func TestChromaMemoryProviderRejectsMissingSecretBeforeNetwork(t *testing.T) {
	_, err := (environmentChromaMemoryProvider{}).New(context.Background(), backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000", AppID: "app_00000000000000000000000000"}, backend.CapabilityBinding{
		Capability: backend.CapabilityMemory,
		Provider:   "chromadb",
		Endpoint:   "https://chroma.example.test",
	}, modelprofile.SecretValue{})
	if !errors.Is(err, storagefactory.ErrStorageFactory) {
		t.Fatalf("missing Chroma secret error = %v", err)
	}
}
