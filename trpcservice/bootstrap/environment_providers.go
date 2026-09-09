package bootstrap

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	agentcontext "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentsessionstore "github.com/XnLemon/trpc-agent-service/trpcservice/agent/sessionstore"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	knowledgeadmin "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge"
	knowledgepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge/postgres"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	skillsecurity "github.com/XnLemon/trpc-agent-service/trpcservice/skill"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactcos "trpc.group/trpc-go/trpc-agent-go/artifact/cos"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	knowledgeembedder "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder"
	embedderopenai "trpc.group/trpc-go/trpc-agent-go/knowledge/embedder/openai"
	knowledgevector "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memorychromadb "trpc.group/trpc-go/trpc-agent-go/memory/chromadb"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/skill"
)

type environmentSecretResolver struct {
	reference string
	value     string
}

type environmentWeComCredentialResolver struct {
	tenantID string
	config   environmentWeComConfig
}

func (resolver environmentWeComCredentialResolver) Resolve(ctx context.Context, scope channels.SecretScope) (wecom.Credentials, error) {
	if nilvalue.Is(ctx) {
		return wecom.Credentials{}, errors.New("wecom credential resolver context is required")
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return wecom.Credentials{}, err
	}
	if err := scope.Validate(); err != nil || scope.TenantID != resolver.tenantID || scope.SecretRef != resolver.config.secretRef {
		return wecom.Credentials{}, errors.New("configured WeCom secret reference is unavailable")
	}
	return wecom.Credentials{CallbackToken: resolver.config.callbackToken, EncodingAESKey: resolver.config.encodingAESKey, AppSecret: resolver.config.appSecret}, nil
}

type environmentWeComAIBotCredentialResolver struct {
	tenantID string
	secrets  map[string]string
}

func (resolver environmentWeComAIBotCredentialResolver) Resolve(ctx context.Context, scope channels.SecretScope) (wecom_aibot.Credentials, error) {
	if nilvalue.Is(ctx) {
		return wecom_aibot.Credentials{}, errors.New("wecom ai bot credential resolver context is required")
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return wecom_aibot.Credentials{}, err
	}
	if err := scope.Validate(); err != nil || scope.TenantID != resolver.tenantID {
		return wecom_aibot.Credentials{}, errors.New("configured wecom ai bot secret reference is unavailable")
	}
	secret := resolver.secrets[scope.SecretRef]
	if secret == "" {
		return wecom_aibot.Credentials{}, errors.New("configured wecom ai bot secret reference is unavailable")
	}
	return wecom_aibot.Credentials{BotSecret: secret}, nil
}

func environmentWeComOwner() (string, error) {
	hostname, err := os.Hostname()
	if err != nil || strings.TrimSpace(hostname) == "" {
		return "", errors.New("WeCom worker hostname is unavailable")
	}
	return fmt.Sprintf("wecom-%s-%d", hostname, os.Getpid()), nil
}

func (resolver environmentSecretResolver) Resolve(ctx context.Context, scope modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	if nilvalue.Is(ctx) {
		return modelprofile.SecretValue{}, errors.New("secret resolver context is required")
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return modelprofile.SecretValue{}, err
	}
	if err := scope.Validate(); err != nil {
		return modelprofile.SecretValue{}, err
	}
	if scope.SecretRef != resolver.reference || resolver.value == "" {
		return modelprofile.SecretValue{}, errors.New("configured secret reference is unavailable")
	}
	return modelprofile.NewSecretValue(resolver.value)
}

type environmentModelFactory struct{}

type environmentSessionCapabilityProvider struct {
	delegate  session.Service
	store     environmentStorage
	telemetry observability.Provider
	backend   string
}

type environmentRuntimeCapabilityProvider struct {
	capability            backend.Capability
	delegate              session.Service
	store                 environmentStorage
	telemetry             observability.Provider
	backend               string
	redisEndpoint         string
	redisSecretRef        string
	redisPasswordRequired bool
	memory                memory.Service
	artifact              artifact.Service
	knowledge             knowledge.Knowledge
}

type environmentNativeCapabilityProvider struct {
	capability backend.Capability
}

func (provider environmentNativeCapabilityProvider) New(ctx context.Context, _ backend.StorageFactoryInput, _ backend.CapabilityBinding, _ modelprofile.SecretValue) (any, error) {
	if nilvalue.Is(ctx) {
		return nil, context.Canceled
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return nil, err
	}
	switch provider.capability {
	case backend.CapabilityMemory:
		return memoryinmemory.NewMemoryService(), nil
	case backend.CapabilityArtifact:
		return artifactinmemory.NewService(), nil
	case backend.CapabilityKnowledge:
		return knowledge.New(
			knowledge.WithEmbedder(environmentHashEmbedder{}),
			knowledge.WithVectorStore(knowledgevector.New()),
		), nil
	default:
		return nil, storagefactory.ErrStorageFactory
	}
}

// environmentHashEmbedder is intentionally deterministic for the local/demo
// provider. It exercises the upstream indexing and retrieval pipeline without
// requiring network credentials; production profiles should use an upstream
// remote embedder provider.
type environmentHashEmbedder struct{}

var _ knowledgeembedder.Embedder = environmentHashEmbedder{}

func (environmentHashEmbedder) GetEmbedding(ctx context.Context, text string) ([]float64, error) {
	if nilvalue.Is(ctx) {
		return nil, context.Canceled
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return nil, err
	}
	const dimensions = 32
	var vector [dimensions]float64
	for _, token := range strings.Fields(strings.ToLower(text)) {
		digest := sha256.Sum256([]byte(token))
		index := int(digest[0]) % dimensions
		vector[index] += 1
	}
	return vector[:], nil
}

func (e environmentHashEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	vector, err := e.GetEmbedding(ctx, text)
	return vector, nil, err
}

func (environmentHashEmbedder) GetDimensions() int { return 32 }

type environmentChromaMemoryProvider struct{}

func (environmentChromaMemoryProvider) New(ctx context.Context, input backend.StorageFactoryInput, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	if nilvalue.Is(ctx) || nilvalue.ContextErr(ctx) != nil || input.TenantID == "" || strings.TrimSpace(input.AppID) == "" || binding.Capability != backend.CapabilityMemory || strings.TrimSpace(binding.Endpoint) == "" {
		return nil, storagefactory.ErrStorageFactory
	}
	apiKey := secret.Value()
	if apiKey == "" {
		return nil, storagefactory.ErrStorageFactory
	}
	opts := []memorychromadb.ServiceOpt{
		memorychromadb.WithBaseURL(binding.Endpoint),
		memorychromadb.WithAPIKey(apiKey),
		memorychromadb.WithTenant(input.TenantID),
		memorychromadb.WithDatabase(optionOrDefault(binding.Options, "database", "default_database")),
		memorychromadb.WithCollectionName(optionOrDefault(binding.Options, "collection", "memories")),
		memorychromadb.WithEmbedder(embedderopenai.New(embedderopenai.WithAPIKey(apiKey))),
	}
	if dimension := optionInt(binding.Options, "dimension"); dimension > 0 {
		opts = append(opts, memorychromadb.WithIndexDimension(dimension))
	}
	return memorychromadb.NewService(opts...)
}

func optionOrDefault(options map[string]string, key, fallback string) string {
	if value := strings.TrimSpace(options[key]); value != "" {
		return value
	}
	return fallback
}

func optionInt(options map[string]string, key string) int {
	value, err := strconv.Atoi(strings.TrimSpace(options[key]))
	if err != nil || value < 1 {
		return 0
	}
	return value
}

type environmentPostgresVectorKnowledgeProvider struct {
	db *sql.DB
}

func (provider environmentPostgresVectorKnowledgeProvider) New(ctx context.Context, input backend.StorageFactoryInput, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	if nilvalue.Is(ctx) || nilvalue.ContextErr(ctx) != nil || provider.db == nil || input.TenantID == "" || strings.TrimSpace(input.AppID) == "" || binding.Capability != backend.CapabilityKnowledge || strings.ToLower(strings.TrimSpace(binding.Provider)) != "postgres_vector" || secret.Value() == "" {
		return nil, storagefactory.ErrStorageFactory
	}
	dimension := optionInt(binding.Options, "dimension")
	if dimension == 0 {
		dimension = embedderopenai.DefaultDimensions
	}
	store, err := knowledgepostgres.New(provider.db, input.TenantID, knowledgepostgres.WithAppID(input.AppID), knowledgepostgres.WithDimension(dimension))
	if err != nil {
		return nil, storagefactory.ErrStorageFactory
	}
	embedder := embedderopenai.New(embedderopenai.WithAPIKey(secret.Value()), embedderopenai.WithDimensions(dimension))
	service := knowledge.New(knowledge.WithVectorStore(store), knowledge.WithEmbedder(embedder))
	return service, nil
}

// environmentKnowledgeManagementProvider binds the Admin corpus manager to
// the tenant-scoped PostgreSQL store and its independent embedding SecretRef.
// Runtime capability bindings may use a different secret and are never
// borrowed implicitly by the management path.
type environmentKnowledgeManagementProvider struct {
	db        *sql.DB
	secrets   modelprofile.SecretResolver
	secretRef string
	demo      bool

	mu         sync.Mutex
	demoStores map[string]*knowledgevector.VectorStore
}

func (provider *environmentKnowledgeManagementProvider) Open(ctx context.Context, scope knowledgeadmin.Scope) (knowledgeadmin.Backend, error) {
	if nilvalue.Is(ctx) || provider == nil || scope.Validate() != nil {
		return knowledgeadmin.Backend{}, storagefactory.ErrStorageFactory
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return knowledgeadmin.Backend{}, err
	}
	if provider.demo {
		provider.mu.Lock()
		if provider.demoStores == nil {
			provider.demoStores = make(map[string]*knowledgevector.VectorStore)
		}
		key := scope.TenantID + "\x00" + scope.AppID
		store := provider.demoStores[key]
		if store == nil {
			store = knowledgevector.New()
			provider.demoStores[key] = store
		}
		provider.mu.Unlock()
		return knowledgeadmin.Backend{Store: store, Embedder: environmentHashEmbedder{}}, nil
	}
	if provider.db == nil || nilvalue.Is(provider.secrets) || strings.TrimSpace(provider.secretRef) == "" {
		return knowledgeadmin.Backend{}, storagefactory.ErrStorageFactory
	}
	secret, err := provider.secrets.Resolve(ctx, modelprofile.SecretScope{TenantID: scope.TenantID, SecretRef: provider.secretRef})
	if err != nil || secret.Value() == "" {
		return knowledgeadmin.Backend{}, storagefactory.ErrStorageFactory
	}
	store, err := knowledgepostgres.New(provider.db, scope.TenantID, knowledgepostgres.WithAppID(scope.AppID), knowledgepostgres.WithDimension(embedderopenai.DefaultDimensions))
	if err != nil {
		return knowledgeadmin.Backend{}, storagefactory.ErrStorageFactory
	}
	return knowledgeadmin.Backend{
		Store:    store,
		Embedder: embedderopenai.New(embedderopenai.WithAPIKey(secret.Value()), embedderopenai.WithDimensions(embedderopenai.DefaultDimensions)),
		Close:    store.Close,
	}, nil
}

var _ knowledgeadmin.Provider = (*environmentKnowledgeManagementProvider)(nil)

// environmentSkillRepositoryProvider maps each trusted tenant and app to
// a separate upstream FSRepository. The repository is created per request so
// no live filesystem handle or unscoped global root is retained by a Runner.
type environmentSkillRepositoryProvider struct {
	root string
}

func (provider environmentSkillRepositoryProvider) Repository(ctx context.Context, scope skill.SkillScope) (skill.Repository, error) {
	if nilvalue.Is(ctx) || nilvalue.ContextErr(ctx) != nil || provider.root == "" {
		return nil, storagefactory.ErrStorageFactory
	}
	metadata, ok := agentcontext.ExecutionMetadataFromContext(ctx)
	if !ok || strings.TrimSpace(scope.AppName) == "" || scope.AppName != metadata.AppID || scope.UserID != "" {
		return nil, storagefactory.ErrStorageFactory
	}
	if strings.ContainsAny(metadata.TenantID, "/\\\\\x00\r\n") || strings.Contains(metadata.TenantID, "..") {
		return nil, storagefactory.ErrStorageFactory
	}
	parts, err := skill.ScopePathParts(skill.SkillScopeApp, scope)
	if err != nil {
		return nil, storagefactory.ErrStorageFactory
	}
	pathParts := append([]string{provider.root, "tenants", metadata.TenantID}, parts...)
	repositoryRoot := filepath.Join(pathParts...)
	info, err := os.Stat(repositoryRoot)
	if err != nil || !info.IsDir() {
		return nil, storagefactory.ErrStorageFactory
	}
	resolvedRoot, err := filepath.EvalSymlinks(repositoryRoot)
	if err != nil {
		return nil, storagefactory.ErrStorageFactory
	}
	relative, err := filepath.Rel(provider.root, resolvedRoot)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		return nil, storagefactory.ErrStorageFactory
	}
	repository, err := skill.NewFSRepository(resolvedRoot)
	if err != nil {
		return nil, storagefactory.ErrStorageFactory
	}
	attested, err := skillsecurity.NewFilesystemAttestedRepository(repository, "filesystem")
	if err != nil {
		return nil, storagefactory.ErrStorageFactory
	}
	return attested, nil
}

type environmentCOSCapabilityProvider struct{}

func (environmentCOSCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	if nilvalue.Is(ctx) || nilvalue.ContextErr(ctx) != nil || input.TenantID == "" || strings.TrimSpace(input.AppID) == "" || binding.Capability != backend.CapabilityArtifact || strings.ToLower(strings.TrimSpace(binding.Provider)) != "cos" || strings.TrimSpace(binding.Endpoint) == "" {
		return nil, storagefactory.ErrStorageFactory
	}
	parts := strings.SplitN(secret.Value(), ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, storagefactory.ErrStorageFactory
	}
	service, err := artifactcos.NewService(input.TenantID, binding.Endpoint, artifactcos.WithSecretID(parts[0]), artifactcos.WithSecretKey(parts[1]))
	if err != nil {
		return nil, storagefactory.ErrStorageFactory
	}
	return service, nil
}

func (provider environmentRuntimeCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, binding backend.CapabilityBinding, secret modelprofile.SecretValue) (any, error) {
	if nilvalue.Is(ctx) {
		return nil, context.Canceled
	}
	if err := provider.validateRedisBinding(binding, secret); err != nil {
		return nil, err
	}
	return provider.newCapability(ctx, input)
}

func (provider environmentRuntimeCapabilityProvider) validateRedisBinding(binding backend.CapabilityBinding, secret modelprofile.SecretValue) error {
	if provider.backend != "redis" {
		return nil
	}
	if provider.capability != backend.CapabilitySession && provider.capability != backend.CapabilityMemory {
		return storagefactory.ErrStorageFactory
	}
	if provider.redisEndpoint != "" && binding.Endpoint != provider.redisEndpoint {
		return storagefactory.ErrStorageFactory
	}
	if provider.redisSecretRef != "" && binding.SecretRef != "" && binding.SecretRef != provider.redisSecretRef {
		return storagefactory.ErrStorageFactory
	}
	if provider.redisPasswordRequired && secret.Value() == "" {
		return storagefactory.ErrStorageFactory
	}
	return nil
}

func (provider environmentRuntimeCapabilityProvider) newCapability(ctx context.Context, input backend.StorageFactoryInput) (any, error) {
	if provider.capability == backend.CapabilitySession {
		return agentsessionstore.NewWithObservability(input.TenantID, provider.delegate, provider.store, provider.telemetry, provider.backend)
	}
	// The runtime store is owned by the environment, not by an individual
	// tenant CapabilitySet. Wrap it so factory cleanup cannot stop shared
	// workers when one runner is torn down.
	switch provider.capability {
	case backend.CapabilityMemory:
		if nilvalue.Is(provider.memory) {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedMemoryService{Service: provider.memory}, nil
	case backend.CapabilitySummary:
		store, ok := provider.store.(runtimestorage.SummaryStore)
		if !ok || nilvalue.Is(store) {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedSummaryStore{SummaryStore: store}, nil
	case backend.CapabilityKnowledge:
		if nilvalue.Is(provider.knowledge) {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedKnowledgeService{Knowledge: provider.knowledge}, nil
	case backend.CapabilityArtifact:
		if nilvalue.Is(provider.artifact) {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedArtifactService{Service: provider.artifact}, nil
	case backend.CapabilityAudit:
		store, ok := provider.store.(runtimestorage.AuditStore)
		if !ok || nilvalue.Is(store) {
			return nil, storagefactory.ErrStorageFactory
		}
		return borrowedAuditStore{AuditStore: store}, nil
	default:
		return nil, storagefactory.ErrStorageFactory
	}
}

type borrowedMemoryService struct{ memory.Service }
type borrowedSummaryStore struct{ runtimestorage.SummaryStore }
type borrowedAuditStore struct{ runtimestorage.AuditStore }
type borrowedArtifactService struct{ artifact.Service }
type borrowedKnowledgeService struct{ knowledge.Knowledge }

func (borrowedMemoryService) Close() error    { return nil }
func (borrowedSummaryStore) Close() error     { return nil }
func (borrowedAuditStore) Close() error       { return nil }
func (borrowedArtifactService) Close() error  { return nil }
func (borrowedKnowledgeService) Close() error { return nil }

func (provider environmentSessionCapabilityProvider) New(ctx context.Context, input backend.StorageFactoryInput, _ backend.CapabilityBinding, _ modelprofile.SecretValue) (any, error) {
	if nilvalue.Is(ctx) {
		return nil, context.Canceled
	}
	return agentsessionstore.NewWithObservability(input.TenantID, provider.delegate, provider.store, provider.telemetry, provider.backend)
}

func (environmentModelFactory) New(ctx context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (trpcmodel.Model, error) {
	if nilvalue.Is(ctx) {
		return nil, errors.New("model factory context is required")
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return nil, err
	}
	provider := strings.ToLower(strings.TrimSpace(input.Provider))
	if provider == demoModelProvider {
		return deterministicModel{model: input.Model}, nil
	}
	apiKey := secret.Value()
	if apiKey == "" {
		return nil, errors.New("model factory secret is required")
	}
	if provider != "" && provider != defaultModelProvider {
		return nil, fmt.Errorf("model factory provider %q is unsupported", input.Provider)
	}
	endpoint := strings.TrimSpace(input.Endpoint)
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1"
	}
	return &responsesModel{apiKey: apiKey, endpoint: endpoint, model: input.Model}, nil
}
