package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmysql "github.com/XnLemon/trpc-agent-service/trpcservice/app/mysql"
	apppostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	auditmysql "github.com/XnLemon/trpc-agent-service/trpcservice/audit/mysql"
	auditpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/audit/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/channels/mysql"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func environmentRepositories(config environmentConfig, db *sql.DB) (tenant.Repository, appmodel.Repository, channels.CandidateConsumer, audit.Writer, error) {
	if config.driver == ControlPlaneDriverMySQL {
		var auditWriter audit.Writer
		var err error
		if len(config.apiIdentities) > 1 {
			auditWriter = auditmysql.NewMultiTenant(db)
		} else {
			auditWriter, err = auditmysql.New(db, config.tenantID)
		}
		return tenantmysql.NewRepository(db), appmysql.NewAppRepository(db), channelmysql.NewRepository(db), auditWriter, err
	}
	tenantRepo := tenantpostgres.NewRepository(db)
	appRepo := apppostgres.NewAppRepository(db)
	channelRepo := channelpostgres.NewRepository(db)
	var auditWriter audit.Writer
	var err error
	if len(config.apiIdentities) > 1 {
		auditWriter = auditpostgres.NewMultiTenant(db)
	} else {
		auditWriter, err = auditpostgres.New(db, config.tenantID)
	}
	return tenantRepo, appRepo, channelRepo, auditWriter, err
}

type environmentWeComDependencies struct {
	config      environmentConfig
	channels    channels.CandidateConsumer
	tenants     tenant.Repository
	apps        appmodel.Repository
	attachments runtimestorage.AttachmentStore
	auditWriter audit.Writer
}

func environmentWeComComponents(dependencies environmentWeComDependencies) (func(gateway.DispatchService) (http.Handler, error), outbox.Provider, error) {
	config := dependencies.config
	if config.wecom == nil {
		return nil, nil, nil
	}
	credentials := environmentWeComCredentialResolver{tenantID: config.tenantID, config: *config.wecom}
	var mediaDownloader wecom.MediaDownloader
	if dependencies.attachments != nil {
		mediaDownloader = &wecom.HTTPMediaDownloader{}
	}
	factory := func(dispatcher gateway.DispatchService) (http.Handler, error) {
		return wecom.New(wecom.Config{Candidates: dependencies.channels, Tenants: dependencies.tenants, Apps: dependencies.apps, Credentials: credentials, Dispatcher: dispatcher, Attachments: dependencies.attachments, MediaDownloader: mediaDownloader, AuditWriter: dependencies.auditWriter, Observability: config.telemetry})
	}
	return factory, &wecom.BindingProvider{Bindings: dependencies.channels, Credentials: credentials}, nil
}

type environmentWeComAIBotDependencies struct {
	ctx      context.Context
	config   environmentConfig
	channels channels.CandidateConsumer
	tenants  tenant.Repository
	apps     appmodel.Repository
}

func environmentWeComAIBotComponents(dependencies environmentWeComAIBotDependencies) ([]func(gateway.DispatchService) (channels.PollingAdapter, error), map[string]struct{}, error) {
	config := dependencies.config
	if len(config.wecomAIBots) == 0 {
		return nil, nil, nil
	}
	secrets := make(map[string]string, len(config.wecomAIBots))
	targets := make([]channels.RoutingTarget, 0, len(config.wecomAIBots))
	for _, value := range config.wecomAIBots {
		if _, exists := secrets[value.SecretRef]; exists {
			return nil, nil, errors.New("wecom ai bot secret reference is duplicated")
		}
		secrets[value.SecretRef] = value.BotSecret
		target, err := channels.ResolveConfiguredRoutingTarget(dependencies.ctx, dependencies.channels, dependencies.tenants, dependencies.apps, config.tenantID, value.BindingID)
		if err != nil || target.Channel != channels.ChannelWeComAIBot {
			return nil, nil, errors.New("wecom ai bot binding is unavailable")
		}
		targets = append(targets, target)
	}
	credentials := environmentWeComAIBotCredentialResolver{tenantID: config.tenantID, secrets: secrets}
	factories := make([]func(gateway.DispatchService) (channels.PollingAdapter, error), 0, len(targets))
	bindingIDs := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		target := target
		bindingIDs[target.BindingID] = struct{}{}
		factories = append(factories, func(dispatcher gateway.DispatchService) (channels.PollingAdapter, error) {
			return wecom_aibot.NewForBinding(dependencies.ctx, wecom_aibot.BindingConfig{Target: target, Bindings: dependencies.channels, Credentials: credentials, Dispatcher: dispatcher})
		})
	}
	return factories, bindingIDs, nil
}

type environmentOutboxWorkerDependencies struct {
	config        environmentConfig
	replyStore    runtimestorage.ReplyStore
	messageStore  runtimestorage.MessageStore
	deliveryStore wecom_aibot.DeliveryStore
	auditWriter   audit.Writer
	legacy        outbox.Provider
	aiBotBindings map[string]struct{}
}

func environmentOutboxWorkerFactory(dependencies environmentOutboxWorkerDependencies) func([]channels.PollingAdapter) (*outbox.Worker, error) {
	config := dependencies.config
	replyStore := dependencies.replyStore
	messageStore := dependencies.messageStore
	deliveryStore := dependencies.deliveryStore
	auditWriter := dependencies.auditWriter
	legacy := dependencies.legacy
	aiBotBindingIDs := dependencies.aiBotBindings
	if legacy == nil && len(aiBotBindingIDs) == 0 {
		return nil
	}
	return func(adapters []channels.PollingAdapter) (*outbox.Worker, error) {
		provider := legacy
		channel, providerName := "wecom", "wecom"
		leaseDuration := 30 * time.Second
		if len(aiBotBindingIDs) > 0 {
			leaseDuration = wecom_aibot.OutboxLeaseDuration
			if deliveryStore == nil {
				return nil, errors.New("runtime store does not support durable reply acknowledgements")
			}
			managers := make([]*wecom_aibot.Manager, 0, len(aiBotBindingIDs))
			for _, adapter := range adapters {
				manager, ok := adapter.(*wecom_aibot.Manager)
				if !ok {
					return nil, errors.New("wecom ai bot adapter has an invalid type")
				}
				managers = append(managers, manager)
			}
			if len(managers) != len(aiBotBindingIDs) {
				return nil, errors.New("wecom ai bot manager count is invalid")
			}
			aiBotProvider, err := wecom_aibot.NewBindingProvider(deliveryStore, managers...)
			if err != nil {
				return nil, err
			}
			if provider == nil {
				provider, channel, providerName = aiBotProvider, "wecom_aibot", "wecom_aibot"
			} else {
				provider = environmentReplyProvider{legacy: provider, aiBot: aiBotProvider, aiBotBindingIDs: aiBotBindingIDs}
			}
		}
		owner, err := environmentWeComOwnerFunc()
		if err != nil {
			return nil, err
		}
		return newEnvironmentWeComWorker(outbox.Config{Store: replyStore, MessageStore: messageStore, Provider: provider, Channel: channel, ProviderName: providerName, TenantID: config.tenantID, Owner: owner, LeaseDuration: leaseDuration, AuditWriter: auditWriter, Observability: config.telemetry})
	}
}

func environmentPrimaryDeliveryCapabilities(runtimeStore environmentStorage) (runtimestorage.ReplyStore, runtimestorage.MessageStore, wecom_aibot.DeliveryStore) {
	deliveryStore, _ := runtimeStore.(wecom_aibot.DeliveryStore)
	return runtimeStore, runtimeStore, deliveryStore
}

type environmentReplyProvider struct {
	legacy          outbox.Provider
	aiBot           outbox.Provider
	aiBotBindingIDs map[string]struct{}
}

func (p environmentReplyProvider) Deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	if _, ok := p.aiBotBindingIDs[value.ReplyTarget.BindingID]; ok {
		return p.aiBot.Deliver(ctx, value)
	}
	return p.legacy.Deliver(ctx, value)
}

func (p environmentReplyProvider) Reconcile(ctx context.Context, value runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if _, ok := p.aiBotBindingIDs[value.ReplyTarget.BindingID]; ok {
		return p.aiBot.Reconcile(ctx, value)
	}
	return p.legacy.Reconcile(ctx, value)
}

func environmentRegistries(config environmentConfig, delegateSessions session.Service, runtimeStore environmentStorage) (*modelruntime.SecretRegistry, *modelruntime.ModelProviderRegistry, *storagefactory.ProviderRegistry, error) {
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	return environmentRegistriesForStores(config, delegateSessions, environmentRuntimeStores{
		primary:   runtimeStore,
		providers: map[string]environmentStorage{providerName: runtimeStore},
	})
}

type environmentRuntimeProviderSpec struct {
	name         string
	capabilities []backend.Capability
	store        environmentStorage
	database     *sql.DB
}

func environmentRegistriesForStores(config environmentConfig, delegateSessions session.Service, runtimeStores environmentRuntimeStores) (*modelruntime.SecretRegistry, *modelruntime.ModelProviderRegistry, *storagefactory.ProviderRegistry, error) {
	secretRegistry := modelruntime.NewSecretRegistry()
	modelRegistry := modelruntime.NewModelProviderRegistry()
	backendRegistry := storagefactory.NewProviderRegistry()
	runtimeProviders, err := environmentRuntimeProviders(config, runtimeStores)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, identity := range config.apiIdentities {
		if config.demoMode {
			if err := modelRegistry.Register(identity.TenantID, demoModelProvider, environmentModelFactory{}); err != nil {
				return nil, nil, nil, err
			}
			if err := registerEnvironmentRuntimeProviders(backendRegistry, identity.TenantID, delegateSessions, config, runtimeProviders); err != nil {
				return nil, nil, nil, err
			}
			continue
		}
		modelAPIKey := config.modelAPIKey
		if len(config.modelAPIKeys) != 0 {
			modelAPIKey = config.modelAPIKeys[identity.TenantID]
		}
		if modelAPIKey == "" {
			return nil, nil, nil, ErrInvalidConfig
		}
		if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.secretRef}, modelAPIKey); err != nil {
			return nil, nil, nil, err
		}
		embeddingAPIKey := config.knowledgeEmbeddingAPIKey
		if len(config.knowledgeEmbeddingAPIKeys) != 0 {
			embeddingAPIKey = config.knowledgeEmbeddingAPIKeys[identity.TenantID]
		}
		// loadEnvironment requires this pair for PostgreSQL Knowledge. Keep
		// the lower-level registry constructor usable by tests and explicitly
		// local callers that do not enable Knowledge administration; the
		// management provider itself still fails closed when the pair is absent.
		if embeddingAPIKey != "" && config.knowledgeEmbeddingSecretRef != "" {
			if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.knowledgeEmbeddingSecretRef}, embeddingAPIKey); err != nil {
				return nil, nil, nil, err
			}
		}
		if err := modelRegistry.Register(identity.TenantID, config.modelProvider, environmentModelFactory{}); err != nil {
			return nil, nil, nil, err
		}
		if config.runtimeStorage == "redis" && config.redis.Password != "" {
			if err := secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: identity.TenantID, SecretRef: config.redisSecretRef}, config.redis.Password); err != nil {
				return nil, nil, nil, err
			}
		}
		if err := registerEnvironmentRuntimeProviders(backendRegistry, identity.TenantID, delegateSessions, config, runtimeProviders); err != nil {
			return nil, nil, nil, err
		}
	}
	return secretRegistry, modelRegistry, backendRegistry, nil
}

func environmentRuntimeProviders(config environmentConfig, stores environmentRuntimeStores) ([]environmentRuntimeProviderSpec, error) {
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	primary := stores.providers[providerName]
	if primary == nil {
		return nil, fmt.Errorf("%w: primary runtime provider is unavailable", ErrInvalidConfig)
	}
	providers := []environmentRuntimeProviderSpec{{name: providerName, capabilities: environmentRuntimeCapabilities(config.runtimeStorage), store: primary, database: stores.database}}
	if config.runtimeStorage != "redis" {
		return providers, nil
	}
	fallback := stores.providers["inmemory"]
	if fallback == nil {
		return nil, fmt.Errorf("%w: in-memory runtime provider is unavailable", ErrInvalidConfig)
	}
	return append(providers, environmentRuntimeProviderSpec{name: "inmemory", capabilities: environmentRuntimeCapabilities("inmemory"), store: fallback, database: stores.database}), nil
}

func registerEnvironmentRuntimeProviders(registry *storagefactory.ProviderRegistry, tenantID string, delegateSessions session.Service, config environmentConfig, runtimeProviders []environmentRuntimeProviderSpec) error {
	for _, runtimeProvider := range runtimeProviders {
		sharedMemory := memoryinmemory.NewMemoryService()
		sharedArtifact := artifactinmemory.NewService()
		sharedKnowledge := knowledge.New()
		for _, capability := range runtimeProvider.capabilities {
			provider := environmentRuntimeCapabilityProvider{
				capability: capability, delegate: delegateSessions, store: runtimeProvider.store,
				telemetry: config.telemetry, backend: runtimeProvider.name,
				memory: sharedMemory, artifact: sharedArtifact, knowledge: sharedKnowledge,
			}
			if runtimeProvider.name == "redis" {
				provider.redisEndpoint = config.redisEndpoint
				provider.redisSecretRef = config.redisSecretRef
				provider.redisPasswordRequired = config.redis.Password != ""
			}
			if err := registry.Register(tenantID, capability, runtimeProvider.name, provider); err != nil {
				return err
			}
		}
	}
	if err := registry.Register(tenantID, backend.CapabilityMemory, "chromadb", environmentChromaMemoryProvider{}); err != nil {
		return err
	}
	if err := registry.Register(tenantID, backend.CapabilityArtifact, "cos", environmentCOSCapabilityProvider{}); err != nil {
		return err
	}
	if config.driver != ControlPlaneDriverMySQL && len(runtimeProviders) > 0 && runtimeProviders[0].database != nil {
		if err := registry.Register(tenantID, backend.CapabilityKnowledge, "postgres_vector", environmentPostgresVectorKnowledgeProvider{db: runtimeProviders[0].database}); err != nil {
			return err
		}
	}
	return nil
}

func environmentRuntimeProviderName(runtimeStorage string) string {
	if runtimeStorage == "redis" {
		return "redis"
	}
	return "inmemory"
}

func environmentRuntimeCapabilities(runtimeStorage string) []backend.Capability {
	if runtimeStorage == "redis" {
		return []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory}
	}
	return []backend.Capability{backend.CapabilitySession, backend.CapabilityMemory, backend.CapabilitySummary, backend.CapabilityKnowledge, backend.CapabilityArtifact, backend.CapabilityAudit}
}
