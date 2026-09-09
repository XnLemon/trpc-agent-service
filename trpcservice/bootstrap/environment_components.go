package bootstrap

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	appmysql "github.com/XnLemon/trpc-agent-service/trpcservice/app/mysql"
	apppostgres "github.com/XnLemon/trpc-agent-service/trpcservice/app/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	auditpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/audit/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmysql "github.com/XnLemon/trpc-agent-service/trpcservice/backend/mysql"
	backendpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/backend/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/channels/mysql"
	channelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom_aibot"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmysql "github.com/XnLemon/trpc-agent-service/trpcservice/model/mysql"
	modelpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/model/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	modelruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmysql "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/mysql"
	tenantpostgres "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/postgres"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func environmentRepositories(config environmentConfig, db *sql.DB) (tenant.Repository, appmodel.Repository, channels.CandidateConsumer, audit.Writer, error) {
	if config.driver == ControlPlaneDriverMySQL {
		return tenantmysql.NewRepository(db), appmysql.NewAppRepository(db), channelmysql.NewRepository(db), nil, nil
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

func environmentModelRepository(config environmentConfig, db *sql.DB, catalog *modelprofile.ProviderCatalog) modelprofile.Repository {
	if config.driver == ControlPlaneDriverMySQL {
		return modelmysql.NewRepository(db, catalog)
	}
	return modelpostgres.NewRepository(db, catalog)
}

func environmentBackendRepository(config environmentConfig, db *sql.DB, catalog *backend.ProviderCatalog) backend.Repository {
	if config.driver == ControlPlaneDriverMySQL {
		return backendmysql.NewRepository(db, catalog)
	}
	return backendpostgres.NewRepository(db, catalog)
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
}

// environmentTenantRuntimeDependencies are the control-plane reads required
// to turn one published tenant selection into runtime provider registrations.
// The environment config remains the static provider boundary; repositories
// select the tenant's active model and backend profile at materialization time.
type environmentTenantRuntimeDependencies struct {
	tenants        tenant.Repository
	apps           appmodel.Repository
	models         modelprofile.Repository
	backends       backend.Repository
	modelCatalog   *modelprofile.ProviderCatalog
	backendCatalog *backend.ProviderCatalog
	secrets        modelprofile.SecretResolver
}

type environmentTenantRuntimeOptions struct {
	config           environmentConfig
	delegateSessions session.Service
	runtimeStores    environmentRuntimeStores
	secretRegistry   *modelruntime.SecretRegistry
	modelRegistry    *modelruntime.ModelProviderRegistry
	backendRegistry  *storagefactory.ProviderRegistry
	controlPlane     *environmentTenantRuntimeDependencies
}

func environmentRegistriesForStores(config environmentConfig, delegateSessions session.Service, runtimeStores environmentRuntimeStores) (*modelruntime.SecretRegistry, *modelruntime.ModelProviderRegistry, *storagefactory.ProviderRegistry, error) {
	secretRegistry := modelruntime.NewSecretRegistry()
	modelRegistry := modelruntime.NewModelProviderRegistry()
	backendRegistry := storagefactory.NewProviderRegistry()
	materializer, err := newEnvironmentTenantMaterializer(environmentTenantRuntimeOptions{
		config: config, delegateSessions: delegateSessions, runtimeStores: runtimeStores,
		secretRegistry: secretRegistry, modelRegistry: modelRegistry, backendRegistry: backendRegistry,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	for _, identity := range config.apiIdentities {
		if err := materializer(context.Background(), identity.TenantID); err != nil {
			return nil, nil, nil, err
		}
	}
	return secretRegistry, modelRegistry, backendRegistry, nil
}

func newEnvironmentTenantMaterializer(options environmentTenantRuntimeOptions) (runtime.TenantRuntimeMaterializer, error) {
	runtimeProviders, err := environmentRuntimeProviders(options.config, options.runtimeStores)
	if err != nil {
		return nil, err
	}
	if options.secretRegistry == nil || options.modelRegistry == nil || options.backendRegistry == nil {
		return nil, ErrInvalidConfig
	}
	if options.controlPlane != nil {
		return controlPlaneTenantRuntimeMaterializer(options, runtimeProviders)
	}
	config := options.config
	return func(ctx context.Context, tenantID string) error {
		if ctx == nil || tenantID == "" {
			return ErrInvalidConfig
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if config.demoMode {
			if err := options.modelRegistry.Register(tenantID, demoModelProvider, environmentModelFactory{}); err != nil {
				return err
			}
			return registerEnvironmentRuntimeProviders(options.backendRegistry, tenantID, options.delegateSessions, config, runtimeProviders)
		}
		modelAPIKey := config.modelAPIKey
		if mapped, ok := config.modelAPIKeys[tenantID]; ok {
			modelAPIKey = mapped
		}
		if modelAPIKey == "" {
			return ErrInvalidConfig
		}
		if err := options.secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: config.secretRef}, modelAPIKey); err != nil {
			return err
		}
		if err := options.modelRegistry.Register(tenantID, config.modelProvider, environmentModelFactory{}); err != nil {
			return err
		}
		if config.runtimeStorage == "redis" && config.redis.Password != "" {
			if err := options.secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: config.redisSecretRef}, config.redis.Password); err != nil {
				return err
			}
		}
		if config.s3AccessKeyID != "" {
			if err := options.secretRegistry.RegisterValue(modelprofile.SecretScope{TenantID: tenantID, SecretRef: config.s3SecretRef}, config.s3AccessKeyID+":"+config.s3SecretKey); err != nil {
				return err
			}
		}
		return registerEnvironmentRuntimeProviders(options.backendRegistry, tenantID, options.delegateSessions, config, runtimeProviders)
	}, nil
}

func environmentTenantRuntimeForStores(options environmentTenantRuntimeOptions) (*runtime.TenantRuntimeRegistry, error) {
	materializer, err := newEnvironmentTenantMaterializer(options)
	if err != nil {
		return nil, err
	}
	return runtime.NewTenantRuntimeRegistry(materializer)
}

func controlPlaneTenantRuntimeMaterializer(options environmentTenantRuntimeOptions, runtimeProviders []environmentRuntimeProviderSpec) (runtime.TenantRuntimeMaterializer, error) {
	if options.controlPlane == nil || options.controlPlane.invalid() {
		return nil, ErrInvalidConfig
	}
	config := options.config
	dependencies := options.controlPlane
	return func(ctx context.Context, tenantID string) error {
		modelInput, storageInput, err := dependencies.resolve(ctx, tenantID)
		if err != nil {
			return err
		}
		provider := strings.ToLower(strings.TrimSpace(modelInput.Provider))
		if config.demoMode {
			if provider != demoModelProvider || modelInput.SecretRef != "" {
				return ErrInvalidConfig
			}
		} else {
			expectedProvider := strings.ToLower(strings.TrimSpace(config.modelProvider))
			if expectedProvider == "" {
				expectedProvider = defaultModelProvider
			}
			if provider != expectedProvider {
				return ErrInvalidConfig
			}
			fallback := config.modelAPIKeys[tenantID]
			if fallback == "" {
				fallback = config.modelAPIKey
			}
			if modelInput.SecretRef != config.secretRef {
				fallback = ""
			}
			if err := ensureEnvironmentSecret(ctx, options.secretRegistry, dependencies.secrets, tenantID, modelInput.SecretRef, fallback); err != nil {
				return err
			}
		}
		if err := options.modelRegistry.Register(tenantID, provider, environmentModelFactory{}); err != nil {
			return err
		}
		return options.registerControlPlaneRuntimeProviders(ctx, tenantID, runtimeProviders, storageInput)
	}, nil
}

func (dependencies environmentTenantRuntimeDependencies) invalid() bool {
	return dependencies.tenants == nil || dependencies.apps == nil || dependencies.models == nil || dependencies.backends == nil || dependencies.modelCatalog == nil || dependencies.backendCatalog == nil || dependencies.secrets == nil
}

//nolint:gocyclo // Control-plane resolution validates each scope before any runtime registration.
func (dependencies environmentTenantRuntimeDependencies) resolve(ctx context.Context, tenantID string) (modelprofile.ModelFactoryInput, backend.StorageFactoryInput, error) {
	if ctx == nil || tenantID == "" {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, ErrInvalidConfig
	}
	if err := ctx.Err(); err != nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, err
	}
	root, err := dependencies.tenants.Get(ctx, tenantID)
	if err != nil || root == nil || root.TenantID != tenantID || root.DefaultAgentAppID == nil || root.DefaultBackendProfileID == nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, controlPlaneDependencyError(ctx, err)
	}
	app, err := dependencies.apps.Get(ctx, tenantID, *root.DefaultAgentAppID)
	if err != nil || app == nil || app.TenantID != tenantID || app.AppID != *root.DefaultAgentAppID || !app.CanAcceptExecution() || app.CurrentRevision == nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, controlPlaneDependencyError(ctx, err)
	}
	revisionNumber := *app.CurrentRevision
	if app.CanaryRevision != nil {
		revisionNumber = *app.CanaryRevision
	}
	revision, err := dependencies.apps.GetRevision(ctx, tenantID, app.AppID, revisionNumber)
	if err != nil || revision == nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, controlPlaneDependencyError(ctx, err)
	}
	modelValue, err := dependencies.models.Get(ctx, tenantID, revision.ModelProfileID)
	if err != nil || modelValue == nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, controlPlaneDependencyError(ctx, err)
	}
	backendValue, err := dependencies.backends.Get(ctx, tenantID, *root.DefaultBackendProfileID)
	if err != nil || backendValue == nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, controlPlaneDependencyError(ctx, err)
	}
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, ErrInvalidConfig
	}
	modelSnapshot, err := modelprofile.NewModelExecutionSnapshot(tenantSnapshot, modelValue, dependencies.modelCatalog)
	if err != nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, ErrInvalidConfig
	}
	backendSnapshot, err := backend.NewBackendExecutionSnapshot(tenantSnapshot, backendValue, dependencies.backendCatalog)
	if err != nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, ErrInvalidConfig
	}
	modelInput, err := modelSnapshot.FactoryInput()
	if err != nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, ErrInvalidConfig
	}
	storageInput, err := backendSnapshot.FactoryInput()
	if err != nil {
		return modelprofile.ModelFactoryInput{}, backend.StorageFactoryInput{}, ErrInvalidConfig
	}
	return modelInput, storageInput, nil
}

func controlPlaneDependencyError(ctx context.Context, cause error) error {
	if cause != nil && ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return ErrInvalidConfig
}

func ensureEnvironmentSecret(ctx context.Context, registry *modelruntime.SecretRegistry, resolver modelprofile.SecretResolver, tenantID, secretRef, fallback string) error {
	if ctx == nil || registry == nil || tenantID == "" || secretRef == "" {
		return ErrInvalidConfig
	}
	scope := modelprofile.SecretScope{TenantID: tenantID, SecretRef: secretRef}
	if resolver != nil {
		value, err := resolver.Resolve(ctx, scope)
		if err == nil && value.Value() != "" {
			return registry.Register(scope, value)
		}
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
	}
	if fallback == "" {
		return ErrInvalidConfig
	}
	return registry.RegisterValue(scope, fallback)
}

func (options environmentTenantRuntimeOptions) registerControlPlaneRuntimeProviders(ctx context.Context, tenantID string, runtimeProviders []environmentRuntimeProviderSpec, input backend.StorageFactoryInput) error {
	config := options.config
	secrets := options.controlPlane.secrets
	for _, binding := range input.Bindings {
		providerName := strings.ToLower(strings.TrimSpace(binding.Provider))
		if providerName == "s3" {
			fallback := ""
			if binding.SecretRef == config.s3SecretRef && config.s3AccessKeyID != "" && config.s3SecretKey != "" {
				fallback = config.s3AccessKeyID + ":" + config.s3SecretKey
			}
			if err := ensureEnvironmentSecret(ctx, options.secretRegistry, secrets, tenantID, binding.SecretRef, fallback); err != nil {
				return err
			}
			if err := options.backendRegistry.Register(tenantID, binding.Capability, providerName, environmentS3CapabilityProvider{tenantID: tenantID, secretRef: binding.SecretRef, allowSecretRef: true}); err != nil {
				return err
			}
			continue
		}
		runtimeProvider, ok := environmentRuntimeProvider(runtimeProviders, providerName)
		if !ok || !environmentRuntimeProviderSupports(runtimeProvider, binding.Capability) {
			return ErrInvalidConfig
		}
		if providerName == "redis" {
			if config.redis.Password != "" && binding.SecretRef != config.redisSecretRef {
				return ErrInvalidConfig
			}
			if config.redis.Password == "" && binding.SecretRef != "" {
				return ErrInvalidConfig
			}
		}
		if binding.SecretRef != "" {
			fallback := ""
			if providerName == "redis" {
				fallback = config.redis.Password
			}
			if err := ensureEnvironmentSecret(ctx, options.secretRegistry, secrets, tenantID, binding.SecretRef, fallback); err != nil {
				return err
			}
		}
		provider := environmentRuntimeCapabilityProvider{capability: binding.Capability, delegate: options.delegateSessions, store: runtimeProvider.store, telemetry: config.telemetry, backend: providerName}
		if providerName == "redis" {
			provider.redisEndpoint = config.redisEndpoint
			provider.redisSecretRef = config.redisSecretRef
			provider.redisPasswordRequired = config.redis.Password != ""
		}
		if err := options.backendRegistry.Register(tenantID, binding.Capability, providerName, provider); err != nil {
			return err
		}
	}
	return nil
}

func environmentRuntimeProvider(providers []environmentRuntimeProviderSpec, name string) (environmentRuntimeProviderSpec, bool) {
	for _, provider := range providers {
		if provider.name == name {
			return provider, true
		}
	}
	return environmentRuntimeProviderSpec{}, false
}

func environmentRuntimeProviderSupports(provider environmentRuntimeProviderSpec, capability backend.Capability) bool {
	for _, supported := range provider.capabilities {
		if supported == capability {
			return true
		}
	}
	return false
}

func environmentRuntimeProviders(config environmentConfig, stores environmentRuntimeStores) ([]environmentRuntimeProviderSpec, error) {
	providerName := environmentRuntimeProviderName(config.runtimeStorage)
	primary := stores.providers[providerName]
	if primary == nil {
		return nil, fmt.Errorf("%w: primary runtime provider is unavailable", ErrInvalidConfig)
	}
	providers := []environmentRuntimeProviderSpec{{name: providerName, capabilities: environmentRuntimeCapabilities(config.runtimeStorage), store: primary}}
	if config.runtimeStorage != "redis" {
		return providers, nil
	}
	fallback := stores.providers["inmemory"]
	if fallback == nil {
		return nil, fmt.Errorf("%w: in-memory runtime provider is unavailable", ErrInvalidConfig)
	}
	return append(providers, environmentRuntimeProviderSpec{name: "inmemory", capabilities: environmentRuntimeCapabilities("inmemory"), store: fallback}), nil
}

func registerEnvironmentRuntimeProviders(registry *storagefactory.ProviderRegistry, tenantID string, delegateSessions session.Service, config environmentConfig, runtimeProviders []environmentRuntimeProviderSpec) error {
	for _, runtimeProvider := range runtimeProviders {
		for _, capability := range runtimeProvider.capabilities {
			provider := environmentRuntimeCapabilityProvider{capability: capability, delegate: delegateSessions, store: runtimeProvider.store, telemetry: config.telemetry, backend: runtimeProvider.name}
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
	if err := registry.Register(tenantID, backend.CapabilityArtifact, "s3", environmentS3CapabilityProvider{tenantID: tenantID, secretRef: config.s3SecretRef}); err != nil {
		return err
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
