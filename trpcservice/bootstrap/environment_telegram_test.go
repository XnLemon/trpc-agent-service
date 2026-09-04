package bootstrap

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentmemory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

func TestEnvironmentTelegramConfigurationRequiresOneSafePollingBinding(t *testing.T) {
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	identity := gateway.APIIdentity{TenantID: tenantID, AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV"}
	base := environmentConfig{tenantID: tenantID, apiIdentities: map[string]gateway.APIIdentity{"api-token": identity}}
	tests := []struct {
		name      string
		configure func()
		base      environmentConfig
		wantErr   bool
		wantMode  string
	}{
		{name: "disabled"},
		{
			name: "valid polling",
			configure: func() {
				t.Setenv(envTelegramBotToken, "bot-token")
				t.Setenv(envTelegramBindingID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAV")
				t.Setenv(envTelegramSecretRef, "env/telegram")
				t.Setenv(envTelegramMode, "polling")
			},
			wantMode: "polling",
		},
		{
			name:      "partial credentials",
			configure: func() { t.Setenv(envTelegramBotToken, "bot-token") },
			wantErr:   true,
		},
		{
			name: "webhook mode is not an environment transport",
			configure: func() {
				t.Setenv(envTelegramBotToken, "bot-token")
				t.Setenv(envTelegramBindingID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAV")
				t.Setenv(envTelegramSecretRef, "env/telegram")
				t.Setenv(envTelegramMode, "webhook")
			},
			wantErr: true,
		},
		{
			name: "multiple identities",
			configure: func() {
				t.Setenv(envTelegramBotToken, "bot-token")
				t.Setenv(envTelegramBindingID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAV")
				t.Setenv(envTelegramSecretRef, "env/telegram")
			},
			base: environmentConfig{tenantID: tenantID, apiIdentities: map[string]gateway.APIIdentity{
				"api-one": identity, "api-two": {TenantID: tenantID, AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			}},
			wantErr: true,
		},
		{
			name: "demo mode",
			configure: func() {
				t.Setenv(envTelegramBotToken, "bot-token")
				t.Setenv(envTelegramBindingID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAV")
				t.Setenv(envTelegramSecretRef, "env/telegram")
			},
			base:    environmentConfig{tenantID: tenantID, demoMode: true, apiIdentities: base.apiIdentities},
			wantErr: true,
		},
		{
			name: "token contains a newline",
			configure: func() {
				t.Setenv(envTelegramBotToken, "bot-token\nsecret")
				t.Setenv(envTelegramBindingID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAV")
				t.Setenv(envTelegramSecretRef, "env/telegram")
				t.Setenv(envTelegramMode, "polling")
			},
			wantErr: true,
		},
		{
			name: "binding contains a newline",
			configure: func() {
				t.Setenv(envTelegramBotToken, "bot-token")
				t.Setenv(envTelegramBindingID, "binding\nsecret")
				t.Setenv(envTelegramSecretRef, "env/telegram")
				t.Setenv(envTelegramMode, "polling")
			},
			wantErr: true,
		},
		{
			name: "secret reference is outside the tenant scope",
			configure: func() {
				t.Setenv(envTelegramBotToken, "bot-token")
				t.Setenv(envTelegramBindingID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAV")
				t.Setenv(envTelegramSecretRef, "other\ntelegram")
				t.Setenv(envTelegramMode, "polling")
			},
			wantErr: true,
		},
		{
			name: "polling mode is case insensitive",
			configure: func() {
				t.Setenv(envTelegramBotToken, "bot-token")
				t.Setenv(envTelegramBindingID, "cb_01ARZ3NDEKTSV4RRFFQ69G5FAV")
				t.Setenv(envTelegramSecretRef, "env/telegram")
				t.Setenv(envTelegramMode, "POLLING")
			},
			wantMode: "polling",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, name := range []string{envTelegramBotToken, envTelegramBindingID, envTelegramSecretRef, envTelegramMode} {
				t.Setenv(name, "")
			}
			if test.configure != nil {
				test.configure()
			}
			config := test.base
			if config.apiIdentities == nil {
				config = base
			}
			err := config.loadTelegram()
			if test.wantErr {
				if !errors.Is(err, ErrInvalidConfig) {
					t.Fatalf("loadTelegram() error = %v", err)
				}
				if strings.Contains(err.Error(), "bot-token") {
					t.Fatal("Telegram token was echoed in configuration error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if test.wantMode == "" {
				if config.telegram != nil {
					t.Fatalf("disabled Telegram configuration = %+v", config.telegram)
				}
				return
			}
			if config.telegram == nil || config.telegram.mode != test.wantMode || config.telegram.botToken != "bot-token" {
				t.Fatalf("Telegram configuration = %+v", config.telegram)
			}
		})
	}
}

func TestEnvironmentTelegramComponentsSealBindingAndPassAttachmentStore(t *testing.T) {
	modelCatalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"test-model"}, EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldOptional,
	})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{
		Provider: "memory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString}},
	})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantmemory.NewRepository()
	apps := agentmemory.NewRepository()
	models := modelmemory.NewRepository(modelCatalog)
	backends := backendmemory.NewRepository(backendCatalog)
	channelsRepo := channelmemory.NewRepository()
	root, app := createBootstrapTenantExecutionState(t, tenants, apps, models, backends, "telegram-components", "telegram-components", "test-model", "secret/model")
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "telegram-components")
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err := channelsRepo.Create(context.Background(), channels.CreateInput{
		TenantID: root.TenantID, BindingKey: "telegram-components", Channel: channels.ChannelTelegram,
		ProviderAccountID: "12345", PublicRouteKeyDigest: routeDigest, AppID: app.AppID,
		SecretRef: "env/telegram", Status: channels.StatusActive,
		Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{APIBaseURL: "https://api.telegram.org"}},
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "bootstrap", Reason: "fixture", CorrelationID: "telegram-components"},
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeStore := runtimestorageinmemory.New()
	defer func() { _ = runtimeStore.Close() }()
	config := environmentConfig{
		tenantID: root.TenantID, telemetry: nil,
		telegram: &environmentTelegramConfig{bindingID: binding.BindingID, secretRef: binding.SecretRef, botToken: "bot-token", mode: "polling"},
	}
	previous := newEnvironmentTelegramAdapter
	defer func() { newEnvironmentTelegramAdapter = previous }()
	var received telegram.Config
	newEnvironmentTelegramAdapter = func(ctx context.Context, receivedConfig telegram.Config) (*telegram.Adapter, error) {
		received = receivedConfig
		receivedConfig.Factory = telegram.BotFactoryFunc(func(string, telegram.BotFactoryConfig) (telegram.BotClient, error) {
			return environmentTelegramBot{}, nil
		})
		return telegram.New(ctx, receivedConfig)
	}
	factory, err := environmentTelegramComponents(context.Background(), config, channelsRepo, tenants, apps, runtimeStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	value, err := factory(bootstrapNoopDispatcher{})
	if err != nil {
		t.Fatal(err)
	}
	adapter, ok := value.(*telegram.Adapter)
	if !ok {
		t.Fatalf("Telegram adapter type = %T", value)
	}
	defer adapter.Close()
	if received.BotToken != "bot-token" || received.Target.BindingID != binding.BindingID || received.Target.TenantID != root.TenantID || received.APIBaseURL != "https://api.telegram.org" || received.Attachments == nil {
		t.Fatalf("Telegram adapter config = %+v", received)
	}
	provider, err := telegram.NewBindingProvider(adapter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Deliver(context.Background(), runtimestorage.ReplyOutbox{TenantID: root.TenantID, ReplyID: "reply", SegmentIndex: 0, Payload: "hello", ReplyTarget: runtimestorage.ReplyTarget{BindingID: binding.BindingID, ConversationKind: "direct", ReceiverID: "42"}}); err != nil {
		t.Fatalf("Telegram binding provider delivery = %v", err)
	}
	bad := config
	bad.telegram = &environmentTelegramConfig{bindingID: binding.BindingID, secretRef: "env/other", botToken: "bot-token", mode: "polling"}
	if _, err := environmentTelegramComponents(context.Background(), bad, channelsRepo, tenants, apps, runtimeStore, nil); err == nil {
		t.Fatal("Telegram binding with mismatched SecretRef was accepted")
	}
	assertEnvironmentTelegramOutboxRouting(t, config, runtimeStore, root.TenantID, binding.BindingID, adapter)
	assertEnvironmentTelegramFactoryFailures(t, config, channelsRepo, tenants, apps, runtimeStore, binding.BindingID, factory, &received)
}

func assertEnvironmentTelegramOutboxRouting(t *testing.T, config environmentConfig, runtimeStore runtimestorage.RuntimeStore, tenantID, bindingID string, adapter *telegram.Adapter) {
	t.Helper()
	telegramProvider, bindingIDs, err := environmentTelegramOutboxProvider(config, []channels.PollingAdapter{adapter})
	if err != nil || telegramProvider == nil || len(bindingIDs) != 1 {
		t.Fatalf("Telegram outbox provider = %v, %v, %v", telegramProvider, bindingIDs, err)
	}
	selected, channel, providerName, leaseDuration, err := environmentOutboxProvider(config, runtimeStore, nil, nil, []channels.PollingAdapter{adapter})
	if err != nil || selected == nil || channel != "telegram" || providerName != "telegram" || leaseDuration <= 0 {
		t.Fatalf("Telegram outbox selection = %T/%q/%q/%s/%v", selected, channel, providerName, leaseDuration, err)
	}
	legacy := bootstrapStaticProvider{receipt: "legacy"}
	mixed, channel, providerName, _, err := environmentOutboxProvider(config, runtimeStore, legacy, nil, []channels.PollingAdapter{adapter})
	if err != nil || channel != "mixed" || providerName != "mixed" {
		t.Fatalf("mixed outbox selection = %q/%q/%v", channel, providerName, err)
	}
	if receipt, err := mixed.Deliver(context.Background(), runtimestorage.ReplyOutbox{TenantID: tenantID, ReplyID: "mixed-reply", SegmentIndex: 0, Payload: "mixed", ReplyTarget: runtimestorage.ReplyTarget{BindingID: bindingID, ConversationKind: "direct", ReceiverID: "42"}}); err != nil || receipt != "1" {
		t.Fatalf("mixed Telegram delivery = %q, %v", receipt, err)
	}
	if _, _, err := environmentTelegramOutboxProvider(config, nil); err == nil {
		t.Fatal("Telegram outbox provider accepted a missing adapter")
	}
	if _, _, _, _, err := environmentOutboxProvider(environmentConfig{}, runtimeStore, nil, nil, nil); err == nil {
		t.Fatal("empty outbox provider selection unexpectedly succeeded")
	}
	if _, channel, providerName, leaseDuration, err := environmentOutboxProvider(environmentConfig{}, runtimeStore, legacy, nil, nil); err != nil || channel != "wecom" || providerName != "wecom" || leaseDuration <= 0 {
		t.Fatalf("legacy outbox selection = %q/%q/%s/%v", channel, providerName, leaseDuration, err)
	}
}

func assertEnvironmentTelegramFactoryFailures(
	t *testing.T,
	config environmentConfig,
	channelsRepo channels.CandidateConsumer,
	tenantsRepo tenant.Repository,
	appsRepo agent.Repository,
	runtimeStore runtimestorage.RuntimeStore,
	bindingID string,
	factory func(gateway.DispatchService) (channels.PollingAdapter, error),
	received *telegram.Config,
) {
	t.Helper()
	if _, err := factory(nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Telegram factory accepted a nil dispatcher: %v", err)
	}
	if noStoreFactory, err := environmentTelegramComponents(context.Background(), config, channelsRepo, tenantsRepo, appsRepo, nil, nil); err != nil {
		t.Fatal(err)
	} else if noStoreAdapter, err := noStoreFactory(bootstrapNoopDispatcher{}); err != nil {
		t.Fatal(err)
	} else {
		if received.Attachments != nil {
			t.Fatal("Telegram factory passed an attachment store that was not configured")
		}
		_ = noStoreAdapter.Close()
	}
	badMode := config
	badMode.telegram = &environmentTelegramConfig{bindingID: bindingID, secretRef: config.telegram.secretRef, botToken: "bot-token", mode: "webhook"}
	badModeFactory, err := environmentTelegramComponents(context.Background(), badMode, channelsRepo, tenantsRepo, appsRepo, runtimeStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := badModeFactory(bootstrapNoopDispatcher{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Telegram factory accepted unsupported mode: %v", err)
	}
	newEnvironmentTelegramAdapter = func(context.Context, telegram.Config) (*telegram.Adapter, error) {
		return nil, errors.New("secret constructor failure")
	}
	if _, err := factory(bootstrapNoopDispatcher{}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("Telegram factory exposed adapter construction error: %v", err)
	}
	if _, err := environmentTelegramComponents(context.Background(), config, nil, tenantsRepo, appsRepo, runtimeStore, nil); err == nil {
		t.Fatal("Telegram components accepted missing channel dependencies")
	}
	if disabledFactory, err := environmentTelegramComponents(context.Background(), environmentConfig{}, channelsRepo, tenantsRepo, appsRepo, runtimeStore, nil); err != nil || disabledFactory != nil {
		t.Fatalf("disabled Telegram components factory-nil=%t, err=%v", disabledFactory == nil, err)
	}
}

type environmentTelegramBot struct{}

func (environmentTelegramBot) Start(context.Context) {}
func (environmentTelegramBot) GetMe(context.Context) (*models.User, error) {
	return &models.User{ID: 12345, IsBot: true}, nil
}
func (environmentTelegramBot) SendMessage(context.Context, *bot.SendMessageParams) (*models.Message, error) {
	return &models.Message{ID: 1}, nil
}
