package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// loadTelegram parses the opt-in deployment path. Polling is deliberately the
// only environment mode: it matches the current OpenClaw-style integration
// and keeps transport lifecycle out of the HTTP server.
func (config *environmentConfig) loadTelegram() error {
	botToken := strings.TrimSpace(os.Getenv(envTelegramBotToken))
	bindingID := strings.TrimSpace(os.Getenv(envTelegramBindingID))
	secretRef := strings.TrimSpace(os.Getenv(envTelegramSecretRef))
	modeValue := strings.TrimSpace(os.Getenv(envTelegramMode))
	values := []string{botToken, bindingID, secretRef, modeValue}
	configured := 0
	for _, value := range values {
		if value != "" {
			configured++
		}
	}
	if configured == 0 {
		return nil
	}
	if config.demoMode {
		return fmt.Errorf("%w: telegram credentials cannot be enabled in demo mode", ErrInvalidConfig)
	}
	if configured != len(values) {
		return fmt.Errorf("%w: telegram deployment credentials must be configured together", ErrInvalidConfig)
	}
	if len(config.apiIdentities) != 1 {
		return fmt.Errorf("%w: telegram credentials require exactly one API identity", ErrInvalidConfig)
	}
	if strings.ContainsAny(botToken, "\r\n") || len([]rune(botToken)) > 1024 {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envTelegramBotToken)
	}
	if bindingID == "" || strings.ContainsAny(bindingID, "\r\n") {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envTelegramBindingID)
	}
	if err := (channels.SecretScope{TenantID: config.tenantID, SecretRef: secretRef}).Validate(); err != nil {
		return fmt.Errorf("%w: %s is invalid", ErrInvalidConfig, envTelegramSecretRef)
	}
	mode := strings.ToLower(modeValue)
	if mode != "polling" {
		return fmt.Errorf("%w: %s must be polling", ErrInvalidConfig, envTelegramMode)
	}
	config.telegram = &environmentTelegramConfig{bindingID: bindingID, secretRef: secretRef, botToken: botToken, mode: mode}
	return nil
}

// environmentTelegramComponents resolves the one trusted Binding before the
// adapter receives the runtime token. The returned factory is invoked only
// after the Dispatcher has been constructed.
func environmentTelegramComponents(
	ctx context.Context,
	config environmentConfig,
	channelsRepo channels.CandidateConsumer,
	tenantsRepo tenant.Repository,
	appsRepo agent.Repository,
	runtimeStore runtimestorage.RuntimeStore,
	auditWriter audit.Writer,
) (func(gateway.DispatchService) (channels.PollingAdapter, error), error) {
	if config.telegram == nil {
		return nil, nil
	}
	if ctx == nil || channelsRepo == nil || tenantsRepo == nil || appsRepo == nil {
		return nil, errors.New("telegram deployment dependencies are unavailable")
	}
	binding, err := channelsRepo.Get(ctx, config.tenantID, config.telegram.bindingID)
	if err != nil || binding == nil || !binding.CanAcceptInbound() || binding.Channel != channels.ChannelTelegram || binding.SecretRef != config.telegram.secretRef {
		return nil, errors.New("telegram deployment binding is unavailable")
	}
	target, err := channels.ResolveConfiguredRoutingTarget(ctx, channelsRepo, tenantsRepo, appsRepo, config.tenantID, config.telegram.bindingID)
	if err != nil || target.Channel != channels.ChannelTelegram {
		return nil, errors.New("telegram deployment binding is unavailable")
	}
	apiBaseURL := ""
	if binding.Protocol.Telegram != nil {
		apiBaseURL = binding.Protocol.Telegram.APIBaseURL
	}
	var attachments runtimestorage.AttachmentStore
	if store, ok := runtimeStore.(runtimestorage.AttachmentStore); ok {
		attachments = store
	}
	telegramConfig := *config.telegram
	factory := func(dispatcher gateway.DispatchService) (channels.PollingAdapter, error) {
		if dispatcher == nil || telegramConfig.mode != "polling" {
			return nil, ErrInvalidConfig
		}
		adapter, err := newEnvironmentTelegramAdapter(ctx, telegram.Config{
			BotToken:      telegramConfig.botToken,
			Target:        target,
			Dispatcher:    dispatcher,
			APIBaseURL:    apiBaseURL,
			AuditWriter:   auditWriter,
			Observability: config.telemetry,
			Attachments:   attachments,
		})
		if err != nil || adapter == nil {
			return nil, ErrInvalidConfig
		}
		return adapter, nil
	}
	return factory, nil
}
