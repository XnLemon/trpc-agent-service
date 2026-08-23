// Package telegram implements the tenant-scoped Telegram long-polling
// Channel Adapter.
package telegram

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	defaultPollTimeout = time.Minute
	minimumPollTimeout = 2 * time.Second
	maximumPollTimeout = 10 * time.Minute
	maximumTokenRunes  = 1024
	maximumReplyRunes  = 4096
	failureReply       = "Sorry, I couldn't process that message."
)

var (
	// ErrInvalid reports malformed adapter configuration or update input.
	ErrInvalid = errors.New("invalid telegram adapter input")
	// ErrNotReady reports that the adapter has no usable Bot client.
	ErrNotReady = errors.New("telegram adapter is not ready")
	// ErrClosed reports an adapter or its owned process-local state after close.
	ErrClosed = errors.New("telegram adapter is closed")
	// ErrAlreadyRunning reports a second concurrent Run call.
	ErrAlreadyRunning = errors.New("telegram adapter is already running")
	// ErrInitialization reports a redacted Bot construction or getMe failure.
	ErrInitialization = errors.New("telegram bot initialization failed")
	// ErrBotIdentityMismatch reports a Bot identity different from the trusted
	// Binding provider account.
	ErrBotIdentityMismatch = errors.New("telegram bot identity does not match binding")
	// ErrInvalidUpdate reports a malformed supported update shape.
	ErrInvalidUpdate = errors.New("invalid telegram update")
	// ErrUnsupportedUpdate reports an update outside the first text-only scope.
	ErrUnsupportedUpdate = errors.New("unsupported telegram update")
	// ErrDuplicateUpdate reports an update already being handled by this process.
	ErrDuplicateUpdate = errors.New("duplicate telegram update")
	// ErrDispatch reports a redacted Gateway dispatch failure.
	ErrDispatch = errors.New("telegram dispatch failed")
	// ErrSendMessage reports a redacted Telegram sendMessage failure.
	ErrSendMessage = errors.New("telegram send message failed")
	// ErrPolling reports a redacted SDK polling error delivered to ErrorHook.
	ErrPolling = errors.New("telegram polling failed")
)

// ErrorOperation identifies the safe operation category supplied to ErrorHook.
type ErrorOperation string

const (
	// ErrorOperationInitialization identifies construction or getMe failures.
	ErrorOperationInitialization ErrorOperation = "initialization"
	// ErrorOperationPolling identifies long-polling failures.
	ErrorOperationPolling ErrorOperation = "polling"
	// ErrorOperationUpdate identifies rejected or unsupported updates.
	ErrorOperationUpdate ErrorOperation = "update"
	// ErrorOperationDispatch identifies Gateway execution failures.
	ErrorOperationDispatch ErrorOperation = "dispatch"
	// ErrorOperationSend identifies outbound sendMessage failures.
	ErrorOperationSend ErrorOperation = "send"
)

// ErrorEvent is the redacted payload passed to an ErrorHook. Err is always a
// stable adapter sentinel and never a provider error, token, or stack trace.
type ErrorEvent struct {
	Operation ErrorOperation
	Err       error
}

// ErrorHook observes stable adapter failures without receiving provider
// details or runtime secrets.
type ErrorHook func(ErrorEvent)

// BotClient is the small SDK surface required by the adapter. A fake client
// can implement it without credentials or network access.
type BotClient interface {
	Start(context.Context)
	GetMe(context.Context) (*models.User, error)
	SendMessage(context.Context, *bot.SendMessageParams) (*models.Message, error)
}

// BotFactoryConfig contains non-secret options for constructing one BotClient.
// The token is passed separately to BotFactory.New and is never stored in this
// configuration value.
type BotFactoryConfig struct {
	// Handler receives updates from the SDK's long-polling consumer.
	Handler bot.HandlerFunc
	// APIBaseURL overrides the Telegram API origin when non-empty.
	APIBaseURL string
	// HTTPClient is the optional HTTP transport used by the SDK.
	HTTPClient bot.HttpClient
	// PollTimeout is the Telegram getUpdates long-poll timeout.
	PollTimeout time.Duration
	// Workers is the explicitly validated SDK update worker count.
	Workers int
	// OnPollingError receives no raw error; it only signals that polling failed.
	OnPollingError func()
}

// BotFactory constructs a BotClient with the supplied runtime token and safe
// options. Production uses the public github.com/go-telegram/bot SDK; tests
// inject a fake implementation.
type BotFactory interface {
	New(string, BotFactoryConfig) (BotClient, error)
}

// BotFactoryFunc adapts a function into a BotFactory.
type BotFactoryFunc func(string, BotFactoryConfig) (BotClient, error)

// New implements BotFactory for a BotFactoryFunc.
func (factory BotFactoryFunc) New(token string, config BotFactoryConfig) (BotClient, error) {
	if factory == nil {
		return nil, ErrInvalid
	}
	return factory(token, config)
}

// Config defines one tenant-scoped Telegram Binding adapter. BotToken is a
// runtime-only secret and must not be persisted or placed in diagnostics.
type Config struct {
	// BotToken is resolved before construction and retained only by the SDK
	// client created for this adapter.
	BotToken string
	// Target is the trusted, non-secret route for exactly one active Binding.
	Target channels.RoutingTarget
	// Dispatcher is the existing protocol-neutral Gateway execution service.
	Dispatcher gateway.DispatchService
	// Idempotency optionally supplies a shared process-local store. When nil,
	// the adapter owns a new process-local store.
	Idempotency *gateway.IdempotencyStore
	// APIBaseURL optionally overrides the Telegram HTTPS API origin.
	APIBaseURL string
	// HTTPClient optionally supplies the SDK HTTP transport.
	HTTPClient bot.HttpClient
	// PollTimeout optionally overrides the long-poll timeout.
	PollTimeout time.Duration
	// Workers controls SDK update workers. Zero defaults to one.
	Workers int
	// ErrorHook observes stable, redacted adapter failures.
	ErrorHook ErrorHook
	// Factory optionally replaces the public SDK factory for tests.
	Factory BotFactory
}

// Adapter owns one trusted Telegram Binding and routes its updates through the
// existing Gateway contracts. It does not create or cache a Runner directly.
type Adapter struct {
	client         BotClient
	dispatcher     gateway.DispatchService
	principal      gateway.Principal
	target         channels.RoutingTarget
	idempotency    *gateway.IdempotencyStore
	ownIdempotency bool
	errorHook      ErrorHook

	mu        sync.RWMutex
	closed    bool
	runCancel context.CancelFunc
}

// New validates the trusted route, constructs the Bot client, and verifies its
// getMe identity before returning an adapter that can handle updates.
func New(ctx context.Context, config Config) (*Adapter, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	token, err := normalizeToken(config.BotToken)
	if err != nil {
		return nil, err
	}
	if err := config.Target.Validate(); err != nil {
		return nil, fmt.Errorf("%w: trusted routing target is invalid", ErrInvalid)
	}
	if config.Target.Channel != channels.ChannelTelegram {
		return nil, fmt.Errorf("%w: routing target is not Telegram", ErrInvalid)
	}
	providerAccountID, err := strconv.ParseInt(config.Target.ProviderAccountID, 10, 64)
	if err != nil || providerAccountID <= 0 || strconv.FormatInt(providerAccountID, 10) != config.Target.ProviderAccountID {
		return nil, fmt.Errorf("%w: Telegram provider account ID is not canonical", ErrInvalid)
	}
	if config.Dispatcher == nil {
		return nil, fmt.Errorf("%w: dispatcher is required", ErrInvalid)
	}
	apiBaseURL, err := normalizeAPIBaseURL(config.APIBaseURL)
	if err != nil {
		return nil, err
	}
	pollTimeout, err := normalizePollTimeout(config.PollTimeout)
	if err != nil {
		return nil, err
	}
	workers, err := normalizeWorkers(config.Workers)
	if err != nil {
		return nil, err
	}
	principal, err := gateway.NewChannelPrincipal(config.Target)
	if err != nil {
		return nil, fmt.Errorf("%w: trusted principal is invalid", ErrInvalid)
	}

	idempotency := config.Idempotency
	ownIdempotency := false
	if idempotency == nil {
		idempotency, err = gateway.NewIdempotencyStore(gateway.IdempotencyConfig{})
		if err != nil {
			return nil, fmt.Errorf("%w: idempotency store is unavailable", ErrInvalid)
		}
		ownIdempotency = true
	}
	factory := config.Factory
	if factory == nil {
		factory = sdkBotFactory{}
	}
	adapter := &Adapter{
		dispatcher: config.Dispatcher, principal: principal, target: config.Target,
		idempotency: idempotency, ownIdempotency: ownIdempotency, errorHook: config.ErrorHook,
	}
	client, err := factory.New(token, BotFactoryConfig{
		Handler:        adapter.sdkHandler(),
		APIBaseURL:     apiBaseURL,
		HTTPClient:     config.HTTPClient,
		PollTimeout:    pollTimeout,
		Workers:        workers,
		OnPollingError: func() { adapter.report(ErrorOperationPolling, ErrPolling) },
	})
	if err != nil || client == nil {
		adapter.report(ErrorOperationInitialization, ErrInitialization)
		_ = adapter.closeOwnedIdempotency()
		return nil, ErrInitialization
	}
	adapter.client = client
	me, err := client.GetMe(ctx)
	if err != nil {
		adapter.report(ErrorOperationInitialization, ErrInitialization)
		_ = adapter.closeOwnedIdempotency()
		return nil, ErrInitialization
	}
	if me == nil || !me.IsBot || me.ID <= 0 || strconv.FormatInt(me.ID, 10) != config.Target.ProviderAccountID {
		adapter.report(ErrorOperationInitialization, ErrBotIdentityMismatch)
		_ = adapter.closeOwnedIdempotency()
		return nil, ErrBotIdentityMismatch
	}
	return adapter, nil
}

// Run starts blocking Telegram long polling and returns after ctx is canceled
// or Close cancels the run. The SDK owns its polling and worker goroutines.
func (adapter *Adapter) Run(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if adapter == nil {
		return ErrNotReady
	}
	adapter.mu.Lock()
	closed, client := adapter.closed, adapter.client
	if closed {
		adapter.mu.Unlock()
		return ErrClosed
	}
	if client == nil {
		adapter.mu.Unlock()
		return ErrNotReady
	}
	if adapter.runCancel != nil {
		adapter.mu.Unlock()
		return ErrAlreadyRunning
	}
	runContext, cancel := context.WithCancel(ctx)
	adapter.runCancel = cancel
	adapter.mu.Unlock()
	defer func() {
		adapter.mu.Lock()
		adapter.runCancel = nil
		adapter.mu.Unlock()
		cancel()
	}()
	client.Start(runContext)
	return nil
}

// Close closes only idempotency state owned by the adapter. Injected stores and
// HTTP clients remain owned by their callers; polling is stopped by canceling
// the Context passed to Run.
func (adapter *Adapter) Close() error {
	if adapter == nil {
		return nil
	}
	adapter.mu.Lock()
	if adapter.closed {
		adapter.mu.Unlock()
		return nil
	}
	adapter.closed = true
	cancel := adapter.runCancel
	adapter.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return adapter.closeOwnedIdempotency()
}

// HandleUpdate validates and processes one Telegram update. It is exposed for
// deterministic tests and for the SDK's default handler.
func (adapter *Adapter) HandleUpdate(ctx context.Context, update *models.Update) error {
	if adapter == nil {
		return ErrNotReady
	}
	if ctx == nil {
		err := fmt.Errorf("%w: context is required", ErrInvalid)
		adapter.report(ErrorOperationUpdate, ErrInvalid)
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	adapter.mu.RLock()
	closed, client := adapter.closed, adapter.client
	adapter.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if client == nil || adapter.idempotency == nil {
		return ErrNotReady
	}
	message, err := normalizeUpdate(adapter.target, update)
	if err != nil {
		adapter.report(ErrorOperationUpdate, err)
		return err
	}
	claim, replay, err := adapter.idempotency.Begin(ctx, adapter.principal, message)
	if err != nil {
		if errors.Is(err, gateway.ErrDuplicateMessage) {
			return ErrDuplicateUpdate
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		if errors.Is(err, gateway.ErrClosed) {
			return ErrClosed
		}
		return ErrInvalid
	}
	if claim == nil {
		if err := adapter.sendEvents(ctx, update.Message, replay); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			adapter.report(ErrorOperationSend, ErrSendMessage)
			return err
		}
		return nil
	}

	events, dispatchErr := adapter.dispatch(ctx, message)
	if dispatchErr != nil {
		_ = claim.Fail()
		if errors.Is(dispatchErr, context.Canceled) || errors.Is(dispatchErr, context.DeadlineExceeded) {
			return dispatchErr
		}
		adapter.report(ErrorOperationDispatch, ErrDispatch)
		if sendErr := adapter.sendText(ctx, update.Message, failureReply); sendErr != nil {
			if !errors.Is(sendErr, context.Canceled) && !errors.Is(sendErr, context.DeadlineExceeded) {
				adapter.report(ErrorOperationSend, ErrSendMessage)
			}
		}
		return ErrDispatch
	}
	if err := claim.Complete(events); err != nil {
		return ErrDispatch
	}
	if err := adapter.sendEvents(ctx, update.Message, events); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		adapter.report(ErrorOperationSend, ErrSendMessage)
		return err
	}
	return nil
}

func (adapter *Adapter) sdkHandler() bot.HandlerFunc {
	return func(ctx context.Context, _ *bot.Bot, update *models.Update) {
		_ = adapter.HandleUpdate(ctx, update)
	}
}

func (adapter *Adapter) dispatch(ctx context.Context, message gateway.InboundMessage) ([]gateway.DispatchEvent, error) {
	stream, err := adapter.dispatcher.Dispatch(ctx, gateway.DispatchRequest{
		Principal: adapter.principal, Message: message, RequestID: message.ExternalMessageID,
	})
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, ErrDispatch
	}
	if stream == nil {
		return nil, ErrDispatch
	}
	events := make([]gateway.DispatchEvent, 0, 4)
	done := false
	failed := false
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case event, ok := <-stream:
			if !ok {
				if !done || failed {
					return nil, ErrDispatch
				}
				return events, nil
			}
			events = append(events, event)
			if event.Type == gateway.DispatchEventError {
				failed = true
			}
			if event.Done {
				done = true
			}
		}
	}
}

func (adapter *Adapter) sendEvents(ctx context.Context, message *models.Message, events []gateway.DispatchEvent) error {
	for _, event := range events {
		if event.Type == gateway.DispatchEventError {
			return adapter.sendText(ctx, message, failureReply)
		}
	}
	var builder strings.Builder
	for _, event := range events {
		if event.Type == gateway.DispatchEventMessage {
			builder.WriteString(event.Text)
		}
	}
	if builder.Len() == 0 {
		return nil
	}
	return adapter.sendText(ctx, message, builder.String())
}

func (adapter *Adapter) sendText(ctx context.Context, message *models.Message, text string) error {
	if message == nil {
		return ErrInvalidUpdate
	}
	if adapter == nil || adapter.client == nil {
		return ErrNotReady
	}
	chunks := splitText(text, maximumReplyRunes)
	for _, chunk := range chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := adapter.client.SendMessage(ctx, &bot.SendMessageParams{
			ChatID: message.Chat.ID, MessageThreadID: message.MessageThreadID, Text: chunk,
		})
		if err != nil {
			return ErrSendMessage
		}
	}
	return nil
}

func normalizeUpdate(target channels.RoutingTarget, update *models.Update) (gateway.InboundMessage, error) {
	if update == nil || update.ID < 0 {
		return gateway.InboundMessage{}, ErrInvalidUpdate
	}
	if hasUnsupportedUpdate(update) || update.Message == nil {
		return gateway.InboundMessage{}, ErrUnsupportedUpdate
	}
	message := update.Message
	if hasUnsupportedMessage(message) || strings.TrimSpace(message.Text) == "" {
		return gateway.InboundMessage{}, ErrUnsupportedUpdate
	}
	if message.From == nil || message.From.ID <= 0 || message.Chat.ID == 0 {
		return gateway.InboundMessage{}, ErrInvalidUpdate
	}
	if message.MessageThreadID < 0 {
		return gateway.InboundMessage{}, ErrInvalidUpdate
	}
	inbound := gateway.InboundMessage{
		Content: message.Text, ContentType: gateway.ContentTypeText,
		ExternalMessageID: externalMessageID(target, update.ID),
		ExternalUserID:    strconv.FormatInt(message.From.ID, 10),
	}
	switch message.Chat.Type {
	case models.ChatTypePrivate:
		inbound.ConversationKind = channels.ConversationDirect
		inbound.ExternalPeerID = strconv.FormatInt(message.Chat.ID, 10)
	case models.ChatTypeGroup, models.ChatTypeSupergroup:
		inbound.ConversationKind = channels.ConversationGroup
		inbound.ExternalChatID = strconv.FormatInt(message.Chat.ID, 10)
	default:
		return gateway.InboundMessage{}, ErrUnsupportedUpdate
	}
	if message.MessageThreadID > 0 {
		inbound.ExternalThreadID = strconv.Itoa(message.MessageThreadID)
	}
	normalized, err := inbound.Normalize()
	if err != nil {
		return gateway.InboundMessage{}, ErrInvalidUpdate
	}
	return normalized, nil
}

func hasUnsupportedMessage(message *models.Message) bool {
	return message == nil || message.DirectMessagesTopic != nil || message.SenderChat != nil ||
		message.SenderBusinessBot != nil || message.ReceiverUser != nil || message.BusinessConnectionID != "" ||
		message.RichMessage != nil || message.Animation != nil || message.Audio != nil || message.Document != nil ||
		message.PaidMedia != nil || len(message.Photo) > 0 || message.Sticker != nil || message.Story != nil ||
		message.Video != nil || message.VideoNote != nil || message.Voice != nil || message.Caption != "" ||
		len(message.CaptionEntities) > 0 || message.HasMediaSpoiler || message.Checklist != nil ||
		message.MediaGroupID != "" || message.ReplyToStore != nil || message.SuggestedPostInfo != nil ||
		message.EffectID != "" || message.EditDate != 0 ||
		message.Contact != nil || message.Dice != nil || message.Game != nil || message.Poll != nil ||
		message.Venue != nil || message.Location != nil || len(message.NewChatMembers) > 0 ||
		message.LeftChatMember != nil || message.NewChatTitle != "" || len(message.NewChatPhoto) > 0 ||
		message.DeleteChatPhoto || message.GroupChatCreated || message.SupergroupChatCreated ||
		message.ChannelChatCreated || message.MessageAutoDeleteTimerChanged != nil || message.MigrateToChatID != 0 ||
		message.MigrateFromChatID != 0 || message.PinnedMessage != nil || message.Invoice != nil ||
		message.SuccessfulPayment != nil || message.RefundedPayment != nil || message.UsersShared != nil ||
		message.ChatShared != nil || message.Gift != nil || message.UniqueGift != nil ||
		message.GiftUpgradeSent != nil || message.ConnectedWebsite != "" || message.WriteAccessAllowed != nil ||
		message.PassportData != nil || message.ProximityAlertTriggered != nil || message.BoostAdded != nil ||
		message.ChatBackgroundSet != nil || message.ChecklistTasksDone != nil || message.ChecklistTasksAdded != nil ||
		message.DirectMessagePriceChanged != nil || message.ForumTopicCreated != nil || message.ForumTopicEdited != nil ||
		message.ForumTopicClosed != nil || message.ForumTopicReopened != nil || message.GeneralForumTopicHidden != nil ||
		message.GeneralForumTopicUnhidden != nil || message.GiveawayCreated != nil || message.Giveaway != nil ||
		message.GiveawayWinners != nil || message.GiveawayCompleted != nil || message.PaidMessagePriceChanged != nil ||
		message.ChatOwnerLeft != nil || message.ChatOwnerChanged != nil || message.CommunityChatAdded != nil ||
		message.CommunityChatRemoved != nil || message.SuggestedPostApproved != nil ||
		message.SuggestedPostApprovalFailed != nil || message.SuggestedPostDeclined != nil ||
		message.SuggestedPostPaid != nil || message.SuggestedPostRefunded != nil || message.VideoChatScheduled != nil ||
		message.VideoChatStarted != nil || message.VideoChatEnded != nil || message.VideoChatParticipantsInvited != nil ||
		message.WebAppData != nil || message.ManagedBotCreated != nil || message.PollOptionAdded != nil ||
		message.PollOptionDeleted != nil || message.GuestBotCallerUser != nil || message.GuestBotCallerChat != nil ||
		message.GuestQueryID != "" || message.ReplyToPollOptionID != "" || message.LivePhoto != nil
}

func hasUnsupportedUpdate(update *models.Update) bool {
	return update.EditedMessage != nil || update.ChannelPost != nil || update.EditedChannelPost != nil ||
		update.BusinessConnection != nil || update.BusinessMessage != nil || update.EditedBusinessMessage != nil ||
		update.DeletedBusinessMessages != nil || update.MessageReaction != nil || update.MessageReactionCount != nil ||
		update.InlineQuery != nil || update.ChosenInlineResult != nil || update.CallbackQuery != nil ||
		update.ShippingQuery != nil || update.PreCheckoutQuery != nil || update.PurchasedPaidMedia != nil ||
		update.Poll != nil || update.PollAnswer != nil || update.ManagedBot != nil || update.GuestMessage != nil ||
		update.MyChatMember != nil || update.ChatMember != nil || update.ChatJoinRequest != nil ||
		update.ChatBoost != nil || update.RemovedChatBoost != nil || update.Subscription != nil
}

func externalMessageID(target channels.RoutingTarget, updateID int64) string {
	return encodeParts("telegram-update", target.BindingID, strconv.FormatInt(updateID, 10))
}

func encodeParts(parts ...string) string {
	var builder strings.Builder
	for _, part := range parts {
		builder.WriteString(strconv.Itoa(len([]byte(part))))
		builder.WriteByte(':')
		builder.WriteString(part)
	}
	return builder.String()
}

func splitText(text string, maximum int) []string {
	if text == "" {
		return nil
	}
	runes := []rune(text)
	if len(runes) <= maximum {
		return []string{text}
	}
	chunks := make([]string, 0, (len(runes)+maximum-1)/maximum)
	for len(runes) > 0 {
		end := maximum
		if len(runes) < end {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[:end]))
		runes = runes[end:]
	}
	return chunks
}

func normalizeToken(token string) (string, error) {
	if token == "" || strings.TrimSpace(token) != token || len([]rune(token)) > maximumTokenRunes || hasControl(token) {
		return "", fmt.Errorf("%w: bot token is invalid", ErrInvalid)
	}
	return token, nil
}

func normalizeAPIBaseURL(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || (parsed.Path != "" && parsed.Path != "/") || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.Fragment != "" || hasControl(value) {
		return "", fmt.Errorf("%w: API base URL must be an HTTPS origin", ErrInvalid)
	}
	return strings.TrimRight(value, "/"), nil
}

func normalizePollTimeout(value time.Duration) (time.Duration, error) {
	if value == 0 {
		return defaultPollTimeout, nil
	}
	if value < minimumPollTimeout || value > maximumPollTimeout {
		return 0, fmt.Errorf("%w: polling timeout is outside supported bounds", ErrInvalid)
	}
	return value, nil
}

func normalizeWorkers(value int) (int, error) {
	if value == 0 {
		return 1, nil
	}
	if value < 1 {
		return 0, fmt.Errorf("%w: worker count must be positive", ErrInvalid)
	}
	return value, nil
}

func hasControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}

func (adapter *Adapter) report(operation ErrorOperation, err error) {
	if adapter == nil || adapter.errorHook == nil || err == nil {
		return
	}
	adapter.errorHook(ErrorEvent{Operation: operation, Err: err})
}

func (adapter *Adapter) closeOwnedIdempotency() error {
	if adapter == nil || !adapter.ownIdempotency || adapter.idempotency == nil {
		return nil
	}
	return adapter.idempotency.Close()
}

type sdkBotFactory struct{}

func (sdkBotFactory) New(token string, config BotFactoryConfig) (BotClient, error) {
	if config.Handler == nil || config.Workers < 1 || config.PollTimeout < minimumPollTimeout {
		return nil, ErrInvalid
	}
	options := []bot.Option{
		bot.WithSkipGetMe(), bot.WithDefaultHandler(config.Handler), bot.WithNotAsyncHandlers(), bot.WithWorkers(config.Workers),
		bot.WithHTTPClient(config.PollTimeout, configuredHTTPClient(config.HTTPClient, config.PollTimeout)),
		bot.WithErrorsHandler(func(error) {
			if config.OnPollingError != nil {
				config.OnPollingError()
			}
		}),
	}
	if config.APIBaseURL != "" {
		options = append(options, bot.WithServerURL(config.APIBaseURL))
	}
	return bot.New(token, options...)
}

func configuredHTTPClient(client bot.HttpClient, pollTimeout time.Duration) bot.HttpClient {
	if client != nil {
		return client
	}
	return &http.Client{Timeout: pollTimeout + 5*time.Second}
}
