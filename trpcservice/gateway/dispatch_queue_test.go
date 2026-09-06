package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	channelsinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/channels/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimequeue "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

//nolint:gocyclo // This is a vertical contract test for admission, queue, worker, lifecycle, and outbox state.
func TestDispatcherEnqueueRunsThroughIndependentWorkerAndMaterializesReply(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, channelRepo := newQueueChannelPrincipal(t, fixture)
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	queue := runtimequeue.NewMemory()
	t.Cleanup(func() { _ = queue.Close() })
	queueSecret := strings.Join([]string{"offline", "queue", "secret"}, "-")
	var runs atomic.Int32
	runnerValue := &testRunner{runFn: func(_ context.Context, _, _ string, _ trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
		runs.Add(1)
		events := make(chan *trpcevent.Event, 2)
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "worker reply"}}}}}
		events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
		close(events)
		return events, nil
	}}
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return runnerValue, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	dispatcher, err := NewDispatcher(DispatchConfig{
		Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store,
		ExecutionQueue: queue, Channels: channelRepo, Tenants: fixture.tenants, Apps: fixture.apps,
	})
	if err != nil {
		t.Fatal(err)
	}

	accepted := make(chan struct{}, 1)
	request := DispatchRequest{
		Principal: principal,
		Message: InboundMessage{
			Content: "hello", ExternalMessageID: "telegram-update-1", ExternalUserID: "user-1",
			ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1",
		},
		RequestID: "queue-request-1", Accepted: accepted,
	}
	result, err := dispatcher.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if result.TaskID == "" {
		t.Fatal("enqueue returned an empty task ID")
	}
	select {
	case <-accepted:
	default:
		t.Fatal("enqueue did not signal acceptance")
	}
	if got := runs.Load(); got != 0 {
		t.Fatalf("runner calls before worker = %d, want 0", got)
	}
	task, err := queue.Get(context.Background(), fixture.tenant.TenantID, result.TaskID)
	if err != nil || task.Status != runtimequeue.StatusQueued {
		t.Fatalf("queued task = %+v, err=%v", task, err)
	}
	if strings.Contains(string(task.Payload), queueSecret) {
		t.Fatal("queue payload contains a channel secret")
	}

	worker, err := runtimequeue.New(runtimequeue.Config{
		Store: queue, Handler: dispatcher.HandleExecutionTask, TenantID: fixture.tenant.TenantID,
		Owner: "test-execution-worker", LeaseDuration: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(context.Background()); !processed || err != nil {
		t.Fatalf("worker run = processed:%v err:%v", processed, err)
	}
	completed, err := queue.Get(context.Background(), fixture.tenant.TenantID, result.TaskID)
	if err != nil || completed.Status != runtimequeue.StatusCompleted {
		t.Fatalf("completed task = %+v, err=%v", completed, err)
	}
	if got := runs.Load(); got != 1 {
		t.Fatalf("runner calls after worker = %d, want 1", got)
	}
	event, err := store.GetMessage(context.Background(), fixture.tenant.TenantID, result.TaskID)
	if err != nil || event.Status != runtimestorage.EventCompleted || event.SegmentCount != 1 {
		t.Fatalf("message event = %+v, err=%v", event, err)
	}
	replies, err := store.ListReplyCandidates(context.Background(), fixture.tenant.TenantID)
	if err != nil || len(replies) != 1 || replies[0].Payload != "worker reply" {
		t.Fatalf("reply outbox = %+v, err=%v", replies, err)
	}

	if _, err := dispatcher.Enqueue(context.Background(), request); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate enqueue error = %v", err)
	}
}

func TestAsyncDispatchReadinessPreservesSynchronousGraphs(t *testing.T) {
	if AsyncDispatchReady(nil) {
		t.Fatal("nil async service was reported ready")
	}
	if AsyncDispatchReady(&Dispatcher{}) {
		t.Fatal("dispatcher without a queue was reported async-ready")
	}
	queue := runtimequeue.NewMemory()
	defer func() { _ = queue.Close() }()
	if !AsyncDispatchReady(&Dispatcher{executionQueue: queue}) {
		t.Fatal("dispatcher with a queue was not reported async-ready")
	}
}

func TestDecodeExecutionTaskRejectsMissingIdentity(t *testing.T) {
	base := executionTaskPayload{
		Version: executionTaskPayloadVersion, TenantID: "tenant", AppID: "app", BindingID: "binding",
		BindingVersion: 1, ConfigDigest: "digest", RequestID: "request", PrincipalKind: PrincipalChannel,
		EventID: "event", Message: InboundMessage{ExternalMessageID: "external", Content: "hello", ContentType: ContentTypeText},
	}
	for name, payload := range map[string]executionTaskPayload{
		"missing tenant": func() executionTaskPayload { value := base; value.TenantID = ""; return value }(),
		"missing event":  func() executionTaskPayload { value := base; value.EventID = ""; return value }(),
	} {
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeExecutionTask(runtimequeue.Task{TenantID: payload.TenantID, TaskID: payload.EventID, Kind: ExecutionTaskKind, Payload: encoded}); !errors.Is(err, ErrInvalidExecutionTask) {
				t.Fatalf("decode error=%v", err)
			}
		})
	}
}

func TestEnqueueExecutionTaskRejectsConflictingPayload(t *testing.T) {
	queue := runtimequeue.NewMemory()
	defer func() { _ = queue.Close() }()
	dispatcher := &Dispatcher{executionQueue: queue}
	event := runtimestorage.MessageEvent{TenantID: "tenant", EventID: "event"}
	first := []byte(`{"version":1,"request_id":"first"}`)
	if _, _, err := queue.Enqueue(context.Background(), runtimequeue.TaskInput{TenantID: event.TenantID, TaskID: event.EventID, Kind: ExecutionTaskKind, Payload: first}); err != nil {
		t.Fatal(err)
	}
	if _, err := dispatcher.enqueueExecutionTask(context.Background(), event, []byte(`{"version":1,"request_id":"second"}`)); !errors.Is(err, ErrExecution) {
		t.Fatalf("conflicting payload error=%v", err)
	}
}

func TestDispatcherWorkerRevalidatesCurrentChannelRoute(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal, channelRepo := newQueueChannelPrincipal(t, fixture)
	resolver, err := NewPlanResolver(resolverTestConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	queue := runtimequeue.NewMemory()
	t.Cleanup(func() { _ = queue.Close() })
	var runs atomic.Int32
	registry, err := runtimerunner.NewRunnerRegistry(runtimerunner.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (runtimerunner.Runner, error) {
		return &testRunner{runFn: func(context.Context, string, string, trpcmodel.Message, ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
			runs.Add(1)
			events := make(chan *trpcevent.Event, 1)
			events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
			close(events)
			return events, nil
		}}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	dispatcher, err := NewDispatcher(DispatchConfig{
		Resolver: resolver, Registry: registry, SessionStore: store, MessageStore: store, ReplyBatchStore: store,
		ExecutionQueue: queue, Channels: channelRepo, Tenants: fixture.tenants, Apps: fixture.apps,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := DispatchRequest{Principal: principal, RequestID: "route-request-1", Message: InboundMessage{
		Content: "hello", ExternalMessageID: "route-update-1", ExternalUserID: "user-1",
		ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer-1",
	}}
	result, err := dispatcher.Enqueue(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channelRepo.Get(context.Background(), fixture.tenant.TenantID, principalMustTarget(t, principal).BindingID)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := channelRepo.Disable(context.Background(), channels.TransitionStatusInput{
		TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "queue", Reason: "route disabled", CorrelationID: "queue-route"},
	}); err != nil {
		t.Fatal(err)
	}
	worker, err := runtimequeue.New(runtimequeue.Config{Store: queue, Handler: dispatcher.HandleExecutionTask, TenantID: fixture.tenant.TenantID, Owner: "route-worker", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(context.Background()); !processed || err != nil {
		t.Fatalf("stale route worker run = processed:%v err:%v", processed, err)
	}
	task, err := queue.Get(context.Background(), fixture.tenant.TenantID, result.TaskID)
	if err != nil || task.Status != runtimequeue.StatusFailed {
		t.Fatalf("stale route task = %+v, err=%v", task, err)
	}
	event, err := store.GetMessage(context.Background(), fixture.tenant.TenantID, result.TaskID)
	if err != nil || event.Status != runtimestorage.EventFailed {
		t.Fatalf("stale route message event = %+v, err=%v", event, err)
	}
	if runs.Load() != 0 {
		t.Fatalf("runner calls for stale route = %d, want 0", runs.Load())
	}
}

func newQueueChannelPrincipal(t *testing.T, fixture gatewayFixture) (Principal, *channelsinmemory.InMemoryRepository) {
	t.Helper()
	routeDigest, err := channels.DigestPublicRouteKey(channels.ChannelTelegram, "queue-test-route")
	if err != nil {
		t.Fatal(err)
	}
	repo := channelsinmemory.NewInMemoryRepository()
	binding, _, err := repo.Create(context.Background(), channels.CreateInput{
		TenantID: fixture.tenant.TenantID, BindingKey: "queue-test-binding", Channel: channels.ChannelTelegram,
		ProviderAccountID: "12345", PublicRouteKeyDigest: routeDigest, AppID: fixture.app.AppID,
		SecretRef: strings.Join([]string{"secret", "queue-test"}, "/"), Protocol: channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{}},
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "queue", Reason: "fixture", CorrelationID: "queue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	binding, _, err = repo.Activate(context.Background(), channels.TransitionStatusInput{
		TenantID: binding.TenantID, BindingID: binding.BindingID, ExpectedVersion: binding.Version,
		Metadata: channels.ChangeMetadata{ActorType: "test", ActorID: "queue", Reason: "fixture", CorrelationID: "queue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	secret := strings.Join([]string{"offline", "queue", "secret"}, "-")
	resolver := channelsinmemory.NewFakeCandidateResolver(repo, map[channels.SecretScope]string{{TenantID: binding.TenantID, SecretRef: binding.SecretRef}: secret})
	candidates, err := repo.LookupCandidates(context.Background(), channels.ChannelTelegram, routeDigest)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("queue route discovery failed: candidates=%d err=%v", len(candidates), err)
	}
	handle, err := resolver.ResolveCandidate(context.Background(), channels.CandidateSecretRequest{Candidate: candidates[0], Purpose: channels.PurposeWebhookVerification})
	if err != nil {
		t.Fatal(err)
	}
	verification := channels.VerificationRequest{
		Purpose: channels.PurposeWebhookVerification, Timestamp: time.Now().UTC(), Nonce: "queue-nonce",
		MessageDigest: strings.Repeat("a", 64), ReceiveID: "receive",
	}
	verification.Signature = channelsinmemory.SignFakeRequest(secret, verification)
	verified, err := resolver.Verify(context.Background(), handle, verification)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(fixture.tenant)
	if err != nil {
		t.Fatal(err)
	}
	target, err := channels.NewRoutingTarget(snapshot, binding, fixture.app, verified)
	if err != nil {
		t.Fatal(err)
	}
	principal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	return principal, repo
}

func principalMustTarget(t *testing.T, principal Principal) channels.RoutingTarget {
	t.Helper()
	target, ok := principal.RoutingTarget()
	if !ok {
		t.Fatal("principal does not contain a routing target")
	}
	return target
}
