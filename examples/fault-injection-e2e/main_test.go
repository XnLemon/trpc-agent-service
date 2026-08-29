package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	agentinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	backendinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	modelinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/model/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	tenantinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/tenant/inmemory"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

const injectedSecret = "fault-injection-provider-secret" // #nosec G101 -- deterministic fixture value, never a credential.

// TestFaultInjectionHTTPBoundaryE2E runs an authenticated request through the
// HTTP, Gateway, plan, Registry, and Runner boundaries. It proves request
// routing cannot be forged and provider details are redacted at the response
// boundary.
func TestFaultInjectionHTTPBoundaryE2E(t *testing.T) {
	fixture := newFixture(t)
	authenticator, err := gateway.NewStaticAPIAuthenticator(map[string]gateway.APIIdentity{
		"tenant-a-token": {TenantID: fixture.tenantA.TenantID, AppID: fixture.appA.AppID, SubjectID: "api-a"},
		"tenant-b-token": {TenantID: fixture.tenantB.TenantID, AppID: fixture.appB.AppID, SubjectID: "api-b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	limiter, err := gateway.NewTenantLimiter(gateway.TenantLimiterConfig{MaxConcurrent: 8, MaxRequests: 100, Window: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := gateway.NewIdempotencyStore(gateway.IdempotencyConfig{TTL: time.Minute, MaxEntries: 100})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := gateway.NewHTTPHandler(gateway.HTTPConfig{
		Dispatcher: fixture.dispatcher, Authenticator: authenticator, Limiter: limiter,
		Idempotency: idempotency, RequestTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(handler.BeginShutdown)

	forgedBody := fmt.Sprintf(`{"content":"hello","tenant_id":%q,"external_user_id":"user","conversation_kind":"direct","external_peer_id":"peer"}`, fixture.tenantB.TenantID)
	forged := postJSON(t, handler, "tenant-a-token", forgedBody)
	if forged.Code != http.StatusBadRequest || fixture.runner.Calls() != 0 {
		t.Fatalf("forged route response = %d %q runner_calls=%d", forged.Code, forged.Body.String(), fixture.runner.Calls())
	}

	fixture.runner.SetError(errors.New("provider token=" + injectedSecret + " endpoint=https://private.invalid"))
	failure := postJSON(t, handler, "tenant-a-token", `{"content":"hello","external_message_id":"fault-redaction","external_user_id":"user","conversation_kind":"direct","external_peer_id":"peer"}`)
	if failure.Code != http.StatusBadGateway || strings.Contains(failure.Body.String(), injectedSecret) || !strings.Contains(failure.Body.String(), `"error":"execution failed"`) {
		t.Fatalf("redacted provider response = %d %q", failure.Code, failure.Body.String())
	}
}

// TestFaultInjectionOutboxRetryAndConcurrencyE2E verifies bounded recovery,
// fencing, and exactly-once delivery when two workers race for one reply.
func TestFaultInjectionOutboxRetryAndConcurrencyE2E(t *testing.T) {
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	seedCompletedReply(t, store, "tenant-fault", "event-fault", "reply-fault")

	provider := &faultProvider{failures: []error{errors.New("provider token=" + injectedSecret)}}
	worker, err := outbox.New(outbox.Config{Store: store, Provider: provider, TenantID: "tenant-fault", Owner: "worker-a", LeaseDuration: time.Second, MaxAttempts: 3, BackoffBase: time.Nanosecond, BackoffMax: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := worker.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("retryable run = %d err=%v", processed, err)
	}
	retry, err := store.GetReply(context.Background(), "tenant-fault", "reply-fault", 0)
	if err != nil || retry.Status != runtimestorage.ReplyRetryable || retry.LastErrorClass != "provider_error" {
		t.Fatalf("retry state = %+v err=%v", retry, err)
	}
	if strings.Contains(retry.LastErrorClass, injectedSecret) {
		t.Fatal("provider secret leaked into durable error class")
	}
	time.Sleep(2 * time.Millisecond)
	if processed, err := worker.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("recovery run = %d err=%v", processed, err)
	}

	seedCompletedReply(t, store, "tenant-fault", "event-concurrent", "reply-concurrent")
	started := make(chan struct{})
	release := make(chan struct{})
	provider.blockOnce = true
	provider.started = started
	provider.release = release
	workerB, err := outbox.New(outbox.Config{Store: store, Provider: provider, TenantID: "tenant-fault", Owner: "worker-b", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	go func() { _, runErr := worker.RunOnce(context.Background()); results <- runErr }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not enter provider")
	}
	go func() { _, runErr := workerB.RunOnce(context.Background()); results <- runErr }()
	close(release)
	for index := 0; index < 2; index++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	concurrent, err := store.GetReply(context.Background(), "tenant-fault", "reply-concurrent", 0)
	if err != nil || concurrent.Status != runtimestorage.ReplySent || provider.CallsFor("reply-concurrent") != 1 {
		t.Fatalf("concurrent delivery = %+v provider_calls=%d err=%v", concurrent, provider.CallsFor("reply-concurrent"), err)
	}
}

// TestFaultInjectionMaterializationFailureE2E asserts storage failures are
// returned in a stable wrapper and do not expose provider details or partial
// reply rows.
func TestFaultInjectionMaterializationFailureE2E(t *testing.T) {
	base := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = base.Close() })
	store := &failingBatchStore{RuntimeStore: base, err: errors.New("database password=" + injectedSecret)}
	materializer, err := outbox.NewMaterializer(outbox.MaterializerConfig{Store: store, SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	_, err = materializer.Materialize(context.Background(), outbox.MaterializeInput{TenantID: "tenant-fault", EventID: "event-storage", ReplyID: "reply-storage", Payload: "abcdef"})
	if !errors.Is(err, outbox.ErrMaterialization) || strings.Contains(err.Error(), injectedSecret) {
		t.Fatalf("storage failure = %v", err)
	}
	if store.attempted != 2 {
		t.Fatalf("batch failure did not receive complete segment set: attempted=%d", store.attempted)
	}
	if rows, listErr := base.ListReplyCandidates(context.Background(), "tenant-fault"); listErr != nil || len(rows) != 0 {
		t.Fatalf("partial rows after storage failure = %+v err=%v", rows, listErr)
	}
}

// TestFaultInjectionMaterializationAtomicityE2E uses the real in-memory batch
// implementation with a conflicting later segment. The conflict is discovered
// after the first segment would otherwise be visible, so a passing test proves
// the storage boundary rolls the batch back atomically.
func TestFaultInjectionMaterializationAtomicityE2E(t *testing.T) {
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	ctx := context.Background()
	if _, err := store.CreateSession(ctx, "tenant-fault", "session-atomic", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-fault", EventID: "event-atomic", SessionID: "session-atomic", BindingID: "binding", ExternalMessageID: "external-atomic"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-fault", EventID: "event-atomic", ReplyID: "reply-atomic", SegmentIndex: 1, SegmentCount: 2, Payload: "old"}); err != nil {
		t.Fatal(err)
	}
	materializer, err := outbox.NewMaterializer(outbox.MaterializerConfig{Store: store, SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	_, err = materializer.Materialize(ctx, outbox.MaterializeInput{TenantID: "tenant-fault", EventID: "event-atomic", ReplyID: "reply-atomic", Payload: "abcdef"})
	if !errors.Is(err, outbox.ErrMaterialization) || !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("atomic conflict = %v", err)
	}
	rows, err := store.ListReplyCandidates(ctx, "tenant-fault")
	if err != nil || len(rows) != 1 || rows[0].SegmentIndex != 1 || rows[0].Payload != "old" {
		t.Fatalf("materialization exposed a partial batch: rows=%+v err=%v", rows, err)
	}
}

// TestFaultInjectionRegistryConstructionE2E exercises concurrent plan
// acquisition for two tenants and verifies each immutable cache key is built
// once, without cross-tenant runner reuse.
func TestFaultInjectionRegistryConstructionE2E(t *testing.T) {
	fixture := newFixture(t)
	planA, planB := fixture.planA, fixture.planB
	var err error
	keyA, err := planA.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	keyB, err := planB.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if keyA.TenantID == keyB.TenantID {
		t.Fatal("tenant plans share a cache key")
	}
	var builds atomic.Int32
	registry, err := gateway.NewRunnerRegistry(gateway.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (gateway.Runner, error) {
		builds.Add(1)
		return &faultRunner{}, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	const callers = 16
	var wait sync.WaitGroup
	errorsCh := make(chan error, callers*2)
	for index := 0; index < callers; index++ {
		wait.Add(2)
		go func() {
			defer wait.Done()
			lease, acquireErr := registry.Acquire(context.Background(), planA)
			if acquireErr == nil {
				acquireErr = lease.Release()
			}
			errorsCh <- acquireErr
		}()
		go func() {
			defer wait.Done()
			lease, acquireErr := registry.Acquire(context.Background(), planB)
			if acquireErr == nil {
				acquireErr = lease.Release()
			}
			errorsCh <- acquireErr
		}()
	}
	wait.Wait()
	close(errorsCh)
	for acquireErr := range errorsCh {
		if acquireErr != nil {
			t.Fatal(acquireErr)
		}
	}
	if builds.Load() != 2 {
		t.Fatalf("runner builds = %d, want one per tenant plan", builds.Load())
	}
}

type fixture struct {
	tenantA, tenantB *tenant.Tenant
	appA, appB       *agent.App
	resolver         *gateway.PlanResolver
	dispatcher       *gateway.Dispatcher
	runner           *faultRunner
	planA, planB     runtime.ExecutionPlan
	modelCatalog     *model.ProviderCatalog
	backendCatalog   *backend.ProviderCatalog
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	modelCatalog, err := model.NewProviderCatalog(model.ProviderSpec{Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: model.FieldForbidden, SecretRefPolicy: model.FieldForbidden})
	if err != nil {
		t.Fatal(err)
	}
	backendCatalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden})
	if err != nil {
		t.Fatal(err)
	}
	tenants := tenantinmemory.NewRepository()
	apps := agentinmemory.NewRepository()
	models := modelinmemory.NewRepository(modelCatalog)
	backends := backendinmemory.NewRepository(backendCatalog)
	tenantA, appA := createTenant(t, tenants, apps, models, backends, "a")
	tenantB, appB := createTenant(t, tenants, apps, models, backends, "b")
	resolver, err := gateway.NewPlanResolver(gateway.PlanResolverConfig{Tenants: tenants, Apps: apps, Models: models, Backends: backends, ModelCatalog: modelCatalog, BackendCatalog: backendCatalog})
	if err != nil {
		t.Fatal(err)
	}
	runner := &faultRunner{}
	registry, err := gateway.NewRunnerRegistry(gateway.RunnerRegistryConfig{Factory: func(context.Context, runtime.ExecutionPlan) (gateway.Runner, error) { return runner, nil }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registry.Close() })
	store := runtimestorageinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	dispatcher, err := gateway.NewDispatcher(gateway.DispatchConfig{Resolver: resolver, Registry: registry, RuntimeStore: store})
	if err != nil {
		t.Fatal(err)
	}
	planA := makePlan(t, tenantA, appA, apps, models, backends, modelCatalog, backendCatalog)
	planB := makePlan(t, tenantB, appB, apps, models, backends, modelCatalog, backendCatalog)
	return fixture{tenantA: tenantA, tenantB: tenantB, appA: appA, appB: appB, resolver: resolver, dispatcher: dispatcher, runner: runner, planA: planA, planB: planB, modelCatalog: modelCatalog, backendCatalog: backendCatalog}
}

func makePlan(t *testing.T, root *tenant.Tenant, app *agent.App, apps *agentinmemory.InMemoryRepository, models *modelinmemory.InMemoryRepository, backends *backendinmemory.InMemoryRepository, modelCatalog *model.ProviderCatalog, backendCatalog *backend.ProviderCatalog) runtime.ExecutionPlan {
	t.Helper()
	if app.CurrentRevision == nil || root.DefaultBackendProfileID == nil {
		t.Fatal("fixture did not publish default references")
	}
	ctx := context.Background()
	revision, err := apps.GetRevision(ctx, root.TenantID, app.AppID, *app.CurrentRevision)
	if err != nil {
		t.Fatal(err)
	}
	modelProfile, err := models.Get(ctx, root.TenantID, revision.ModelProfileID)
	if err != nil {
		t.Fatal(err)
	}
	backendProfile, err := backends.Get(ctx, root.TenantID, *root.DefaultBackendProfileID)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := runtime.NewExecutionPlan(snapshot, app, revision, modelProfile, modelCatalog, backendProfile, backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func createTenant(t *testing.T, tenants *tenantinmemory.InMemoryRepository, apps *agentinmemory.InMemoryRepository, models *modelinmemory.InMemoryRepository, backends *backendinmemory.InMemoryRepository, suffix string) (*tenant.Tenant, *agent.App) {
	t.Helper()
	ctx := context.Background()
	root, err := tenants.Create(ctx, tenant.CreateInput{TenantKey: "fault-tenant-" + suffix, DisplayName: "Fault Tenant " + suffix, AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingStrict, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	modelProfile, _, err := models.Create(ctx, model.CreateInput{TenantID: root.TenantID, ProfileKey: "deterministic", DisplayName: "Deterministic", Configuration: model.Configuration{Provider: "fake", Model: "deterministic"}, Metadata: model.ChangeMetadata{ActorType: "example", ActorID: "fault-e2e", Reason: "fixture", CorrelationID: "fault-e2e"}})
	if err != nil {
		t.Fatal(err)
	}
	backendProfile, _, err := backends.Create(ctx, backend.CreateInput{TenantID: root.TenantID, ProfileKey: "session", DisplayName: "Session", Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory"}}, Metadata: backend.ChangeMetadata{ActorType: "example", ActorID: "fault-e2e", Reason: "fixture", CorrelationID: "fault-e2e"}})
	if err != nil {
		t.Fatal(err)
	}
	app, err := apps.Create(ctx, agent.CreateInput{TenantID: root.TenantID, AppKey: "fault-app-" + suffix, DisplayName: "Fault App " + suffix, Description: "Deterministic fault-injection E2E"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := apps.CreateDraft(ctx, agent.CreateDraftInput{TenantID: root.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Configuration: agent.DraftConfiguration{Instruction: "Reply deterministically.", ModelProfileID: modelProfile.ProfileID, Runtime: agent.DefaultRuntimePolicy()}})
	if err != nil {
		t.Fatal(err)
	}
	published, _, _, err := apps.Publish(ctx, agent.PublishInput{TenantID: root.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true, Metadata: agent.ChangeMetadata{ActorType: "example", ActorID: "fault-e2e", Reason: "fixture", CorrelationID: "fault-e2e"}})
	if err != nil {
		t.Fatal(err)
	}
	appID, backendID := published.AppID, backendProfile.ProfileID
	updated, err := tenants.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{TenantID: root.TenantID, ExpectedVersion: root.Version, DisplayName: root.DisplayName, AuditRetentionDays: root.AuditRetentionDays, LogMaskingLevel: tenant.MaskingStrict, TraceSamplingRate: 1, DefaultAgentAppID: &appID, DefaultBackendProfileID: &backendID})
	if err != nil {
		t.Fatal(err)
	}
	return updated, published
}

type faultRunner struct {
	mu    sync.RWMutex
	err   error
	calls atomic.Int32
}

func (r *faultRunner) Run(ctx context.Context, _, _ string, _ trpcmodel.Message, _ ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.calls.Add(1)
	r.mu.RLock()
	err := r.err
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	events := make(chan *trpcevent.Event, 2)
	events <- &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Delta: trpcmodel.Message{Content: "fault-e2e-ok"}}}}}
	events <- &trpcevent.Event{Response: &trpcmodel.Response{Done: true}}
	close(events)
	return events, nil
}

func (*faultRunner) Close() error         { return nil }
func (r *faultRunner) SetError(err error) { r.mu.Lock(); r.err = err; r.mu.Unlock() }
func (r *faultRunner) Calls() int32       { return r.calls.Load() }

type faultProvider struct {
	mu        sync.Mutex
	failures  []error
	calls     map[string]int
	blockOnce bool
	started   chan struct{}
	release   chan struct{}
}

func (p *faultProvider) Deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	p.mu.Lock()
	if p.calls == nil {
		p.calls = make(map[string]int)
	}
	p.calls[value.ReplyID]++
	if p.blockOnce {
		p.blockOnce = false
		if p.started != nil {
			close(p.started)
		}
		release := p.release
		p.mu.Unlock()
		select {
		case <-release:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	} else {
		p.mu.Unlock()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.failures) > 0 {
		err := p.failures[0]
		p.failures = p.failures[1:]
		return "", err
	}
	return fmt.Sprintf("provider-%d", p.calls[value.ReplyID]), nil
}

func (*faultProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryUnknown, "", nil
}

func (p *faultProvider) CallsFor(replyID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls[replyID]
}

type failingBatchStore struct {
	runtimestorage.RuntimeStore
	err       error
	attempted int
}

func (s *failingBatchStore) EnqueueReplies(_ context.Context, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	s.attempted = len(values)
	return nil, s.err
}

func (s *failingBatchStore) EnqueueRepliesWithCorrelation(_ context.Context, _ runtimestorage.ReplyCorrelation, values []runtimestorage.ReplyOutbox) ([]runtimestorage.ReplyOutbox, error) {
	s.attempted = len(values)
	return nil, s.err
}

func seedCompletedReply(t *testing.T, store runtimestorage.RuntimeStore, tenantID, eventID, replyID string) {
	t.Helper()
	ctx := context.Background()
	if _, err := store.CreateSession(ctx, tenantID, "session-"+eventID, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: tenantID, EventID: eventID, SessionID: "session-" + eventID, BindingID: "binding", ExternalMessageID: "external-" + eventID}); err != nil {
		t.Fatal(err)
	}
	event, err := store.GetMessage(ctx, tenantID, eventID)
	if err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: tenantID, EventID: eventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "runner", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: tenantID, EventID: eventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "runner", FencingToken: running.FencingToken, ReplyID: replyID, SegmentCount: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: tenantID, ReplyID: replyID, EventID: event.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
}

func postJSON(t *testing.T, handler http.Handler, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}
