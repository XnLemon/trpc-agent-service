package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestExecutionPlanFreezesAllTenantScopedInputs(t *testing.T) {
	fixture := runtimeFixture(t)
	plan, err := NewExecutionPlan(fixture.tenantSnapshot, fixture.app, fixture.revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if key.TenantID != fixture.root.TenantID || key.AppID != fixture.app.AppID || key.Revision != fixture.revision.Revision || key.ModelProfileID != fixture.modelProfile.ProfileID || key.BackendProfileID != fixture.backendProfile.ProfileID {
		t.Fatalf("unexpected plan cache key: %+v", key)
	}
	agentInput, err := plan.AgentFactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	modelInput, err := plan.ModelFactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	storageInput, err := plan.StorageFactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if agentInput.ModelProfileID != fixture.modelProfile.ProfileID || modelInput.ProfileID != fixture.modelProfile.ProfileID || len(storageInput.Bindings) != 1 {
		t.Fatalf("plan lost component references: agent=%+v model=%+v storage=%+v", agentInput, modelInput, storageInput)
	}

	fixture.app.DisplayName = "mutated app"
	fixture.revision.Instruction = "mutated instruction"
	fixture.modelProfile.Configuration.Options["mode"] = "fast"
	fixture.backendProfile.Bindings[0].Options["namespace"] = "mutated backend"
	if plan.Tenant().TenantID != fixture.root.TenantID || plan.AgentSnapshot().App().DisplayName == "mutated app" || plan.AgentSnapshot().Revision().Instruction == "mutated instruction" {
		t.Fatal("plan retained mutable source control-plane state")
	}
	frozenModel := plan.ModelSnapshot().Profile()
	if frozenModel.Configuration.Options["mode"] != "safe" {
		t.Fatal("plan retained mutable Model Profile options")
	}
	frozenBackend := plan.BackendSnapshot().Profile()
	if frozenBackend.Bindings[0].Options["namespace"] != "session" {
		t.Fatal("plan retained mutable Backend Profile options")
	}
	keyAgain, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if keyAgain != key {
		t.Fatalf("plan cache identity drifted after source mutation: before=%+v after=%+v", key, keyAgain)
	}
}

func TestExecutionPlanRejectsRevisionFromDifferentAppInSameTenant(t *testing.T) {
	fixture := runtimeFixture(t)
	otherApp, otherRevision := runtimeAgentFixture(t, fixture.root.TenantID, fixture.modelProfile.ProfileID, "other-app")
	if otherApp.TenantID != fixture.app.TenantID || otherRevision.AppID != otherApp.AppID {
		t.Fatal("test fixture did not create a same-tenant distinct App")
	}
	if _, err := NewExecutionPlan(fixture.tenantSnapshot, fixture.app, otherRevision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog); err == nil || (!errors.Is(err, agent.ErrInvalid) && !strings.Contains(err.Error(), "does not belong to App")) {
		t.Fatalf("different-App revision error = %v", err)
	}
}

func TestExecutionPlanContextAndInvalidBoundaries(t *testing.T) {
	fixture := runtimeFixture(t)
	plan, err := NewExecutionPlan(fixture.tenantSnapshot, fixture.app, fixture.revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	if zero := (ExecutionPlan{}).Tenant(); zero.TenantID != "" {
		t.Fatalf("zero plan tenant = %+v", zero)
	}
	if _, err := (ExecutionPlan{}).CacheKey(); err == nil {
		t.Fatal("zero plan cache key unexpectedly succeeded")
	}
	if _, err := (ExecutionPlan{}).AgentFactoryInput(); err == nil {
		t.Fatal("zero plan agent input unexpectedly succeeded")
	}
	if _, err := (ExecutionPlan{}).ModelFactoryInput(); err == nil {
		t.Fatal("zero plan model input unexpectedly succeeded")
	}
	if _, err := (ExecutionPlan{}).StorageFactoryInput(); err == nil {
		t.Fatal("zero plan storage input unexpectedly succeeded")
	}
	if _, ok := ExecutionPlanFromContext(context.Background()); ok {
		t.Fatal("empty context returned an execution plan")
	}
	zeroContext := WithExecutionPlan(context.Background(), ExecutionPlan{})
	if _, ok := ExecutionPlanFromContext(zeroContext); ok {
		t.Fatal("zero plan entered context")
	}
	planContext := WithExecutionPlan(context.Background(), plan)
	fromContext, ok := ExecutionPlanFromContext(planContext)
	if !ok || fromContext.Tenant().TenantID != fixture.root.TenantID {
		t.Fatalf("valid plan context = %+v, ok=%v", fromContext, ok)
	}
	if fromContext.tenant == plan.tenant {
		t.Fatal("ExecutionPlan context retained tenant pointer")
	}

	invalidTenantPlan := plan
	invalidTenantPlan.tenant = nil
	if err := invalidTenantPlan.validate(); err == nil {
		t.Fatal("nil tenant plan unexpectedly validated")
	}
	inactivePlan := plan
	inactiveTenant := plan.tenant.Clone()
	inactivePlan.tenant = &inactiveTenant
	inactivePlan.tenant.Status = tenant.StatusSuspended
	if err := inactivePlan.validate(); err == nil {
		t.Fatal("inactive tenant plan unexpectedly validated")
	}
	invalidAgentPlan := plan
	invalidAgentPlan.agent = agent.AgentExecutionSnapshot{}
	if err := invalidAgentPlan.validate(); err == nil {
		t.Fatal("invalid agent plan unexpectedly validated")
	}
	invalidModelPlan := plan
	invalidModelPlan.model = modelprofile.ModelExecutionSnapshot{}
	if err := invalidModelPlan.validate(); err == nil {
		t.Fatal("invalid model plan unexpectedly validated")
	}
	invalidBackendPlan := plan
	invalidBackendPlan.backend = backend.BackendExecutionSnapshot{}
	if err := invalidBackendPlan.validate(); err == nil {
		t.Fatal("invalid backend plan unexpectedly validated")
	}
	if _, err := NewExecutionPlan(tenant.ConfigurationSnapshot{}, fixture.app, fixture.revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog); err == nil {
		t.Fatal("invalid tenant snapshot unexpectedly built a plan")
	}
}

func TestNewRunnerRejectsInvalidInputsAndFactoryFailures(t *testing.T) {
	fixture := runtimeFixture(t)
	plan, err := NewExecutionPlan(fixture.tenantSnapshot, fixture.app, fixture.revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	sessions := inmemory.NewSessionService()
	defer func() {
		if err := sessions.Close(); err != nil {
			t.Errorf("sessions.Close() error = %v", err)
		}
	}()
	var nilContext context.Context
	if _, err := NewRunner(nilContext, plan, nil, &runtimeModelFactory{}, sessions); err == nil {
		t.Fatal("nil runner context unexpectedly succeeded")
	}
	if _, err := NewRunner(context.Background(), plan, nil, &runtimeModelFactory{}, nil); err == nil {
		t.Fatal("nil session service unexpectedly succeeded")
	}
	if _, err := NewRunner(context.Background(), ExecutionPlan{}, nil, &runtimeModelFactory{}, sessions); err == nil {
		t.Fatal("zero execution plan unexpectedly succeeded")
	}
	invalidStorage := plan
	invalidStorage.backend = backend.BackendExecutionSnapshot{}
	if _, err := NewRunner(context.Background(), invalidStorage, nil, &runtimeModelFactory{}, sessions); err == nil {
		t.Fatal("invalid storage plan unexpectedly succeeded")
	}
	if _, err := NewRunner(context.Background(), plan, nil, &runtimeModelFactory{err: errors.New("provider failure")}, sessions); err == nil || !strings.Contains(err.Error(), "build runner: model") {
		t.Fatalf("factory failure = %v", err)
	}
	if _, err := NewRunner(context.Background(), plan, nil, &runtimeModelFactory{returnNil: true}, sessions); err == nil || !strings.Contains(err.Error(), "build runner: model") {
		t.Fatalf("nil model failure = %v", err)
	}
}

func TestNewRunnerCarriesPublishedRuntimePolicy(t *testing.T) {
	fixture := runtimeFixture(t)
	policy := agent.DefaultRuntimePolicy()
	policy.EnableParallelTools = true
	policy.MaxParallelTools = 7
	policy.ExecutionTimeoutSeconds = 9
	app, revision := runtimeAgentFixtureWithPolicy(t, fixture.root.TenantID, fixture.modelProfile.ProfileID, "policy-app", policy)
	plan, err := NewExecutionPlan(fixture.tenantSnapshot, app, revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	agentInput, err := plan.AgentFactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	options := llmagent.Options{}
	for _, option := range llmAgentOptions(agentInput, runtimeFakeModel{}) {
		option(&options)
	}
	if !options.EnableParallelTools || options.ToolConcurrencyConfig.MaxConcurrency != policy.MaxParallelTools || options.MaxLLMCalls != policy.MaxLLMCalls || options.MaxToolIterations != policy.MaxToolCalls {
		t.Fatalf("LLMAgent runtime options = %+v", options)
	}
	sessions := inmemory.NewSessionService()
	defer func() {
		if err := sessions.Close(); err != nil {
			t.Errorf("sessions.Close() error = %v", err)
		}
	}()
	runner, err := NewRunner(context.Background(), plan, nil, &runtimeModelFactory{}, sessions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("runner.Close() error = %v", err)
		}
	}()
	policyRunner, ok := runner.(*policyRunner)
	if !ok {
		t.Fatalf("NewRunner returned %T, want policyRunner", runner)
	}
	runOptions := trpcagent.NewRunOptions(policyRunner.runOptions...)
	if runOptions.MaxRunDuration != time.Duration(policy.ExecutionTimeoutSeconds)*time.Second {
		t.Fatalf("MaxRunDuration = %v, want %v", runOptions.MaxRunDuration, time.Duration(policy.ExecutionTimeoutSeconds)*time.Second)
	}
}

func TestRunnerExecutesFakeModelAndPersistsTenantScopedSession(t *testing.T) {
	fixture := runtimeFixture(t)
	plan, err := NewExecutionPlan(fixture.tenantSnapshot, fixture.app, fixture.revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	sessions := inmemory.NewSessionService()
	factory := &runtimeModelFactory{response: "deterministic reply"}
	runner, err := NewRunner(context.Background(), plan, nil, factory, sessions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("runner.Close() error = %v", err)
		}
	}()
	defer func() {
		if err := sessions.Close(); err != nil {
			t.Errorf("sessions.Close() error = %v", err)
		}
	}()
	identity, err := tenant.NewRunnerIdentity(fixture.root.TenantID, "external-user", "external-session")
	if err != nil {
		t.Fatal(err)
	}
	events, err := runner.Run(context.Background(), identity.UserID, identity.SessionID, trpcmodel.NewUserMessage("hello"))
	if err != nil {
		t.Fatal(err)
	}
	assistantReply := ""
	eventCount := 0
	for evt := range events {
		eventCount++
		if evt != nil && evt.Response != nil {
			for _, choice := range evt.Choices {
				if choice.Message.Role == trpcmodel.RoleAssistant && choice.Message.Content != "" {
					assistantReply = choice.Message.Content
				}
			}
		}
	}
	if eventCount == 0 || assistantReply != "deterministic reply" {
		t.Fatalf("runner events=%d assistantReply=%q", eventCount, assistantReply)
	}
	inspector, err := NewTenantSessionService(*fixture.root, sessions)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := inspector.GetSession(context.Background(), session.Key{AppName: fixture.app.AppID, UserID: identity.UserID, SessionID: identity.SessionID})
	if err != nil {
		t.Fatal(err)
	}
	if stored == nil || len(stored.Events) < 2 {
		t.Fatalf("stored session missing user/assistant events: %+v", stored)
	}
	storedReply := ""
	for _, storedEvent := range stored.Events {
		if storedEvent.Response == nil {
			continue
		}
		for _, choice := range storedEvent.Choices {
			if choice.Message.Role == trpcmodel.RoleAssistant {
				storedReply = choice.Message.Content
			}
		}
	}
	if !strings.Contains(storedReply, "deterministic reply") {
		t.Fatalf("stored assistant events did not contain reply: %+v", stored.Events)
	}
	if factory.input.TenantID != fixture.root.TenantID || factory.secret.Value() != "" {
		t.Fatalf("factory crossed secret or tenant boundary: input=%+v secret=%q", factory.input, factory.secret.Value())
	}
}

func TestRunnerCancellationDrainsAndClosesEventChannel(t *testing.T) {
	fixture := runtimeFixture(t)
	plan, err := NewExecutionPlan(fixture.tenantSnapshot, fixture.app, fixture.revision, fixture.modelProfile, fixture.modelCatalog, fixture.backendProfile, fixture.backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	sessions := inmemory.NewSessionService()
	runner, err := NewRunner(context.Background(), plan, nil, &runtimeModelFactory{block: true}, sessions)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("runner.Close() error = %v", err)
		}
	}()
	defer func() {
		if err := sessions.Close(); err != nil {
			t.Errorf("sessions.Close() error = %v", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	identity, err := tenant.NewRunnerIdentity(fixture.root.TenantID, "cancel-user", "cancel-session")
	if err != nil {
		t.Fatal(err)
	}
	events, err := runner.Run(ctx, identity.UserID, identity.SessionID, trpcmodel.NewUserMessage("cancel"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		for range events {
		}
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled Runner event channel did not close")
	}
}

func TestTenantSessionServiceRejectsCrossTenantGetAndAppend(t *testing.T) {
	rootOne := runtimeTenant(t, "session-tenant-one")
	rootTwo := runtimeTenant(t, "session-tenant-two")
	delegate := inmemory.NewSessionService()
	defer func() {
		if err := delegate.Close(); err != nil {
			t.Errorf("delegate.Close() error = %v", err)
		}
	}()
	serviceOne, err := NewTenantSessionService(*rootOne, delegate)
	if err != nil {
		t.Fatal(err)
	}
	serviceTwo, err := NewTenantSessionService(*rootTwo, delegate)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "shared-app", UserID: "same-user", SessionID: "same-session"}
	stored, err := serviceOne.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := serviceTwo.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if other != nil {
		t.Fatal("tenant two read tenant one session")
	}
	if err := serviceTwo.AppendEvent(context.Background(), stored, &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.NewAssistantMessage("cross-tenant")}}, Done: true}}); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("cross-tenant AppendEvent error = %v", err)
	}
	if err := serviceOne.UpdateUserState(context.Background(), session.UserKey{AppName: "shared-app", UserID: "same-user"}, session.StateMap{"visible": []byte("one")}); err != nil {
		t.Fatal(err)
	}
	otherState, err := serviceTwo.ListUserStates(context.Background(), session.UserKey{AppName: "shared-app", UserID: "same-user"})
	if err != nil {
		t.Fatal(err)
	}
	if len(otherState) != 0 {
		t.Fatalf("tenant two observed tenant one user state: %+v", otherState)
	}
	if _, err := serviceTwo.CreateSession(context.Background(), key, nil); err != nil {
		t.Fatal(err)
	}
	one, err := serviceOne.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	two, err := serviceTwo.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if one == nil || two == nil || one.AppName == two.AppName || one.UserID == two.UserID {
		t.Fatalf("tenant namespaces are not distinct: one=%+v two=%+v", one, two)
	}
}

func TestTenantSessionServiceDelegatesEveryOperationAndScopesKeys(t *testing.T) {
	root := runtimeTenant(t, "all-session-operations")
	delegate := inmemory.NewSessionService()
	service, err := NewTenantSessionService(*root, delegate)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := service.Close(); err != nil {
			t.Errorf("service.Close() error = %v", err)
		}
	}()
	key := session.Key{AppName: "operations-app", UserID: "operations-user", SessionID: "operations-session"}
	created, err := service.CreateSession(context.Background(), key, session.StateMap{"initial": []byte("value")})
	if err != nil {
		t.Fatal(err)
	}
	if created == nil || !service.isScoped(created.AppName) || !service.isScoped(created.UserID) {
		t.Fatalf("created session was not tenant scoped: %+v", created)
	}
	if sessions, err := service.ListSessions(context.Background(), session.UserKey{AppName: key.AppName, UserID: key.UserID}); err != nil || len(sessions) != 1 {
		t.Fatalf("ListSessions = %d, %v", len(sessions), err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{AppName: created.AppName, UserID: created.UserID, SessionID: created.ID}); err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateAppState(context.Background(), key.AppName, session.StateMap{"app-key": []byte("app-value")}); err != nil {
		t.Fatal(err)
	}
	if state, err := service.ListAppStates(context.Background(), key.AppName); err != nil || string(state["app-key"]) != "app-value" {
		t.Fatalf("ListAppStates = %+v, %v", state, err)
	}
	if err := service.DeleteAppState(context.Background(), key.AppName, "app-key"); err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateUserState(context.Background(), session.UserKey{AppName: key.AppName, UserID: key.UserID}, session.StateMap{"user-key": []byte("user-value")}); err != nil {
		t.Fatal(err)
	}
	if state, err := service.ListUserStates(context.Background(), session.UserKey{AppName: key.AppName, UserID: key.UserID}); err != nil || string(state["user-key"]) != "user-value" {
		t.Fatalf("ListUserStates = %+v, %v", state, err)
	}
	if err := service.DeleteUserState(context.Background(), session.UserKey{AppName: key.AppName, UserID: key.UserID}, "user-key"); err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateSessionState(context.Background(), key, session.StateMap{"session-key": []byte("session-value")}); err != nil {
		t.Fatal(err)
	}
	if err := service.AppendEvent(context.Background(), created, &trpcevent.Event{Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.NewAssistantMessage("event")}}, Done: true}}); err != nil {
		t.Fatal(err)
	}
	if err := service.CreateSessionSummary(context.Background(), created, "", false); err != nil {
		t.Fatal(err)
	}
	if err := service.EnqueueSummaryJob(context.Background(), created, "", false); err != nil {
		t.Fatal(err)
	}
	if summary, ok := service.GetSessionSummaryText(context.Background(), created); ok || summary != "" {
		t.Fatalf("unexpected summary = %q, ok=%v", summary, ok)
	}
	if err := service.DeleteSession(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if deleted, err := service.GetSession(context.Background(), key); err != nil || deleted != nil {
		t.Fatalf("deleted session = %+v, err=%v", deleted, err)
	}

	if _, err := NewTenantSessionService(tenant.Tenant{}, delegate); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("invalid tenant constructor error = %v", err)
	}
	if _, err := NewTenantSessionService(*root, nil); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("nil delegate constructor error = %v", err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{UserID: "user", SessionID: "session"}); !errors.Is(err, session.ErrAppNameRequired) {
		t.Fatalf("invalid app key error = %v", err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{AppName: "app", UserID: "user"}); !errors.Is(err, session.ErrSessionIDRequired) {
		t.Fatalf("missing session ID error = %v", err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{AppName: service.prefix + "one", UserID: "user", SessionID: "session"}); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("partially scoped session key error = %v", err)
	}
	if _, err := service.ListSessions(context.Background(), session.UserKey{AppName: "tenant:foreign", UserID: "user"}); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("foreign scoped user key error = %v", err)
	}
	if err := service.UpdateAppState(context.Background(), "", nil); !errors.Is(err, session.ErrAppNameRequired) {
		t.Fatalf("empty app state key error = %v", err)
	}
	if err := service.UpdateAppState(context.Background(), "tenant:foreign", nil); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("foreign app state key error = %v", err)
	}
	if err := service.AppendEvent(context.Background(), nil, &trpcevent.Event{}); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("nil session append error = %v", err)
	}
	if err := service.CreateSessionSummary(context.Background(), nil, "", false); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("nil session summary error = %v", err)
	}
	if err := service.EnqueueSummaryJob(context.Background(), nil, "", false); !errors.Is(err, ErrTenantSessionScope) {
		t.Fatalf("nil summary enqueue error = %v", err)
	}
	if summary, ok := service.GetSessionSummaryText(context.Background(), nil); ok || summary != "" {
		t.Fatalf("invalid session summary = %q, ok=%v", summary, ok)
	}
}

type runtimeFixtureData struct {
	root           *tenant.Tenant
	tenantSnapshot tenant.ConfigurationSnapshot
	app            *agent.App
	revision       *agent.Revision
	modelProfile   *modelprofile.Profile
	modelCatalog   *modelprofile.ProviderCatalog
	backendProfile *backend.Profile
	backendCatalog *backend.ProviderCatalog
}

func runtimeFixture(t *testing.T) runtimeFixtureData {
	t.Helper()
	root := runtimeTenant(t, "runtime-tenant")
	modelCatalog := runtimeModelCatalog(t)
	modelProfile, err := modelprofile.NewProfile(modelprofile.CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary-model", DisplayName: "Primary Model",
		Configuration: modelprofile.Configuration{Provider: "fake", Model: "deterministic"},
	}, modelCatalog)
	if err != nil {
		t.Fatal(err)
	}
	app, revision := runtimeAgentFixture(t, root.TenantID, modelProfile.ProfileID, "support-app")
	backendCatalog := runtimeBackendCatalog(t)
	backendProfile, err := backend.NewProfile(backend.CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary-backend", DisplayName: "Primary Backend",
		Bindings: []backend.CapabilityBinding{{Capability: backend.CapabilitySession, Provider: "inmemory", Options: map[string]string{"namespace": "session"}}},
	}, backendCatalog)
	if err != nil {
		t.Fatal(err)
	}
	root.DefaultAgentAppID = stringPointer(app.AppID)
	root.DefaultBackendProfileID = stringPointer(backendProfile.ProfileID)
	root.Version++
	root.UpdatedAt = root.UpdatedAt.Add(time.Second)
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	return runtimeFixtureData{root: root, tenantSnapshot: tenantSnapshot, app: app, revision: revision, modelProfile: modelProfile, modelCatalog: modelCatalog, backendProfile: backendProfile, backendCatalog: backendCatalog}
}

func runtimeAgentFixture(t *testing.T, tenantID, modelProfileID, appKey string) (*agent.App, *agent.Revision) {
	return runtimeAgentFixtureWithPolicy(t, tenantID, modelProfileID, appKey, agent.DefaultRuntimePolicy())
}

func runtimeAgentFixtureWithPolicy(t *testing.T, tenantID, modelProfileID, appKey string, policy agent.RuntimePolicy) (*agent.App, *agent.Revision) {
	t.Helper()
	app, err := agent.NewApp(agent.CreateInput{TenantID: tenantID, AppKey: appKey, DisplayName: "Support App", Description: "Support"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := agent.NewRevision(agent.CreateRevisionInput{
		TenantID: tenantID, AppID: app.AppID, Revision: 1,
		Configuration: agent.DraftConfiguration{Description: "Support revision", Instruction: "Answer accurately.", GlobalInstruction: "Follow policy.", ModelProfileID: modelProfileID, Runtime: policy},
	})
	if err != nil {
		t.Fatal(err)
	}
	publishedAt := draft.UpdatedAt.Add(time.Second)
	published, err := draft.Publish(publishedAt)
	if err != nil {
		t.Fatal(err)
	}
	app.Status = agent.StatusActive
	app.CurrentRevision = int64Pointer(published.Revision)
	app.Version++
	app.UpdatedAt = publishedAt
	if err := app.Validate(); err != nil {
		t.Fatal(err)
	}
	return app, &published
}

func runtimeTenant(t *testing.T, key string) *tenant.Tenant {
	t.Helper()
	root, err := tenant.NewTenant(tenant.CreateInput{TenantKey: key, DisplayName: "Runtime Tenant", AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1})
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func runtimeModelCatalog(t *testing.T) *modelprofile.ProviderCatalog {
	t.Helper()
	defaultMode := "safe"
	catalog, err := modelprofile.NewProviderCatalog(modelprofile.ProviderSpec{
		Provider: "fake", Models: []string{"deterministic"}, EndpointPolicy: modelprofile.FieldForbidden, SecretRefPolicy: modelprofile.FieldForbidden,
		Options: map[string]modelprofile.OptionSpec{"mode": {Kind: modelprofile.OptionEnum, DefaultValue: &defaultMode, AllowedValues: []string{"fast", "safe"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

func runtimeBackendCatalog(t *testing.T) *backend.ProviderCatalog {
	t.Helper()
	catalog, err := backend.NewProviderCatalog(backend.ProviderSpec{Provider: "inmemory", Capabilities: []backend.Capability{backend.CapabilitySession}, EndpointPolicy: backend.FieldForbidden, SecretRefPolicy: backend.FieldForbidden, Options: map[string]backend.OptionSpec{"namespace": {Kind: backend.OptionString, Required: true}}})
	if err != nil {
		t.Fatal(err)
	}
	return catalog
}

type runtimeModelFactory struct {
	response  string
	block     bool
	input     modelprofile.ModelFactoryInput
	secret    modelprofile.SecretValue
	err       error
	returnNil bool
}

func (factory *runtimeModelFactory) New(_ context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (trpcmodel.Model, error) {
	factory.input = input
	factory.secret = secret
	if factory.err != nil {
		return nil, factory.err
	}
	if factory.returnNil {
		return nil, nil
	}
	return runtimeFakeModel{response: factory.response, block: factory.block}, nil
}

type runtimeFakeModel struct {
	response string
	block    bool
}

func (model runtimeFakeModel) Info() trpcmodel.Info { return trpcmodel.Info{Name: "deterministic"} }

func (model runtimeFakeModel) GenerateContent(ctx context.Context, _ *trpcmodel.Request) (<-chan *trpcmodel.Response, error) {
	responses := make(chan *trpcmodel.Response, 1)
	go func() {
		defer close(responses)
		if model.block {
			<-ctx.Done()
			return
		}
		select {
		case responses <- &trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.NewAssistantMessage(model.response)}}, Done: true}:
		case <-ctx.Done():
		}
	}()
	return responses, nil
}

func stringPointer(value string) *string { return &value }
func int64Pointer(value int64) *int64    { return &value }
