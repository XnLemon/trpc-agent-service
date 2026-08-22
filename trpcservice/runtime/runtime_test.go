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
	t.Helper()
	app, err := agent.NewApp(agent.CreateInput{TenantID: tenantID, AppKey: appKey, DisplayName: "Support App", Description: "Support"})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := agent.NewRevision(agent.CreateRevisionInput{
		TenantID: tenantID, AppID: app.AppID, Revision: 1,
		Configuration: agent.DraftConfiguration{Description: "Support revision", Instruction: "Answer accurately.", GlobalInstruction: "Follow policy.", ModelProfileID: modelProfileID, Runtime: agent.DefaultRuntimePolicy()},
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
	response string
	block    bool
	input    modelprofile.ModelFactoryInput
	secret   modelprofile.SecretValue
}

func (factory *runtimeModelFactory) New(_ context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (trpcmodel.Model, error) {
	factory.input = input
	factory.secret = secret
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
