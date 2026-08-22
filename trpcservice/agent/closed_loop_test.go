package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestPublishedRepositoryStateBuildsExecutionSnapshot(t *testing.T) {
	tenantRoot, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "closed-loop", DisplayName: "Closed Loop",
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(tenantRoot)
	if err != nil {
		t.Fatal(err)
	}
	repository := inmemory.NewRepository()
	app, err := repository.Create(context.Background(), agent.CreateInput{
		TenantID: tenantRoot.TenantID, AppKey: "support", DisplayName: "Support",
	})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := repository.CreateDraft(context.Background(), agent.CreateDraftInput{
		TenantID: tenantRoot.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Configuration: agent.DraftConfiguration{
			Description: "Support Agent", Instruction: "Answer accurately.", ModelProfileID: "model-primary",
			Generation: agent.GenerationConfig{Temperature: externalFloat64Pointer(0.2)},
			Runtime:    agent.DefaultRuntimePolicy(), Tools: []agent.ToolAuthorization{{ToolID: "search"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	publishedApp, publishedRevision, _, err := repository.Publish(context.Background(), agent.PublishInput{
		TenantID: tenantRoot.TenantID, AppID: app.AppID, Revision: draft.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: agent.ChangeMetadata{ActorType: "admin", ActorID: "user-1", Reason: "release", CorrelationID: "corr-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := agent.NewAgentExecutionSnapshot(tenantSnapshot, publishedApp, publishedRevision)
	if err != nil {
		t.Fatal(err)
	}
	factoryInput, err := execution.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if factoryInput.TenantID != tenantRoot.TenantID || factoryInput.AppID != app.AppID || factoryInput.Revision != draft.Revision || factoryInput.ContentDigest != publishedRevision.ContentDigest || factoryInput.Name != "support" {
		t.Fatalf("closed loop lost fixed execution identity: %+v", factoryInput)
	}

	suspended, _, err := repository.TransitionStatus(context.Background(), agent.TransitionStatusInput{
		TenantID: tenantRoot.TenantID, AppID: app.AppID, ExpectedVersion: publishedApp.Version,
		NextStatus: agent.StatusSuspended,
		Metadata:   agent.ChangeMetadata{ActorType: "admin", ActorID: "user-1", Reason: "pause", CorrelationID: "corr-2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.NewAgentExecutionSnapshot(tenantSnapshot, suspended, publishedRevision); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("suspended App admitted a new execution: %v", err)
	}
}

func externalFloat64Pointer(value float64) *float64 { return &value }
