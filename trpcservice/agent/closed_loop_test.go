package agent_test

import (
	"context"
	"errors"
	"testing"

	agentruntime "github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/app/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
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
	appRoot, err := repository.Create(context.Background(), appmodel.CreateInput{
		TenantID: tenantRoot.TenantID, AppKey: "support", DisplayName: "Support",
	})
	if err != nil {
		t.Fatal(err)
	}
	draft, err := repository.CreateDraft(context.Background(), appmodel.CreateDraftInput{
		TenantID: tenantRoot.TenantID, AppID: appRoot.AppID, ExpectedAppVersion: appRoot.Version,
		Configuration: appmodel.DraftConfiguration{
			Description: "Support Agent", Instruction: "Answer accurately.", ModelProfileID: "model-primary",
			Generation: appmodel.GenerationConfig{Temperature: externalFloat64Pointer(0.2)},
			Runtime:    appmodel.DefaultRuntimePolicy(), Tools: []appmodel.ToolAuthorization{{ToolID: "search"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	publishedApp, publishedRevision, _, err := repository.Publish(context.Background(), appmodel.PublishInput{
		TenantID: tenantRoot.TenantID, AppID: appRoot.AppID, Revision: draft.Revision,
		ExpectedAppVersion: appRoot.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: appmodel.ChangeMetadata{ActorType: "admin", ActorID: "user-1", Reason: "release", CorrelationID: "corr-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := agentruntime.NewAgentExecutionSnapshot(tenantSnapshot, publishedApp, publishedRevision)
	if err != nil {
		t.Fatal(err)
	}
	factoryInput, err := execution.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if factoryInput.TenantID != tenantRoot.TenantID || factoryInput.AppID != appRoot.AppID || factoryInput.Revision != draft.Revision || factoryInput.ContentDigest != publishedRevision.ContentDigest || factoryInput.Name != "support" {
		t.Fatalf("closed loop lost fixed execution identity: %+v", factoryInput)
	}

	suspended, _, err := repository.TransitionStatus(context.Background(), appmodel.TransitionStatusInput{
		TenantID: tenantRoot.TenantID, AppID: appRoot.AppID, ExpectedVersion: publishedApp.Version,
		NextStatus: appmodel.StatusSuspended,
		Metadata:   appmodel.ChangeMetadata{ActorType: "admin", ActorID: "user-1", Reason: "pause", CorrelationID: "corr-2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agentruntime.NewAgentExecutionSnapshot(tenantSnapshot, suspended, publishedRevision); !errors.Is(err, agentruntime.ErrInvalid) {
		t.Fatalf("suspended App admitted a new execution: %v", err)
	}
}

func TestUpstreamArtifactVersionsAndSessionIsolation(t *testing.T) {
	ctx := context.Background()
	service := artifactinmemory.NewService()
	first := artifact.SessionInfo{AppName: "app", UserID: "user-a", SessionID: "session-a"}
	second := artifact.SessionInfo{AppName: "app", UserID: "user-b", SessionID: "session-a"}

	version, err := service.SaveArtifact(ctx, first, "report.txt", &artifact.Artifact{Data: []byte("version-0"), MimeType: "text/plain"})
	if err != nil || version != 0 {
		t.Fatalf("first upstream artifact version = %d, %v", version, err)
	}
	version, err = service.SaveArtifact(ctx, first, "report.txt", &artifact.Artifact{Data: []byte("version-1"), MimeType: "text/plain"})
	if err != nil || version != 1 {
		t.Fatalf("second upstream artifact version = %d, %v", version, err)
	}
	versions, err := service.ListVersions(ctx, first, "report.txt")
	if err != nil || len(versions) != 2 || versions[0] != 0 || versions[1] != 1 {
		t.Fatalf("upstream artifact versions = %v, %v", versions, err)
	}
	for expectedVersion, expectedData := range map[int]string{0: "version-0", 1: "version-1"} {
		loaded, loadErr := service.LoadArtifact(ctx, first, "report.txt", &expectedVersion)
		if loadErr != nil || loaded == nil || string(loaded.Data) != expectedData {
			t.Fatalf("upstream artifact version %d = %#v, %v", expectedVersion, loaded, loadErr)
		}
	}
	other, err := service.LoadArtifact(ctx, second, "report.txt", nil)
	if err != nil {
		t.Fatal(err)
	}
	if other != nil {
		t.Fatalf("artifact crossed user namespace: %#v", other)
	}
}

func externalFloat64Pointer(value float64) *float64 { return &value }
