package agent

import (
	"context"
	"fmt"
	"sync"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRunnerExportsAuthorizedArtifactToPlatformAttachment(t *testing.T) {
	ctx := context.Background()
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = []appmodel.ToolAuthorization{{ToolID: servicetool.ExportArtifactID, Required: true}}
	artifactService := artifactinmemory.NewService()
	scope := artifact.SessionInfo{AppName: input.Agent.AppID, UserID: "user-a", SessionID: "session-a"}
	if version, err := artifactService.SaveArtifact(ctx, scope, "report.txt", &artifact.Artifact{
		Data: []byte("runner artifact"), MimeType: "text/plain", Name: "report.txt", URL: "https://private.invalid/report",
	}); err != nil || version != 0 {
		t.Fatalf("save artifact = %d, %v", version, err)
	}

	platformStore := runtimestorageinmemory.New()
	if _, err := platformStore.CreateSession(ctx, input.Tenant.TenantID, "session-a", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := platformStore.RecordMessage(ctx, runtimestorage.MessageEventInput{
		TenantID: input.Tenant.TenantID, EventID: "artifact-event", SessionID: "session-a",
		BindingID: "binding-a", ExternalMessageID: "artifact-message",
	}); err != nil {
		t.Fatal(err)
	}
	collector := servicetool.NewReplyCollector()
	runCtx := servicetool.WithExecutionContext(ctx, servicetool.ExecutionContext{
		TenantID: input.Tenant.TenantID, AppID: input.Agent.AppID, UserID: "user-a", SessionID: "session-a",
		EventID: "artifact-event", RequestID: "artifact-request", Attachments: platformStore, Replies: collector,
	})
	toolModel := &artifactExportModel{}
	sessions := sessioninmemory.NewSessionService()
	factory := storagefactory.StorageFactoryFunc(func(_ context.Context, value backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[backend.Capability]any{
			backend.CapabilitySession: sessions, backend.CapabilityArtifact: artifactService,
		})
	})
	runner, err := NewRunnerWithConfig(ctx, RunnerConfig{
		Input: input,
		ModelFactory: knowledgeModelFactory(func(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (model.Model, error) {
			return toolModel, nil
		}),
		StorageFactory: factory,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	events, err := runner.Run(runCtx, "user-a", "session-a", model.NewUserMessage("send the report"))
	if err != nil {
		t.Fatal(err)
	}
	var eventErrors []string
	for event := range events {
		if event != nil && event.Response != nil && event.Response.Error != nil {
			eventErrors = append(eventErrors, fmt.Sprint(event.Response.Error))
		}
	}
	if toolModel.calls() != 2 || !toolModel.sawToolResult() {
		t.Fatalf("model calls = %d, saw result = %v, event errors = %v", toolModel.calls(), toolModel.sawToolResult(), eventErrors)
	}
	intents := collector.Intents()
	if len(intents) != 1 || intents[0].Kind != runtimestorage.ReplyKindDocument {
		t.Fatalf("artifact reply intents = %#v", intents)
	}
	content, err := platformStore.Load(ctx, input.Tenant.TenantID, "artifact-event", intents[0].Attachment)
	if err != nil || string(content.Data) != "runner artifact" {
		t.Fatalf("platform attachment = %q, %v", content.Data, err)
	}
}

type artifactExportModel struct {
	mu             sync.Mutex
	callCount      int
	observedResult bool
}

func (*artifactExportModel) Info() model.Info { return model.Info{Name: "artifact-export-model"} }

func (toolModel *artifactExportModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	toolModel.mu.Lock()
	toolModel.callCount++
	call := toolModel.callCount
	for _, message := range request.Messages {
		if message.Role == model.RoleTool {
			toolModel.observedResult = true
		}
	}
	toolModel.mu.Unlock()
	response := &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewAssistantMessage("The report is queued.")}}}
	if call == 1 {
		response.Choices[0].Message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{
			Type: "function", ID: "artifact-export-call",
			Function: model.FunctionDefinitionParam{Name: servicetool.ExportArtifactID, Arguments: []byte(`{"filename":"report.txt","version":0}`)},
		}}}
	}
	responses := make(chan *model.Response, 1)
	select {
	case responses <- response:
	case <-ctx.Done():
	}
	close(responses)
	return responses, nil
}

func (toolModel *artifactExportModel) calls() int {
	toolModel.mu.Lock()
	defer toolModel.mu.Unlock()
	return toolModel.callCount
}

func (toolModel *artifactExportModel) sawToolResult() bool {
	toolModel.mu.Lock()
	defer toolModel.mu.Unlock()
	return toolModel.observedResult
}
