package agent

import (
	"context"
	"sync"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestRunnerExecutesAuthorizedUpstreamKnowledgeTool(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = []appmodel.ToolAuthorization{{ToolID: "knowledge_search", Required: true}}
	kb := &recordingKnowledge{}
	toolModel := &knowledgeToolCallingModel{}
	sessions := sessioninmemory.NewSessionService()
	factory := storagefactory.StorageFactoryFunc(func(_ context.Context, value backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[backend.Capability]any{
			backend.CapabilitySession:   sessions,
			backend.CapabilityKnowledge: kb,
		})
	})
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
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
	events, err := runner.Run(context.Background(), "user", "session", model.NewUserMessage("What is the tenant policy?"))
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	if kb.query() != "tenant policy" {
		t.Fatalf("knowledge query = %q", kb.query())
	}
	if toolModel.calls() != 2 || !toolModel.sawToolResult() {
		t.Fatalf("model calls = %d, saw tool result = %v", toolModel.calls(), toolModel.sawToolResult())
	}
}

func TestRunnerDoesNotExposeUnauthorizedKnowledgeTool(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = nil
	kb := &recordingKnowledge{}
	captured := make(chan AgentBuildInput, 1)
	factories, err := NewAgentFactoryRegistry(AgentFactoryRegistration{
		Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
		Factory: func(_ context.Context, build AgentBuildInput) (trpcagent.Agent, error) {
			captured <- build
			return invocationCaptureAgent{captured: make(chan *trpcagent.Invocation, 1)}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	sessions := sessioninmemory.NewSessionService()
	factory := storagefactory.StorageFactoryFunc(func(_ context.Context, value backend.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[backend.Capability]any{
			backend.CapabilitySession:   sessions,
			backend.CapabilityKnowledge: kb,
		})
	})
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input,
		ModelFactory: knowledgeModelFactory(func(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (model.Model, error) {
			return &knowledgeToolCallingModel{}, nil
		}),
		StorageFactory: factory,
		AgentFactories: factories,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer runner.Close()
	build := <-captured
	if build.Knowledge != nil {
		t.Fatal("unauthorized Knowledge was passed to the Agent factory")
	}
}

type knowledgeModelFactory func(context.Context, modelprofile.ModelFactoryInput, modelprofile.SecretValue) (model.Model, error)

func (factory knowledgeModelFactory) New(ctx context.Context, input modelprofile.ModelFactoryInput, secret modelprofile.SecretValue) (model.Model, error) {
	return factory(ctx, input, secret)
}

type recordingKnowledge struct {
	mu        sync.Mutex
	lastQuery string
}

func (knowledgeBase *recordingKnowledge) Search(_ context.Context, request *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	knowledgeBase.mu.Lock()
	knowledgeBase.lastQuery = request.Query
	knowledgeBase.mu.Unlock()
	doc := &document.Document{ID: "policy", Name: "Tenant policy", Content: "Every execution uses its authenticated tenant identity."}
	return &knowledge.SearchResult{Document: doc, Text: doc.Content, Score: 1, Documents: []*knowledge.Result{{Document: doc, Score: 1}}}, nil
}

func (knowledgeBase *recordingKnowledge) query() string {
	knowledgeBase.mu.Lock()
	defer knowledgeBase.mu.Unlock()
	return knowledgeBase.lastQuery
}

type knowledgeToolCallingModel struct {
	mu             sync.Mutex
	callCount      int
	observedResult bool
}

func (*knowledgeToolCallingModel) Info() model.Info { return model.Info{Name: "knowledge-tool-caller"} }

func (toolModel *knowledgeToolCallingModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	toolModel.mu.Lock()
	toolModel.callCount++
	call := toolModel.callCount
	for _, message := range request.Messages {
		if message.Role == model.RoleTool {
			toolModel.observedResult = true
		}
	}
	toolModel.mu.Unlock()
	response := &model.Response{Done: true, Choices: []model.Choice{{Message: model.NewAssistantMessage("The tenant policy requires authenticated identity.")}}}
	if call == 1 {
		response.Choices[0].Message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{Type: "function", ID: "knowledge-call", Function: model.FunctionDefinitionParam{Name: "knowledge_search", Arguments: []byte(`{"query":"tenant policy"}`)}}}}
	}
	responses := make(chan *model.Response, 1)
	select {
	case responses <- response:
	case <-ctx.Done():
	}
	close(responses)
	return responses, nil
}

func (toolModel *knowledgeToolCallingModel) calls() int {
	toolModel.mu.Lock()
	defer toolModel.mu.Unlock()
	return toolModel.callCount
}

func (toolModel *knowledgeToolCallingModel) sawToolResult() bool {
	toolModel.mu.Lock()
	defer toolModel.mu.Unlock()
	return toolModel.observedResult
}
