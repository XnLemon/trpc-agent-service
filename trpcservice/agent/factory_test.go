package agent

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
)

func TestDefaultAgentFactoryRegistryBuildsLLMAndChain(t *testing.T) {
	registry := DefaultAgentFactoryRegistry()
	model := agentTestModel{}

	llm, err := registry.Build(context.Background(), AgentBuildInput{
		Definition: LLMAgentFactoryInput{Name: "assistant", Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Instruction: "Answer clearly."},
		Model:      model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if llm.Info().Name != "assistant" || len(llm.SubAgents()) != 0 {
		t.Fatalf("LLM Agent info = %+v, sub-agents = %d", llm.Info(), len(llm.SubAgents()))
	}

	chain, err := registry.Build(context.Background(), AgentBuildInput{
		Definition: LLMAgentFactoryInput{
			Name: "support-pipeline", Kind: appmodel.KindChain, SchemaVersion: appmodel.SchemaVersionV1,
			Description: "A sequential support pipeline", Instruction: "Run the pipeline.",
			Runtime: appmodel.DefaultRuntimePolicy(),
			Chain: &appmodel.ChainConfiguration{Steps: []appmodel.ChainStep{
				{Name: "classify", Instruction: "Classify the request."},
				{Name: "answer", Instruction: "Draft the final answer."},
			}},
		},
		Model: model,
	})
	if err != nil {
		t.Fatal(err)
	}
	if chain.Info().Name != "support-pipeline" {
		t.Fatalf("Chain info = %+v", chain.Info())
	}
	children := chain.SubAgents()
	if len(children) != 2 {
		t.Fatalf("Chain children = %d, want 2", len(children))
	}
	if children[0].Info().Name != "classify" || children[1].Info().Name != "answer" {
		t.Fatalf("Chain child names = %q, %q", children[0].Info().Name, children[1].Info().Name)
	}
	if chain.FindSubAgent("answer") == nil || chain.FindSubAgent("missing") != nil {
		t.Fatal("Chain child lookup did not preserve the published step names")
	}
}

func TestAgentFactoryRegistryRejectsDuplicateAndUnknownFactories(t *testing.T) {
	factory := func(context.Context, AgentBuildInput) (trpcagent.Agent, error) {
		return llmagent.New("custom"), nil
	}
	registry, err := NewAgentFactoryRegistry(AgentFactoryRegistration{Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1, Factory: factory})
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(appmodel.KindLLM, appmodel.SchemaVersionV1, factory); !errors.Is(err, ErrAgentFactory) {
		t.Fatalf("duplicate registration error = %v", err)
	}
	_, err = registry.Build(context.Background(), AgentBuildInput{
		Definition: LLMAgentFactoryInput{Name: "unsupported", Kind: appmodel.KindChain, SchemaVersion: appmodel.SchemaVersionV1},
		Model:      agentTestModel{},
	})
	if !errors.Is(err, ErrAgentFactoryNotFound) {
		t.Fatalf("unknown factory error = %v", err)
	}
}
