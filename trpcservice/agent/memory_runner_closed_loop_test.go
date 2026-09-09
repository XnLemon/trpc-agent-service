package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	memory "trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// TestRunnerMemoryTwoTurnClosedLoop proves that the platform-built Runner
// injects one tenant-scoped upstream memory service into more than one turn.
// The test calls the actual upstream memory tools from an Agent invocation so
// it covers the same context/session lookup used by an LLM tool call, without
// making a network model request part of deterministic CI.
func TestRunnerMemoryTwoTurnClosedLoop(t *testing.T) {
	input := runnerBuilderInputForTest(t)
	input.Agent.Tools = []appmodel.ToolAuthorization{
		{ToolID: memory.AddToolName, Required: true},
		{ToolID: memory.LoadToolName, Required: true},
	}

	sessions := sessioninmemory.NewSessionService()
	memories := memoryinmemory.NewMemoryService()
	storage := storagefactory.StorageFactoryFunc(func(_ context.Context, value storagefactory.StorageFactoryInput) (*storagefactory.CapabilitySet, error) {
		return storagefactory.NewCapabilitySet(value.TenantID, map[storagefactory.Capability]any{
			storagefactory.CapabilitySession: sessions,
			storagefactory.CapabilityMemory:  memories,
		})
	})
	observed := make(chan string, 1)
	factories, err := NewAgentFactoryRegistry(AgentFactoryRegistration{
		Kind: appmodel.KindLLM, SchemaVersion: appmodel.SchemaVersionV1,
		Factory: func(_ context.Context, build AgentBuildInput) (trpcagent.Agent, error) {
			return &memoryTurnAgent{tools: build.Tools, observed: observed}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunnerWithConfig(context.Background(), RunnerConfig{
		Input: input, StorageFactory: storage, ModelFactory: runnerBuilderModelFactory{}, AgentFactories: factories,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Errorf("runner.Close() error = %v", err)
		}
	}()

	run := func(message string) {
		events, runErr := runner.Run(context.Background(), "external-user", "conversation", trpcmodel.NewUserMessage(message))
		if runErr != nil {
			t.Fatalf("runner.Run(%q): %v", message, runErr)
		}
		for range events {
		}
	}
	run("remember my preferred response language is Chinese")
	select {
	case value := <-observed:
		if value != "stored" {
			t.Fatalf("first turn result = %q", value)
		}
	default:
		t.Fatal("first turn did not execute memory_add")
	}

	run("load what you remember")
	select {
	case value := <-observed:
		if !strings.Contains(value, "preferred response language is Chinese") {
			t.Fatalf("second turn memory result = %q", value)
		}
	case <-time.After(time.Second):
		t.Fatal("second turn did not execute memory_load")
	}

	// The raw user/session identifiers must not create an unscoped record in
	// the underlying service. The runner's session supplies the tenant prefix
	// used by the upstream memory tool.
	entries, err := memories.ReadMemories(context.Background(), memory.UserKey{AppName: input.Agent.Name, UserID: "external-user"}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("memory was written outside the tenant session namespace: %#v", entries)
	}
}

type memoryTurnAgent struct {
	tools    []trpctool.Tool
	observed chan<- string
}

func (agent *memoryTurnAgent) Run(ctx context.Context, invocation *trpcagent.Invocation) (<-chan *trpcevent.Event, error) {
	if invocation == nil || invocation.Session == nil {
		return nil, fmt.Errorf("memory test invocation is incomplete")
	}
	var name string
	var args []byte
	if strings.HasPrefix(invocation.Message.Content, "remember") {
		name = memory.AddToolName
		args, _ = json.Marshal(map[string]any{
			"memory": "The user preferred response language is Chinese",
			"topics": []string{"preference"},
		})
	} else {
		name = memory.LoadToolName
		args = []byte(`{"limit":10}`)
	}
	for _, candidate := range agent.tools {
		if candidate == nil || candidate.Declaration() == nil || candidate.Declaration().Name != name {
			continue
		}
		callable, ok := candidate.(trpctool.CallableTool)
		if !ok {
			return nil, fmt.Errorf("memory tool %q is not callable", name)
		}
		result, err := callable.Call(ctx, args)
		if err != nil {
			return nil, err
		}
		if name == memory.AddToolName {
			agent.observed <- "stored"
		} else {
			payload, _ := json.Marshal(result)
			agent.observed <- string(payload)
		}
		out := make(chan *trpcevent.Event)
		close(out)
		return out, nil
	}
	return nil, fmt.Errorf("memory tool %q was not injected", name)
}

func (agent *memoryTurnAgent) Tools() []trpctool.Tool { return agent.tools }
func (agent *memoryTurnAgent) Info() trpcagent.Info {
	return trpcagent.Info{Name: "memory-test-agent"}
}
func (agent *memoryTurnAgent) SubAgents() []trpcagent.Agent        { return nil }
func (agent *memoryTurnAgent) FindSubAgent(string) trpcagent.Agent { return nil }

var _ trpcagent.Agent = (*memoryTurnAgent)(nil)
var _ session.Service = (*sessioninmemory.SessionService)(nil)
