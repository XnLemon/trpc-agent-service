package tool

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestMCPBindingNormalize(t *testing.T) {
	validHTTP := MCPBinding{Name: "github", Transport: "streamable_http", ServerURL: "https://mcp.example.com/tools", SecretRef: "secret://tenant/mcp", ToolAllow: []string{"search", "search", "read"}}
	got, err := validHTTP.Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if got.Transport != "streamable" || len(got.ToolAllow) != 2 || got.ToolAllow[0] != "read" {
		t.Fatalf("normalized MCP binding = %+v", got)
	}

	cases := []MCPBinding{
		{Name: "mcp", Transport: "https", ServerURL: "https://example.com", SecretRef: "secret://mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "sse", ServerURL: "http://example.com", SecretRef: "secret://mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "sse", ServerURL: "https://127.0.0.1/mcp", SecretRef: "secret://mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "sse", ServerURL: "https://example.com?token=secret", SecretRef: "secret://mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "sse", ServerURL: "https://user:pass@example.com/mcp", SecretRef: "secret://mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "stdio", Command: "relative-command", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "stdio", Command: "/usr/bin/mcp", SecretRef: "secret://mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "stdio", Command: "/usr/bin/mcp", Args: []string{"bad\narg"}, ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "stdio", Command: "/usr/bin/mcp", ToolAllow: []string{}},
	}
	for _, value := range cases {
		if _, err := value.Normalize(); !errors.Is(err, ErrInvalidMCPBinding) {
			t.Errorf("binding %+v error = %v", value, err)
		}
	}
}

func TestNewMCPToolSetRequiresTenantScopedSecretResolver(t *testing.T) {
	binding := MCPBinding{Name: "mcp", Transport: "sse", ServerURL: "https://example.com/mcp", SecretRef: "secret://tenant/mcp", ToolAllow: []string{"read"}}
	if _, err := NewMCPToolSet(nil, "tenant", binding, nil); !errors.Is(err, ErrInvalidMCPBinding) {
		t.Fatalf("nil resolver error = %v", err)
	}
}

func TestWrapMCPToolSetLifecycleRetriesDisconnectedCalls(t *testing.T) {
	delegateTool := &lifecycleProbeTool{declaration: &trpctool.Declaration{Name: "echo"}}
	delegate := &lifecycleProbeSet{tool: delegateTool}
	wrapped, err := WrapMCPToolSetLifecycle(delegate)
	if err != nil {
		t.Fatal(err)
	}
	tools := wrapped.Tools(context.Background())
	callable, ok := tools[0].(trpctool.CallableTool)
	if !ok {
		t.Fatalf("lifecycle tool is not callable: %T", tools[0])
	}
	if _, err := callable.Call(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if delegateTool.calls != 2 || delegate.closeCalls != 1 {
		t.Fatalf("lifecycle retry = calls:%d closes:%d", delegateTool.calls, delegate.closeCalls)
	}
}

type lifecycleProbeSet struct {
	tool       *lifecycleProbeTool
	closeCalls int
}

func (set *lifecycleProbeSet) Name() string { return "lifecycle" }
func (set *lifecycleProbeSet) Tools(context.Context) []trpctool.Tool {
	return []trpctool.Tool{set.tool}
}
func (set *lifecycleProbeSet) Close() error { set.closeCalls++; return nil }

type lifecycleProbeTool struct {
	declaration *trpctool.Declaration
	calls       int
}

func (tool *lifecycleProbeTool) Declaration() *trpctool.Declaration { return tool.declaration }
func (tool *lifecycleProbeTool) Call(context.Context, []byte) (any, error) {
	tool.calls++
	if tool.calls == 1 {
		return nil, errors.New("transport closed")
	}
	return "ok", nil
}

func TestNamespaceMCPToolSetPrefixesDeclarationsAndForwardsCalls(t *testing.T) {
	delegateTool := &namespaceProbeTool{declaration: &trpctool.Declaration{Name: "search"}}
	delegate := &namespaceProbeSet{tools: []trpctool.Tool{delegateTool}}
	set, err := NamespaceMCPToolSet(delegate, " github ")
	if err != nil {
		t.Fatal(err)
	}
	tools := set.Tools(context.Background())
	if len(tools) != 1 || tools[0].Declaration().Name != "mcp_github__search" {
		t.Fatalf("namespaced tools = %#v", tools)
	}
	callable, ok := tools[0].(trpctool.CallableTool)
	if !ok {
		t.Fatalf("namespaced tool does not preserve callable capability: %T", tools[0])
	}
	if _, err := callable.Call(context.Background(), []byte(`{"query":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	if !delegateTool.called {
		t.Fatal("namespaced call did not reach delegate")
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if !delegate.closed {
		t.Fatal("namespaced close did not reach delegate")
	}
}

type namespaceProbeSet struct {
	tools  []trpctool.Tool
	closed bool
}

func (set *namespaceProbeSet) Name() string                          { return "probe" }
func (set *namespaceProbeSet) Tools(context.Context) []trpctool.Tool { return set.tools }
func (set *namespaceProbeSet) Close() error                          { set.closed = true; return nil }

type namespaceProbeTool struct {
	declaration *trpctool.Declaration
	metadata    trpctool.ToolMetadata
	called      bool
}

func (tool *namespaceProbeTool) Declaration() *trpctool.Declaration  { return tool.declaration }
func (tool *namespaceProbeTool) ToolMetadata() trpctool.ToolMetadata { return tool.metadata }
func (tool *namespaceProbeTool) Call(context.Context, []byte) (any, error) {
	tool.called = true
	return "ok", nil
}

func TestToolCallBudgetIsRequestLocalAndBounded(t *testing.T) {
	budget, err := NewToolCallBudget(2)
	if err != nil {
		t.Fatal(err)
	}
	if err := budget.Consume(); err != nil {
		t.Fatal(err)
	}
	if err := budget.Consume(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(budget.Consume(), ErrToolBudgetExceeded) {
		t.Fatal("third tool call unexpectedly admitted")
	}
	if _, err := NewToolCallBudget(0); err == nil {
		t.Fatal("zero tool budget accepted")
	}
}

func TestGovernMCPToolSetRequiresReviewAndAuditsExecution(t *testing.T) {
	writer := &mcpGovernanceWriter{}
	attachments := runtimestorageinmemory.New()
	defer attachments.Close()
	binding := MCPBinding{
		Name: "files", Transport: "stdio", Command: "/usr/bin/files", ToolAllow: []string{"read", "delete"},
		ToolPolicies: map[string]appmodel.MCPToolPolicy{"read": appmodel.MCPToolPolicySkipApproval, "delete": appmodel.MCPToolPolicyRequireApproval},
	}
	readTool := &namespaceProbeTool{declaration: &trpctool.Declaration{Name: "mcp_files__read"}, metadata: trpctool.ToolMetadata{ReadOnly: true}}
	deleteTool := &namespaceProbeTool{declaration: &trpctool.Declaration{Name: "mcp_files__delete", Description: "delete a file"}, metadata: trpctool.ToolMetadata{Destructive: true}}
	delegate := &namespaceProbeSet{tools: []trpctool.Tool{readTool, deleteTool}}
	reviewer := &mcpGovernanceReviewer{approved: true}
	governed, err := GovernMCPToolSet(context.Background(), delegate, "tenant-a", binding, reviewer)
	if err != nil {
		t.Fatal(err)
	}
	execution := WithExecutionContext(context.Background(), ExecutionContext{
		TenantID: "tenant-a", AppID: "app-a", UserID: "user-a", SessionID: "session-a", EventID: "event-a", RequestID: "request-a", TraceID: "trace-a",
		Attachments: attachments, Replies: NewReplyCollector(), Audit: audit.NewRecorder(writer, "tenant-a"),
	})
	tools := governed.Tools(execution)
	for _, candidate := range tools {
		callable := candidate.(trpctool.CallableTool)
		if _, err := callable.Call(execution, []byte(`{"path":"/tmp/file"}`)); err != nil {
			t.Fatal(err)
		}
	}
	if reviewer.calls != 1 || !deleteTool.called || !readTool.called {
		t.Fatalf("review/execution calls = reviewer:%d read:%v delete:%v", reviewer.calls, readTool.called, deleteTool.called)
	}
	if len(writer.events) != 5 || writer.events[0].EventType != audit.EventToolAllowed || writer.events[1].EventType != audit.EventToolExecuted || writer.events[2].EventType != audit.EventToolApprovalRequired || writer.events[3].EventType != audit.EventToolAllowed || writer.events[4].EventType != audit.EventToolExecuted {
		t.Fatalf("MCP governance audit events = %#v", writer.events)
	}

	if _, err := GovernMCPToolSet(context.Background(), delegate, "tenant-a", binding, nil); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("missing reviewer error = %v", err)
	}
}

type mcpGovernanceReviewer struct {
	approved bool
	calls    int
}

func (reviewer *mcpGovernanceReviewer) Review(context.Context, *review.Request) (*review.Decision, error) {
	reviewer.calls++
	return &review.Decision{Approved: reviewer.approved, RiskLevel: "low", Reason: "test"}, nil
}

type mcpGovernanceWriter struct {
	events []audit.Event
}

func (writer *mcpGovernanceWriter) Append(_ context.Context, event audit.Event) (audit.AppendResult, error) {
	writer.events = append(writer.events, event)
	return audit.AppendResult{Event: event}, nil
}
