package tool

import (
	"context"
	"errors"
	"testing"

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
	called      bool
}

func (tool *namespaceProbeTool) Declaration() *trpctool.Declaration { return tool.declaration }
func (tool *namespaceProbeTool) Call(context.Context, []byte) (any, error) {
	tool.called = true
	return "ok", nil
}
