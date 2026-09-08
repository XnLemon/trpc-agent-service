package tool

import (
	"errors"
	"testing"
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
