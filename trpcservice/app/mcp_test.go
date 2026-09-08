package app

import (
	"errors"
	"testing"
)

func TestMCPBindingNormalizesPublishedConfiguration(t *testing.T) {
	binding, err := (MCPBinding{
		Name: " files ", Transport: "streamable_http", ServerURL: "https://mcp.example.test/tools",
		SecretRef: " secret://tenant/mcp ", ToolAllow: []string{"write", "read", "write"},
	}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if binding.Name != "files" || binding.Transport != "streamable" || binding.SecretRef != "secret://tenant/mcp" || binding.TimeoutSeconds != defaultMCPTimeoutSeconds {
		t.Fatalf("normalized binding = %+v", binding)
	}
	if len(binding.ToolAllow) != 2 || binding.ToolAllow[0] != "read" || binding.ToolAllow[1] != "write" {
		t.Fatalf("normalized tool allowlist = %#v", binding.ToolAllow)
	}
	withPolicy, err := (MCPBinding{
		Name: "files", Transport: "stdio", Command: "/usr/bin/files", ToolAllow: []string{"read", "delete"},
		ToolPolicies: map[string]MCPToolPolicy{" delete ": MCPToolPolicyRequireApproval, "read": ""},
	}).Normalize()
	if err != nil || withPolicy.ToolPolicies["delete"] != MCPToolPolicyRequireApproval || withPolicy.ToolPolicies["read"] != MCPToolPolicyAuto {
		t.Fatalf("normalized tool policies = %#v err=%v", withPolicy.ToolPolicies, err)
	}
}

func TestMCPBindingRejectsUnsafeOrIncompleteDeclarations(t *testing.T) {
	cases := []MCPBinding{
		{Name: "mcp", Transport: "sse", ServerURL: "http://example.test", SecretRef: "secret://mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "sse", ServerURL: "https://example.test/?token=secret", SecretRef: "secret://mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "sse", ServerURL: "https://example.test", SecretRef: "literal secret", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "stdio", Command: "mcp", ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "stdio", Command: "/usr/bin/mcp", Args: []string{"bad\narg"}, ToolAllow: []string{"read"}},
		{Name: "mcp", Transport: "stdio", Command: "/usr/bin/mcp", ToolAllow: nil},
		{Name: "mcp", Transport: "sse", ServerURL: "https://example.test", SecretRef: "secret://mcp", ToolAllow: []string{"bad name"}},
		{Name: "mcp", Transport: "stdio", Command: "/usr/bin/mcp", ToolAllow: []string{"read"}, TimeoutSeconds: maxMCPTimeoutSeconds + 1},
		{Name: "mcp", Transport: "stdio", Command: "/usr/bin/mcp", ToolAllow: []string{"read"}, ToolPolicies: map[string]MCPToolPolicy{"other": MCPToolPolicyDenied}},
		{Name: "mcp", Transport: "stdio", Command: "/usr/bin/mcp", ToolAllow: []string{"read"}, ToolPolicies: map[string]MCPToolPolicy{"read": "unknown"}},
	}
	for _, value := range cases {
		if _, err := value.Normalize(); !errors.Is(err, ErrInvalidMCPBinding) || !errors.Is(err, ErrInvalid) {
			t.Errorf("binding %+v error = %v", value, err)
		}
	}
}

func TestMCPBindingsAreCanonicalAndTenantRevisionScoped(t *testing.T) {
	first, err := normalizeMCPBindings([]MCPBinding{
		{Name: "zeta", Transport: "stdio", Command: "/usr/bin/zeta", ToolAllow: []string{"z"}},
		{Name: "alpha", Transport: "stdio", Command: "/usr/bin/alpha", ToolAllow: []string{"a"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first[0].Name != "alpha" || first[1].Name != "zeta" {
		t.Fatalf("bindings were not sorted by stable name: %#v", first)
	}
	if _, err := normalizeMCPBindings([]MCPBinding{
		{Name: "same", Transport: "stdio", Command: "/usr/bin/one", ToolAllow: []string{"one"}},
		{Name: "same", Transport: "stdio", Command: "/usr/bin/two", ToolAllow: []string{"two"}},
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("duplicate binding error = %v", err)
	}
}
