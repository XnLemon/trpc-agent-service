package app

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

var ErrInvalidMCPBinding = errors.New("invalid MCP binding")

const (
	defaultMCPTimeoutSeconds = 30
	maxMCPTimeoutSeconds     = 300
)

func invalidMCP(format string, args ...any) error {
	return fmt.Errorf("%w: %w: %s", ErrInvalid, ErrInvalidMCPBinding, fmt.Sprintf(format, args...))
}

// MCPToolPolicy controls approval for one allowlisted MCP tool.
type MCPToolPolicy string

const (
	// MCPToolPolicyAuto uses the tool's MCP metadata: destructive tools require
	// approval while explicitly read-only tools may run without a review.
	MCPToolPolicyAuto MCPToolPolicy = "auto"
	// MCPToolPolicyRequireApproval always sends the call to a Reviewer.
	MCPToolPolicyRequireApproval MCPToolPolicy = "require_approval"
	// MCPToolPolicySkipApproval explicitly permits the call without a Reviewer.
	MCPToolPolicySkipApproval MCPToolPolicy = "skip_approval"
	// MCPToolPolicyDenied blocks the call even when it is allowlisted.
	MCPToolPolicyDenied MCPToolPolicy = "denied"
)

// MCPBinding is the secret-free, revision-scoped declaration of one MCP
// server. It is embedded in the immutable Revision rather than stored as a
// mutable process-level connection. SecretRef is resolved only while a
// runner-owned ToolSet is materialized.
type MCPBinding struct {
	Name           string                   `json:"name"`
	Transport      string                   `json:"transport"`
	ServerURL      string                   `json:"server_url,omitempty"`
	SecretRef      string                   `json:"secret_ref,omitempty"`
	Command        string                   `json:"command,omitempty"`
	Args           []string                 `json:"args,omitempty"`
	ToolAllow      []string                 `json:"tool_allow"`
	ToolPolicies   map[string]MCPToolPolicy `json:"tool_policies,omitempty"`
	TimeoutSeconds int                      `json:"timeout_seconds,omitempty"`
}

// Normalize validates and canonicalizes a revision MCP declaration. It does
// not perform DNS resolution, access the network, resolve secrets, or start a
// process; those checks belong to the runtime materialization boundary.
func (binding MCPBinding) Normalize() (MCPBinding, error) {
	value := binding
	value.Name = strings.TrimSpace(value.Name)
	value.Transport = strings.ToLower(strings.TrimSpace(value.Transport))
	value.ServerURL = strings.TrimSpace(value.ServerURL)
	value.SecretRef = strings.TrimSpace(value.SecretRef)
	value.Command = strings.TrimSpace(value.Command)
	value.Args = append([]string(nil), value.Args...)
	if value.TimeoutSeconds == 0 {
		value.TimeoutSeconds = defaultMCPTimeoutSeconds
	}
	if value.TimeoutSeconds < 1 || value.TimeoutSeconds > maxMCPTimeoutSeconds {
		return MCPBinding{}, invalidMCP("MCP timeout must be between 1 and %d seconds", maxMCPTimeoutSeconds)
	}
	allow, err := normalizeMCPNames(value.ToolAllow)
	if err != nil {
		return MCPBinding{}, err
	}
	value.ToolAllow = allow
	policies, err := normalizeMCPToolPolicies(value.ToolPolicies, allow)
	if err != nil {
		return MCPBinding{}, err
	}
	value.ToolPolicies = policies
	if !validMCPName(value.Name) {
		return MCPBinding{}, invalidMCP("MCP binding name is invalid")
	}
	if value.SecretRef != "" && (!validMCPText(value.SecretRef) || strings.IndexFunc(value.SecretRef, unicode.IsSpace) >= 0) {
		return MCPBinding{}, invalidMCP("MCP secret reference is invalid")
	}
	if len(value.ToolAllow) == 0 {
		return MCPBinding{}, invalidMCP("MCP tool allowlist is required")
	}
	switch value.Transport {
	case "sse", "streamable", "streamable_http":
		if value.SecretRef == "" || value.Command != "" {
			return MCPBinding{}, invalidMCP("MCP HTTP binding requires a secret and no command")
		}
		if err := validateMCPHTTPURL(value.ServerURL); err != nil {
			return MCPBinding{}, err
		}
		value.Transport = strings.TrimSuffix(value.Transport, "_http")
	case "stdio":
		if value.SecretRef != "" || value.ServerURL != "" || !filepath.IsAbs(value.Command) || !validMCPText(value.Command) {
			return MCPBinding{}, invalidMCP("MCP stdio command binding is invalid")
		}
		for _, arg := range value.Args {
			if !validMCPText(arg) {
				return MCPBinding{}, invalidMCP("MCP stdio argument is invalid")
			}
		}
	default:
		return MCPBinding{}, invalidMCP("unsupported MCP transport")
	}
	return value, nil
}

func normalizeMCPBindings(bindings []MCPBinding) ([]MCPBinding, error) {
	if len(bindings) == 0 {
		return []MCPBinding{}, nil
	}
	normalized := make([]MCPBinding, 0, len(bindings))
	seen := make(map[string]struct{}, len(bindings))
	for _, binding := range bindings {
		value, err := binding.Normalize()
		if err != nil {
			return nil, err
		}
		if _, exists := seen[value.Name]; exists {
			return nil, invalidMCP("duplicate MCP binding %q", value.Name)
		}
		seen[value.Name] = struct{}{}
		normalized = append(normalized, value)
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Name < normalized[j].Name })
	return normalized, nil
}

func cloneMCPBindings(bindings []MCPBinding) []MCPBinding {
	if bindings == nil {
		return nil
	}
	clone := make([]MCPBinding, len(bindings))
	for index, binding := range bindings {
		clone[index] = binding
		clone[index].Args = append([]string(nil), binding.Args...)
		clone[index].ToolAllow = append([]string(nil), binding.ToolAllow...)
		if binding.ToolPolicies != nil {
			clone[index].ToolPolicies = make(map[string]MCPToolPolicy, len(binding.ToolPolicies))
			for name, policy := range binding.ToolPolicies {
				clone[index].ToolPolicies[name] = policy
			}
		}
	}
	return clone
}

func sameMCPBindings(left, right []MCPBinding) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index].Name != right[index].Name || left[index].Transport != right[index].Transport || left[index].ServerURL != right[index].ServerURL || left[index].SecretRef != right[index].SecretRef || left[index].Command != right[index].Command || left[index].TimeoutSeconds != right[index].TimeoutSeconds || !sameStrings(left[index].Args, right[index].Args) || !sameStrings(left[index].ToolAllow, right[index].ToolAllow) || !sameMCPToolPolicies(left[index].ToolPolicies, right[index].ToolPolicies) {
			return false
		}
	}
	return true
}

func normalizeMCPToolPolicies(input map[string]MCPToolPolicy, allowed []string) (map[string]MCPToolPolicy, error) {
	if len(input) == 0 {
		return nil, nil
	}
	allowedSet := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allowedSet[name] = struct{}{}
	}
	policies := make(map[string]MCPToolPolicy, len(input))
	for name, policy := range input {
		name = strings.TrimSpace(name)
		if _, ok := allowedSet[name]; !ok || !validMCPName(name) {
			return nil, invalidMCP("MCP tool policy names must be allowlisted")
		}
		if _, duplicate := policies[name]; duplicate {
			return nil, invalidMCP("duplicate MCP tool policy %q", name)
		}
		policy = MCPToolPolicy(strings.ToLower(strings.TrimSpace(string(policy))))
		if policy == "" {
			policy = MCPToolPolicyAuto
		}
		switch policy {
		case MCPToolPolicyAuto, MCPToolPolicyRequireApproval, MCPToolPolicySkipApproval, MCPToolPolicyDenied:
		default:
			return nil, invalidMCP("unsupported MCP tool policy %q", policy)
		}
		policies[name] = policy
	}
	return policies, nil
}

func sameMCPToolPolicies(left, right map[string]MCPToolPolicy) bool {
	if len(left) != len(right) {
		return false
	}
	for name, policy := range left {
		other, ok := right[name]
		if !ok || other != policy {
			return false
		}
	}
	return true
}

func sameStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func normalizeMCPNames(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if !validMCPName(value) {
			return nil, invalidMCP("MCP tool name is invalid")
		}
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	sort.Strings(result)
	return result, nil
}

func validMCPName(value string) bool {
	if value == "" || len([]rune(value)) > 128 || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if !(char == '-' || char == '_' || char == '.' || char >= '0' && char <= '9' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z') {
			return false
		}
	}
	return true
}

func validMCPText(value string) bool {
	if !utf8.ValidString(value) || len([]rune(value)) > 2048 {
		return false
	}
	for _, char := range value {
		if char < 0x20 || char == 0x7f {
			return false
		}
	}
	return true
}

func validateMCPHTTPURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return invalidMCP("MCP endpoint must be an HTTPS URL without credentials, query, or fragment")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || host == "local" || host == "0.0.0.0" || host == "::" {
		return invalidMCP("MCP endpoint targets a local host")
	}
	if ip := net.ParseIP(host); ip != nil && restrictedMCPIP(ip) {
		return invalidMCP("MCP endpoint targets a restricted address")
	}
	return nil
}

func restrictedMCPIP(ip net.IP) bool {
	return ip == nil || !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsMulticast()
}
