package tool

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
	toolmcp "trpc.group/trpc-go/trpc-agent-go/tool/mcp"
	trpcmcp "trpc.group/trpc-go/trpc-mcp-go"
)

// MCPBinding is the revision-scoped control-plane declaration shared with
// the runtime MCP adapter. It contains no resolved credential or connection.
type MCPBinding = appmodel.MCPBinding

var ErrInvalidMCPBinding = appmodel.ErrInvalidMCPBinding

// NewMCPToolSet materializes one ToolSet from a secret-free binding. HTTP
// transports use the upstream MCP client; stdio uses the platform process
// lifecycle boundary. The binding allowlist is enforced before the ToolSet is
// returned; callers still apply the stable binding namespace before a Runner.
func NewMCPToolSet(ctx context.Context, tenantID string, binding MCPBinding, resolver modelprofile.SecretResolver) (trpctool.ToolSet, error) {
	return newMCPToolSet(ctx, tenantID, binding, resolver, mcpNetworkOptions{resolver: net.DefaultResolver})
}

type mcpHostResolver interface {
	LookupIPAddr(context.Context, string) ([]net.IPAddr, error)
}

type mcpNetworkOptions struct {
	resolver       mcpHostResolver
	requestHandler trpcmcp.HTTPReqHandler
	clientOptions  []trpcmcp.ClientOption
}

func newMCPToolSet(ctx context.Context, tenantID string, binding MCPBinding, resolver modelprofile.SecretResolver, network mcpNetworkOptions) (trpctool.ToolSet, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, fmt.Errorf("%w: active context is required", ErrInvalidMCPBinding)
	}
	value, err := binding.Normalize()
	if err != nil {
		return nil, err
	}
	if value.Transport == "stdio" {
		return newStdioMCPToolSet(ctx, value)
	}
	clientOptions := append([]trpcmcp.ClientOption(nil), network.clientOptions...)
	if value.ServerURL != "" {
		handler := network.requestHandler
		if handler == nil {
			handler, err = newPinnedMCPHTTPHandler(ctx, value.ServerURL, network.resolver)
			if err != nil {
				return nil, err
			}
		}
		clientOptions = append(clientOptions, trpcmcp.WithHTTPReqHandler(handler))
	}
	config := toolmcp.ConnectionConfig{
		Transport: value.Transport, ServerURL: value.ServerURL, Command: value.Command, Args: value.Args,
		Timeout: time.Duration(value.TimeoutSeconds) * time.Second,
	}
	options := []toolmcp.ToolSetOption{toolmcp.WithName(value.Name), toolmcp.WithSessionReconnect(3)}
	if len(value.ToolAllow) > 0 {
		options = append(options, toolmcp.WithToolFilterFunc(mcpIncludeFilter(value.ToolAllow)))
	}
	if value.SecretRef != "" {
		if resolver == nil {
			return nil, fmt.Errorf("%w: secret resolver is required", ErrInvalidMCPBinding)
		}
		secret, err := resolver.Resolve(ctx, modelprofile.SecretScope{TenantID: tenantID, SecretRef: value.SecretRef})
		if err != nil {
			return nil, fmt.Errorf("%w: resolve MCP secret", ErrInvalidMCPBinding)
		}
		options = append(options, toolmcp.WithMCPOptions(append(clientOptions, trpcmcp.WithHTTPHeaders(http.Header{"Authorization": []string{"Bearer " + secret.Value()}}))...))
	} else if len(clientOptions) > 0 {
		options = append(options, toolmcp.WithMCPOptions(clientOptions...))
	}
	set := toolmcp.NewMCPToolSet(config, options...)
	if err := set.Init(ctx); err != nil {
		_ = set.Close()
		return nil, fmt.Errorf("%w: initialize MCP tool set", ErrInvalidMCPBinding)
	}
	return set, nil
}

type pinnedMCPHTTPHandler struct {
	host      string
	port      string
	transport *http.Transport
}

func newPinnedMCPHTTPHandler(ctx context.Context, rawURL string, resolver mcpHostResolver) (*pinnedMCPHTTPHandler, error) {
	if resolver == nil {
		return nil, fmt.Errorf("%w: DNS resolver is required", ErrInvalidMCPBinding)
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse HTTP endpoint", ErrInvalidMCPBinding)
	}
	host := strings.ToLower(parsed.Hostname())
	addresses, err := resolver.LookupIPAddr(ctx, host)
	if err != nil || len(addresses) == 0 {
		return nil, fmt.Errorf("%w: resolve HTTP endpoint", ErrInvalidMCPBinding)
	}
	ips := make([]net.IP, 0, len(addresses))
	for _, address := range addresses {
		ip := address.IP
		if restrictedMCPIP(ip) {
			return nil, fmt.Errorf("%w: HTTP endpoint resolved to a restricted address", ErrInvalidMCPBinding)
		}
		ips = append(ips, append(net.IP(nil), ip...))
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	dialer := &net.Dialer{}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	transport.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		var lastErr error
		for _, ip := range ips {
			connection, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
			if err == nil {
				return connection, nil
			}
			lastErr = err
		}
		return nil, lastErr
	}
	tlsConfig := transport.TLSClientConfig
	if tlsConfig == nil {
		tlsConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		tlsConfig = tlsConfig.Clone()
		if tlsConfig.MinVersion < tls.VersionTLS12 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
	}
	tlsConfig.ServerName = host
	transport.TLSClientConfig = tlsConfig
	return &pinnedMCPHTTPHandler{host: host, port: port, transport: transport}, nil
}

func (handler *pinnedMCPHTTPHandler) Handle(ctx context.Context, client *http.Client, request *http.Request) (*http.Response, error) {
	if handler == nil || request == nil || request.URL == nil || request.URL.Scheme != "https" || strings.ToLower(request.URL.Hostname()) != handler.host {
		return nil, fmt.Errorf("%w: MCP request escaped pinned endpoint", ErrInvalidMCPBinding)
	}
	port := request.URL.Port()
	if port == "" {
		port = "443"
	}
	if port != handler.port {
		return nil, fmt.Errorf("%w: MCP request escaped pinned endpoint port", ErrInvalidMCPBinding)
	}
	timeout := time.Duration(0)
	if client != nil {
		timeout = client.Timeout
	}
	pinnedClient := &http.Client{
		Transport: handler.transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return fmt.Errorf("%w: MCP redirects are forbidden", ErrInvalidMCPBinding)
		},
	}
	return pinnedClient.Do(request.WithContext(ctx))
}

func restrictedMCPIP(ip net.IP) bool {
	return ip == nil || !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsMulticast()
}

// NamespaceMCPToolSet wraps a runner-owned MCP ToolSet with stable
// revision-scoped tool names. The server's tool names remain the allowlist
// source; only the model-visible declaration is namespaced.
func NamespaceMCPToolSet(set trpctool.ToolSet, bindingName string) (trpctool.ToolSet, error) {
	if set == nil || strings.TrimSpace(bindingName) == "" {
		return nil, fmt.Errorf("%w: MCP ToolSet and binding name are required", ErrInvalidMCPBinding)
	}
	bindingName = strings.TrimSpace(bindingName)
	if strings.ContainsAny(bindingName, "\r\n") {
		return nil, fmt.Errorf("%w: MCP binding name is invalid", ErrInvalidMCPBinding)
	}
	return namespacedMCPToolSet{delegate: set, name: "mcp_" + bindingName, prefix: "mcp_" + bindingName + "__"}, nil
}

type namespacedMCPToolSet struct {
	delegate trpctool.ToolSet
	name     string
	prefix   string
}

func (set namespacedMCPToolSet) Name() string { return set.name }
func (set namespacedMCPToolSet) Close() error { return set.delegate.Close() }

func (set namespacedMCPToolSet) Tools(ctx context.Context) []trpctool.Tool {
	candidates := set.delegate.Tools(ctx)
	tools := make([]trpctool.Tool, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil || candidate.Declaration() == nil {
			continue
		}
		declaration := *candidate.Declaration()
		declaration.Name = set.prefix + declaration.Name
		base := namespacedMCPTool{delegate: candidate, declaration: &declaration}
		if callable, ok := candidate.(trpctool.CallableTool); ok {
			if streamable, streamableOK := candidate.(trpctool.StreamableTool); streamableOK {
				tools = append(tools, namespacedMCPStreamableTool{namespacedMCPTool: base, callable: callable, streamable: streamable})
			} else {
				tools = append(tools, namespacedMCPCallableTool{namespacedMCPTool: base, callable: callable})
			}
			continue
		}
		if streamable, ok := candidate.(trpctool.StreamableTool); ok {
			tools = append(tools, namespacedMCPStreamableOnlyTool{namespacedMCPTool: base, streamable: streamable})
			continue
		}
		tools = append(tools, base)
	}
	return tools
}

type namespacedMCPTool struct {
	delegate    trpctool.Tool
	declaration *trpctool.Declaration
}

func (tool namespacedMCPTool) Declaration() *trpctool.Declaration { return tool.declaration }
func (tool namespacedMCPTool) ToolMetadata() trpctool.ToolMetadata {
	return trpctool.MetadataOf(tool.delegate)
}

type namespacedMCPCallableTool struct {
	namespacedMCPTool
	callable trpctool.CallableTool
}

func (tool namespacedMCPCallableTool) Call(ctx context.Context, args []byte) (any, error) {
	return tool.callable.Call(ctx, args)
}

type namespacedMCPStreamableTool struct {
	namespacedMCPTool
	callable   trpctool.CallableTool
	streamable trpctool.StreamableTool
}

func (tool namespacedMCPStreamableTool) Call(ctx context.Context, args []byte) (any, error) {
	return tool.callable.Call(ctx, args)
}

func (tool namespacedMCPStreamableTool) StreamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	return tool.streamable.StreamableCall(ctx, args)
}

type namespacedMCPStreamableOnlyTool struct {
	namespacedMCPTool
	streamable trpctool.StreamableTool
}

func (tool namespacedMCPStreamableOnlyTool) StreamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	return tool.streamable.StreamableCall(ctx, args)
}

// WrapMCPToolSetLifecycle adds one bounded reconnect trigger around the
// upstream ToolSet. The upstream stdio transport can observe a child process
// exit as "transport closed" while its session manager still retains the old
// client; closing that client first makes the upstream reconnect path
// deterministic on the retry. Context cancellation and deadlines are never
// retried.
func WrapMCPToolSetLifecycle(set trpctool.ToolSet) (trpctool.ToolSet, error) {
	if set == nil || strings.TrimSpace(set.Name()) == "" {
		return nil, fmt.Errorf("%w: MCP ToolSet is required", ErrInvalidMCPBinding)
	}
	return resilientMCPToolSet{delegate: set}, nil
}

type resilientMCPToolSet struct {
	delegate trpctool.ToolSet
}

func (set resilientMCPToolSet) Name() string { return set.delegate.Name() }
func (set resilientMCPToolSet) Close() error {
	if err := set.delegate.Close(); err != nil && !mcpBenignLifecycleError(err) {
		return err
	}
	return nil
}

func (set resilientMCPToolSet) Tools(ctx context.Context) []trpctool.Tool {
	candidates := set.delegate.Tools(ctx)
	tools := make([]trpctool.Tool, 0, len(candidates))
	for _, candidate := range candidates {
		callable, ok := candidate.(trpctool.CallableTool)
		if !ok {
			tools = append(tools, candidate)
			continue
		}
		tools = append(tools, resilientMCPTool{delegate: callable, owner: set.delegate})
	}
	return tools
}

type resilientMCPTool struct {
	delegate trpctool.CallableTool
	owner    trpctool.ToolSet
}

func (tool resilientMCPTool) Declaration() *trpctool.Declaration { return tool.delegate.Declaration() }
func (tool resilientMCPTool) ToolMetadata() trpctool.ToolMetadata {
	return trpctool.MetadataOf(tool.delegate)
}

func (tool resilientMCPTool) Call(ctx context.Context, args []byte) (any, error) {
	result, err := tool.delegate.Call(ctx, args)
	if err == nil || ctx == nil || ctx.Err() != nil || !mcpConnectionFailure(err) {
		return result, err
	}
	_ = tool.owner.Close()
	return tool.delegate.Call(ctx, args)
}

func mcpBenignLifecycleError(err error) bool {
	if err == nil {
		return true
	}
	value := strings.ToLower(err.Error())
	return strings.Contains(value, "file already closed") || strings.Contains(value, "process already finished") || strings.Contains(value, "os: process already finished")
}

// GovernMCPToolSet applies execution-time authorization to every MCP tool.
// Binding allowlists decide which tools can be advertised; this boundary also
// checks the tenant execution context, per-tool approval policy, reviewer, and
// request-local budget immediately before the remote call.
func GovernMCPToolSet(ctx context.Context, set trpctool.ToolSet, tenantID string, binding MCPBinding, reviewer review.Reviewer) (trpctool.ToolSet, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, fmt.Errorf("%w: active context is required", ErrInvalidMCPBinding)
	}
	if set == nil || strings.TrimSpace(tenantID) == "" {
		return nil, fmt.Errorf("%w: MCP ToolSet and tenant are required", ErrInvalidMCPBinding)
	}
	value, err := binding.Normalize()
	if err != nil {
		return nil, err
	}
	governed := governedMCPToolSet{delegate: set, tenantID: strings.TrimSpace(tenantID), binding: value, reviewer: reviewer}
	for _, candidate := range governed.Tools(ctx) {
		if candidate == nil || candidate.Declaration() == nil {
			return nil, fmt.Errorf("%w: MCP tool declaration is invalid", ErrInvalidMCPBinding)
		}
		if mcpToolDecision(value, candidate.Declaration().Name, trpctool.MetadataOf(candidate)) == ApprovalRequired && reviewer == nil {
			return nil, fmt.Errorf("%w: reviewer is required for MCP tool %q", ErrApprovalRequired, candidate.Declaration().Name)
		}
	}
	return governed, nil
}

type governedMCPToolSet struct {
	delegate trpctool.ToolSet
	tenantID string
	binding  MCPBinding
	reviewer review.Reviewer
}

func (set governedMCPToolSet) Name() string { return set.delegate.Name() }
func (set governedMCPToolSet) Close() error { return set.delegate.Close() }

func (set governedMCPToolSet) Tools(ctx context.Context) []trpctool.Tool {
	candidates := set.delegate.Tools(ctx)
	tools := make([]trpctool.Tool, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		callable, ok := candidate.(trpctool.CallableTool)
		if !ok {
			tools = append(tools, candidate)
			continue
		}
		tools = append(tools, governedMCPTool{delegate: callable, owner: set})
	}
	return tools
}

type governedMCPTool struct {
	delegate trpctool.CallableTool
	owner    governedMCPToolSet
}

func (tool governedMCPTool) Declaration() *trpctool.Declaration { return tool.delegate.Declaration() }
func (tool governedMCPTool) ToolMetadata() trpctool.ToolMetadata {
	return trpctool.MetadataOf(tool.delegate)
}

func (tool governedMCPTool) Call(ctx context.Context, args []byte) (any, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, contextError(ctx)
	}
	execution, err := mcpExecutionContextFromContext(ctx)
	if err != nil || execution.TenantID != tool.owner.tenantID || execution.AppID == "" || execution.UserID == "" || execution.SessionID == "" {
		return nil, ErrMCPExecutionUnavailable
	}
	if len(args) > 1<<20 || !json.Valid(args) && len(strings.TrimSpace(string(args))) > 0 {
		return nil, ErrMCPInvalidArguments
	}
	name := tool.Declaration().Name
	decision := mcpToolDecision(tool.owner.binding, name, trpctool.MetadataOf(tool.delegate))
	policy := Policy{Recorder: execution.Audit, Allowed: map[string]Decision{name: decision}}
	if _, err := policy.Decide(ctx, execution.RequestID, execution.TraceID, name); err != nil {
		if !errors.Is(err, ErrApprovalRequired) {
			return nil, err
		}
		if tool.owner.reviewer == nil {
			return nil, ErrApprovalRequired
		}
		reviewDecision, reviewErr := tool.owner.reviewer.Review(ctx, &review.Request{Action: review.Action{
			ToolName: name, ToolDescription: tool.Declaration().Description, Arguments: append(json.RawMessage(nil), args...),
		}})
		if reviewErr != nil || reviewDecision == nil || !reviewDecision.Approved {
			return nil, ErrApprovalDenied
		}
		allowPolicy := Policy{Recorder: execution.Audit, Allowed: map[string]Decision{name: Allow}}
		if _, err := allowPolicy.Decide(ctx, execution.RequestID, execution.TraceID, name); err != nil {
			return nil, err
		}
	}
	if execution.ToolBudget != nil {
		if err := execution.ToolBudget.Consume(); err != nil {
			_ = execution.Audit.BudgetRejected(ctx, execution.RequestID, execution.TraceID)
			return nil, err
		}
	}
	result, callErr := tool.delegate.Call(ctx, args)
	if callErr != nil {
		return nil, redactedToolError(callErr)
	}
	if err := execution.Audit.ToolExecuted(ctx, execution.RequestID, execution.TraceID, name); err != nil {
		return nil, err
	}
	return result, nil
}

func mcpExecutionContextFromContext(ctx context.Context) (ExecutionContext, error) {
	if ctx == nil {
		return ExecutionContext{}, ErrMCPExecutionUnavailable
	}
	execution, ok := ctx.Value(executionContextKey{}).(ExecutionContext)
	if !ok || execution.TenantID == "" || execution.EventID == "" || execution.RequestID == "" || execution.TraceID == "" {
		return ExecutionContext{}, ErrMCPExecutionUnavailable
	}
	return execution, nil
}

func mcpToolDecision(binding MCPBinding, publicName string, metadata trpctool.ToolMetadata) Decision {
	remoteName := publicName
	prefix := "mcp_" + binding.Name + "__"
	if strings.HasPrefix(remoteName, prefix) {
		remoteName = strings.TrimPrefix(remoteName, prefix)
	}
	switch binding.ToolPolicies[remoteName] {
	case appmodel.MCPToolPolicyDenied:
		return Deny
	case appmodel.MCPToolPolicyRequireApproval:
		return ApprovalRequired
	case appmodel.MCPToolPolicySkipApproval:
		return Allow
	}
	if metadata.ReadOnly && !metadata.Destructive {
		return Allow
	}
	return ApprovalRequired
}

func mcpConnectionFailure(err error) bool {
	if err == nil {
		return false
	}
	value := strings.ToLower(err.Error())
	for _, marker := range []string{"transport closed", "transport is closed", "broken pipe", "connection reset", "file already closed", "eof", "process already finished", "client not initialized", "session not found"} {
		if strings.Contains(value, marker) {
			return true
		}
	}
	return false
}

func mcpIncludeFilter(allowed []string) trpctool.FilterFunc {
	allow := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allow[name] = struct{}{}
	}
	return func(_ context.Context, candidate trpctool.Tool) bool {
		if candidate == nil || candidate.Declaration() == nil {
			return false
		}
		_, ok := allow[candidate.Declaration().Name]
		return ok
	}
}
