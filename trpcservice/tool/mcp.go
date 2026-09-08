package tool

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
	toolmcp "trpc.group/trpc-go/trpc-agent-go/tool/mcp"
	trpcmcp "trpc.group/trpc-go/trpc-mcp-go"
)

// MCPBinding is the revision-scoped control-plane declaration shared with
// the runtime MCP adapter. It contains no resolved credential or connection.
type MCPBinding = appmodel.MCPBinding

var ErrInvalidMCPBinding = appmodel.ErrInvalidMCPBinding

// NewMCPToolSet materializes one upstream ToolSet. It is intentionally
// separate from the factory: callers must still apply Revision allowlists to
// the returned, prefixed tool names before passing it to a Runner.
func NewMCPToolSet(ctx context.Context, tenantID string, binding MCPBinding, resolver modelprofile.SecretResolver) (*toolmcp.ToolSet, error) {
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

func newMCPToolSet(ctx context.Context, tenantID string, binding MCPBinding, resolver modelprofile.SecretResolver, network mcpNetworkOptions) (*toolmcp.ToolSet, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, fmt.Errorf("%w: active context is required", ErrInvalidMCPBinding)
	}
	value, err := binding.Normalize()
	if err != nil {
		return nil, err
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
	config := toolmcp.ConnectionConfig{Transport: value.Transport, ServerURL: value.ServerURL, Command: value.Command, Args: value.Args}
	options := []toolmcp.ToolSetOption{toolmcp.WithName(value.Name)}
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
