package tool

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/jsonstrict"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
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

const (
	maxMCPWireBytes       int64 = 8 << 20
	maxMCPHTTPHeaderBytes int64 = 64 << 10
)

var (
	errMCPWireTooLarge    = errors.New("MCP message exceeds size limit")
	errMCPWireInvalidUTF8 = errors.New("MCP message is not valid UTF-8")
	mcpRestrictedNetworks = mustMCPRestrictedNetworks()
)

func isNilMCPValue(value any) bool { return nilvalue.Is(value) }

func mcpContextErr(ctx context.Context) error {
	if isNilMCPValue(ctx) {
		return ErrInvalidMCPBinding
	}
	return ctx.Err()
}

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

func lookupMCPHTTPAddresses(ctx context.Context, resolver mcpHostResolver, host string) (addresses []net.IPAddr, err error) {
	if isNilMCPValue(ctx) || isNilMCPValue(resolver) {
		return nil, ErrInvalidMCPBinding
	}
	defer func() {
		if recover() != nil {
			addresses = nil
			err = ErrInvalidMCPBinding
		}
	}()
	return resolver.LookupIPAddr(ctx, host)
}

func resolveMCPSecret(ctx context.Context, resolver modelprofile.SecretResolver, scope modelprofile.SecretScope) (secret modelprofile.SecretValue, err error) {
	if isNilMCPValue(ctx) || isNilMCPValue(resolver) {
		return modelprofile.SecretValue{}, ErrInvalidMCPBinding
	}
	defer func() {
		if recover() != nil {
			secret = modelprofile.SecretValue{}
			err = ErrInvalidMCPBinding
		}
	}()
	return resolver.Resolve(ctx, scope)
}

func newMCPToolSet(ctx context.Context, tenantID string, binding MCPBinding, resolver modelprofile.SecretResolver, network mcpNetworkOptions) (trpctool.ToolSet, error) {
	if err := mcpContextErr(ctx); err != nil {
		return nil, fmt.Errorf("%w: active context is required", ErrInvalidMCPBinding)
	}
	if !validMCPTenantID(tenantID) {
		return nil, fmt.Errorf("%w: tenant scope is invalid or not normalized", ErrInvalidMCPBinding)
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
		resolveCtx, cancelResolve := context.WithTimeout(ctx, time.Duration(value.TimeoutSeconds)*time.Second)
		endpoint, endpointErr := newPinnedMCPHTTPHandler(resolveCtx, value.ServerURL, network.resolver)
		cancelResolve()
		if endpointErr != nil {
			return nil, endpointErr
		}
		if err := mcpContextErr(ctx); err != nil {
			return nil, err
		}
		endpoint.allowSSEEndpoint = value.Transport == "sse"
		if isNilMCPValue(handler) {
			handler = endpoint
		} else {
			// Test/in-process handlers still pass through the same request and
			// response boundary. Production callers cannot replace the pinned
			// transport because this option is intentionally unexported.
			handler = &validatedMCPHTTPHandler{endpoint: endpoint, delegate: handler}
		}
		clientOptions = append(clientOptions, trpcmcp.WithHTTPReqHandler(handler))
	}
	config := toolmcp.ConnectionConfig{
		Transport: value.Transport, ServerURL: value.ServerURL, Command: value.Command, Args: value.Args,
		Timeout: time.Duration(value.TimeoutSeconds) * time.Second,
	}
	// Do not enable the upstream session-reconnect option. Its retry loop
	// replays tools/call after a transport error, but a transport error can
	// happen after a provider has accepted a side effect. Reconnection is
	// limited to materialization and to a later explicit tool invocation.
	options := []toolmcp.ToolSetOption{toolmcp.WithName(value.Name)}
	if len(value.ToolAllow) > 0 {
		options = append(options, toolmcp.WithToolFilterFunc(mcpIncludeFilter(value.ToolAllow)))
	}
	if value.SecretRef != "" {
		if isNilMCPValue(resolver) {
			return nil, fmt.Errorf("%w: secret resolver is required", ErrInvalidMCPBinding)
		}
		secret, err := resolveMCPSecret(ctx, resolver, modelprofile.SecretScope{TenantID: tenantID, SecretRef: value.SecretRef})
		if err != nil || !validMCPSecretValue(secret.Value()) {
			return nil, fmt.Errorf("%w: resolve MCP secret", ErrInvalidMCPBinding)
		}
		if contextErr := mcpContextErr(ctx); contextErr != nil {
			return nil, contextErr
		}
		options = append(options, toolmcp.WithMCPOptions(append(clientOptions, trpcmcp.WithHTTPHeaders(http.Header{"Authorization": []string{"Bearer " + secret.Value()}}))...))
	} else if len(clientOptions) > 0 {
		options = append(options, toolmcp.WithMCPOptions(clientOptions...))
	}
	set, err := newUpstreamMCPToolSet(config, options...)
	if err != nil {
		return nil, fmt.Errorf("%w: construct MCP tool set", ErrInvalidMCPBinding)
	}
	if err := initUpstreamMCPToolSet(ctx, set); err != nil {
		_ = safeCloseMCPToolSet(set)
		return nil, fmt.Errorf("%w: initialize MCP tool set", ErrInvalidMCPBinding)
	}
	return set, nil
}

func newUpstreamMCPToolSet(config toolmcp.ConnectionConfig, options ...toolmcp.ToolSetOption) (set *toolmcp.ToolSet, err error) {
	defer func() {
		if recover() != nil {
			set = nil
			err = ErrInvalidMCPBinding
		}
	}()
	return toolmcp.NewMCPToolSet(config, options...), nil
}

func initUpstreamMCPToolSet(ctx context.Context, set *toolmcp.ToolSet) (err error) {
	if isNilMCPValue(ctx) || isNilMCPValue(set) {
		return ErrInvalidMCPBinding
	}
	defer func() {
		if recover() != nil {
			err = ErrInvalidMCPBinding
		}
	}()
	return set.Init(ctx)
}

type pinnedMCPHTTPHandler struct {
	host             string
	port             string
	path             string
	baseURL          *url.URL
	transport        *http.Transport
	allowSSEEndpoint bool
	mu               sync.RWMutex
	sseEndpointPaths map[string]struct{}
}

func validateMCPHTTPURL(raw string) error {
	if !utf8.ValidString(raw) {
		return fmt.Errorf("%w: MCP endpoint is not valid UTF-8", ErrInvalidMCPBinding)
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" {
		return fmt.Errorf("%w: MCP endpoint must be an HTTPS URL without credentials, query, force query, fragment, or opaque path", ErrInvalidMCPBinding)
	}
	if !validMCPHTTPQuery(parsed.RawQuery) {
		return fmt.Errorf("%w: MCP endpoint query is invalid", ErrInvalidMCPBinding)
	}
	if _, ok := mcpEscapedPath(parsed); !ok {
		return fmt.Errorf("%w: MCP endpoint path is invalid", ErrInvalidMCPBinding)
	}
	host := strings.ToLower(parsed.Hostname())
	if !validMCPHTTPHostname(host) {
		return fmt.Errorf("%w: MCP endpoint host is invalid", ErrInvalidMCPBinding)
	}
	if strings.Contains(host, "%") {
		return fmt.Errorf("%w: MCP endpoint host zone is not allowed", ErrInvalidMCPBinding)
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || host == "local" || host == "0.0.0.0" || host == "::" {
		return fmt.Errorf("%w: MCP endpoint targets a local host", ErrInvalidMCPBinding)
	}
	if ip := net.ParseIP(host); ip != nil && restrictedMCPIP(ip) {
		return fmt.Errorf("%w: MCP endpoint targets a restricted address", ErrInvalidMCPBinding)
	}
	return nil
}

func newPinnedMCPHTTPHandler(ctx context.Context, rawURL string, resolver mcpHostResolver) (*pinnedMCPHTTPHandler, error) {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil, fmt.Errorf("%w: active context is required", ErrInvalidMCPBinding)
	}
	if isNilMCPValue(resolver) {
		return nil, fmt.Errorf("%w: DNS resolver is required", ErrInvalidMCPBinding)
	}
	if err := validateMCPHTTPURL(rawURL); err != nil {
		return nil, err
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: parse HTTP endpoint", ErrInvalidMCPBinding)
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" {
		return nil, fmt.Errorf("%w: HTTP endpoint host is required", ErrInvalidMCPBinding)
	}
	addresses, err := lookupMCPHTTPAddresses(ctx, resolver, host)
	if err != nil || len(addresses) == 0 {
		return nil, fmt.Errorf("%w: resolve HTTP endpoint", ErrInvalidMCPBinding)
	}
	ips := make([]net.IP, 0, len(addresses))
	for _, address := range addresses {
		if address.Zone != "" {
			return nil, fmt.Errorf("%w: HTTP endpoint resolved with a zone identifier", ErrInvalidMCPBinding)
		}
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
	if !validMCPHTTPPort(port) {
		return nil, fmt.Errorf("%w: HTTP endpoint port is invalid", ErrInvalidMCPBinding)
	}
	dialer := &net.Dialer{}
	// Construct a fresh transport rather than cloning the process-global
	// DefaultTransport. A caller can mutate DefaultTransport, including its
	// proxy and TLS settings, before this materialization boundary.
	transport := &http.Transport{
		Proxy:                  nil,
		DisableKeepAlives:      true,
		MaxResponseHeaderBytes: 64 << 10,
		TLSClientConfig:        &tls.Config{MinVersion: tls.VersionTLS12, ServerName: host},
	}
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
	path, _ := mcpEscapedPath(parsed)
	return &pinnedMCPHTTPHandler{host: host, port: port, path: path, baseURL: parsed, transport: transport, sseEndpointPaths: map[string]struct{}{}}, nil
}

func validMCPHTTPHostname(value string) bool {
	if value == "" || !utf8.ValidString(value) || len(value) > 253 || strings.HasSuffix(value, ".") {
		return false
	}
	if ip := net.ParseIP(value); ip != nil {
		return true
	}
	for _, label := range strings.Split(value, ".") {
		if label == "" || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, character := range label {
			if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
				return false
			}
		}
	}
	return true
}

func validMCPHTTPPort(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > 1 && value[0] == '0' {
		return false
	}
	var port uint64
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
		port = port*10 + uint64(character-'0')
		if port > 65535 {
			return false
		}
	}
	return port > 0
}

func mcpEscapedPath(value *url.URL) (string, bool) {
	if value == nil {
		return "", false
	}
	path := value.EscapedPath()
	if path == "" {
		path = "/"
	}
	decoded, err := url.PathUnescape(path)
	if err != nil || !utf8.ValidString(path) || !utf8.ValidString(decoded) || !strings.HasPrefix(path, "/") || len(path) > 4096 || strings.IndexFunc(decoded, unicode.IsControl) >= 0 || strings.Contains(decoded, "\\") {
		return "", false
	}
	for _, segment := range strings.Split(decoded, "/") {
		if segment == "." || segment == ".." {
			return "", false
		}
	}
	return path, true
}

func validMCPHTTPQuery(value string) bool {
	if value == "" {
		return true
	}
	if !utf8.ValidString(value) || int64(len(value)) > maxMCPHTTPHeaderBytes || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return false
	}
	parsed, err := url.ParseQuery(value)
	if err != nil {
		return false
	}
	for key, values := range parsed {
		if key == "" || !utf8.ValidString(key) || strings.IndexFunc(key, unicode.IsControl) >= 0 {
			return false
		}
		for _, item := range values {
			if !utf8.ValidString(item) || strings.IndexFunc(item, unicode.IsControl) >= 0 {
				return false
			}
		}
	}
	return true
}

func (handler *pinnedMCPHTTPHandler) validateRequest(ctx context.Context, request *http.Request) error {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return fmt.Errorf("%w: active context is required", ErrInvalidMCPBinding)
	}
	if handler == nil || request == nil || request.URL == nil || request.URL.Scheme != "https" || strings.ToLower(request.URL.Hostname()) != handler.host || request.URL.User != nil || request.URL.ForceQuery || request.URL.Fragment != "" || request.URL.Opaque != "" {
		return fmt.Errorf("%w: MCP request escaped pinned endpoint", ErrInvalidMCPBinding)
	}
	if request.Host != "" && !strings.EqualFold(request.Host, request.URL.Host) {
		return fmt.Errorf("%w: MCP request host header escaped pinned endpoint", ErrInvalidMCPBinding)
	}
	if !validMCPHTTPQuery(request.URL.RawQuery) {
		return fmt.Errorf("%w: MCP request query is invalid", ErrInvalidMCPBinding)
	}
	path, ok := mcpEscapedPath(request.URL)
	if !ok {
		return fmt.Errorf("%w: MCP request path is invalid", ErrInvalidMCPBinding)
	}
	if path != handler.path && !handler.ssePathAllowed(request.Method, path) {
		return fmt.Errorf("%w: MCP request escaped pinned endpoint path", ErrInvalidMCPBinding)
	}
	port := request.URL.Port()
	if port == "" {
		port = "443"
	}
	if !validMCPHTTPPort(port) || port != handler.port {
		return fmt.Errorf("%w: MCP request escaped pinned endpoint port", ErrInvalidMCPBinding)
	}
	if request.ContentLength > maxMCPWireBytes {
		return errMCPWireTooLarge
	}
	return nil
}

func limitMCPHTTPRequest(request *http.Request) error {
	if request == nil {
		return fmt.Errorf("%w: MCP request is required", ErrInvalidMCPBinding)
	}
	if request.ContentLength > maxMCPWireBytes {
		return errMCPWireTooLarge
	}
	if err := validateMCPHTTPHeaders(request.Header); err != nil {
		return err
	}
	if request.Host != "" && !validMCPHTTPHeaderValue(request.Host) {
		return fmt.Errorf("%w: MCP request host is invalid", ErrInvalidMCPBinding)
	}
	if request.Body != nil {
		request.Body = &limitedMCPBody{reader: &io.LimitedReader{R: request.Body, N: maxMCPWireBytes}, closer: request.Body}
	}
	return nil
}

func (handler *pinnedMCPHTTPHandler) Handle(ctx context.Context, client *http.Client, request *http.Request) (response *http.Response, err error) {
	defer func() {
		if recover() != nil {
			if response != nil && response.Body != nil {
				_ = closeMCPCloser(response.Body)
			}
			response = nil
			err = fmt.Errorf("%w: MCP HTTP transport failed", ErrInvalidMCPBinding)
		}
	}()
	if handler == nil || handler.transport == nil {
		return nil, fmt.Errorf("%w: MCP HTTP transport is unavailable", ErrInvalidMCPBinding)
	}
	if err := handler.validateRequest(ctx, request); err != nil {
		return nil, err
	}
	if err := limitMCPHTTPRequest(request); err != nil {
		return nil, err
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
	response, err = pinnedClient.Do(request.WithContext(ctx))
	if err != nil {
		return nil, err
	}
	if response == nil || response.Request == nil {
		if response != nil && response.Body != nil {
			_ = closeMCPCloser(response.Body)
		}
		return nil, errMCPWireTooLarge
	}
	if err := handler.validateRequest(ctx, response.Request); err != nil {
		if response.Body != nil {
			_ = closeMCPCloser(response.Body)
		}
		return nil, err
	}
	if err := limitMCPHTTPResponseWithEndpoint(response, handler.observeSSEEndpoint); err != nil {
		return nil, err
	}
	return response, nil
}

func (handler *pinnedMCPHTTPHandler) ssePathAllowed(method, path string) bool {
	if handler == nil || !handler.allowSSEEndpoint || method != http.MethodPost {
		return false
	}
	handler.mu.RLock()
	_, ok := handler.sseEndpointPaths[path]
	handler.mu.RUnlock()
	return ok
}

func (handler *pinnedMCPHTTPHandler) observeSSEEndpoint(value string) {
	if handler == nil || !handler.allowSSEEndpoint || handler.baseURL == nil {
		return
	}
	if !utf8.ValidString(value) || strings.IndexFunc(value, unicode.IsControl) >= 0 {
		return
	}
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.User != nil || parsed.ForceQuery || parsed.Fragment != "" || parsed.Opaque != "" || !validMCPHTTPQuery(parsed.RawQuery) {
		return
	}
	if !parsed.IsAbs() {
		parsed = handler.baseURL.ResolveReference(parsed)
	}
	if parsed.Scheme != "https" || !strings.EqualFold(parsed.Hostname(), handler.host) {
		return
	}
	port := parsed.Port()
	if port == "" {
		port = "443"
	}
	if port != handler.port {
		return
	}
	path, ok := mcpEscapedPath(parsed)
	if !ok {
		return
	}
	handler.mu.Lock()
	if handler.sseEndpointPaths == nil {
		handler.sseEndpointPaths = make(map[string]struct{})
	}
	handler.sseEndpointPaths[path] = struct{}{}
	handler.mu.Unlock()
}

// validatedMCPHTTPHandler is only used by package-local test transports. It
// keeps their deterministic request handling from bypassing URL, host, size,
// redirect, and response validation at this package boundary.
type validatedMCPHTTPHandler struct {
	endpoint *pinnedMCPHTTPHandler
	delegate trpcmcp.HTTPReqHandler
}

func callMCPHTTPHandler(ctx context.Context, client *http.Client, handler trpcmcp.HTTPReqHandler, request *http.Request) (response *http.Response, err error) {
	if isNilMCPValue(ctx) || isNilMCPValue(handler) {
		return nil, fmt.Errorf("%w: MCP HTTP handler is unavailable", ErrInvalidMCPBinding)
	}
	defer func() {
		if recover() != nil {
			if response != nil && response.Body != nil {
				_ = closeMCPCloser(response.Body)
			}
			response = nil
			err = fmt.Errorf("%w: MCP HTTP handler failed", ErrInvalidMCPBinding)
		}
		if err != nil && response != nil && response.Body != nil {
			_ = closeMCPCloser(response.Body)
			response = nil
		}
	}()
	return handler.Handle(ctx, client, request)
}

func (handler *validatedMCPHTTPHandler) Handle(ctx context.Context, client *http.Client, request *http.Request) (*http.Response, error) {
	if handler == nil || handler.endpoint == nil || isNilMCPValue(handler.delegate) {
		return nil, fmt.Errorf("%w: MCP HTTP handler is unavailable", ErrInvalidMCPBinding)
	}
	if err := handler.endpoint.validateRequest(ctx, request); err != nil {
		return nil, err
	}
	if err := limitMCPHTTPRequest(request); err != nil {
		return nil, err
	}
	response, err := callMCPHTTPHandler(ctx, client, handler.delegate, request)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = closeMCPCloser(response.Body)
		}
		return nil, err
	}
	// A package-local handler is allowed only as a deterministic test seam. It
	// still cannot mutate the request to another origin or return a response
	// that claims to have followed one. Production uses pinnedMCPHTTPHandler
	// directly and never exposes this delegate hook.
	if err := handler.endpoint.validateRequest(ctx, request); err != nil {
		if response != nil && response.Body != nil {
			_ = closeMCPCloser(response.Body)
		}
		return nil, err
	}
	if response == nil || response.Request == nil {
		if response != nil && response.Body != nil {
			_ = closeMCPCloser(response.Body)
		}
		return nil, fmt.Errorf("%w: MCP response origin is unavailable", ErrInvalidMCPBinding)
	}
	if err := handler.endpoint.validateRequest(ctx, response.Request); err != nil {
		if response.Body != nil {
			_ = closeMCPCloser(response.Body)
		}
		return nil, err
	}
	if response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest {
		if response.Body != nil {
			_ = closeMCPCloser(response.Body)
		}
		return nil, fmt.Errorf("%w: MCP redirects are forbidden", ErrInvalidMCPBinding)
	}
	if err := limitMCPHTTPResponseWithEndpoint(response, handler.endpoint.observeSSEEndpoint); err != nil {
		return nil, err
	}
	return response, nil
}

func validateMCPHTTPHeaders(headers http.Header) error {
	var size int64
	for name, values := range headers {
		if !validMCPHTTPHeaderName(name) {
			return fmt.Errorf("%w: MCP HTTP header name is invalid", ErrInvalidMCPBinding)
		}
		size += int64(len(name) + 2)
		if len(values) == 0 {
			size += 2
		}
		for _, value := range values {
			if !validMCPHTTPHeaderValue(value) {
				return fmt.Errorf("%w: MCP HTTP header value is invalid", ErrInvalidMCPBinding)
			}
			size += int64(len(value) + 2)
		}
		if size > maxMCPHTTPHeaderBytes {
			return errMCPWireTooLarge
		}
	}
	return nil
}

func validMCPHTTPHeaderName(value string) bool {
	if value == "" || !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", char)) {
			return false
		}
	}
	return true
}

func validMCPHTTPHeaderValue(value string) bool {
	if !utf8.ValidString(value) {
		return false
	}
	for _, char := range value {
		if unicode.IsControl(char) && char != '\t' {
			return false
		}
	}
	return true
}

type mcpUTF8Validator struct {
	pending []byte
}

func (validator *mcpUTF8Validator) accept(value []byte) bool {
	if validator == nil {
		return false
	}
	data := value
	if len(validator.pending) > 0 {
		data = make([]byte, 0, len(validator.pending)+len(value))
		data = append(data, validator.pending...)
		data = append(data, value...)
		validator.pending = nil
	}
	for index := 0; index < len(data); {
		if !utf8.FullRune(data[index:]) {
			validator.pending = append(validator.pending[:0], data[index:]...)
			return true
		}
		runeValue, size := utf8.DecodeRune(data[index:])
		if runeValue == utf8.RuneError && size == 1 {
			return false
		}
		index += size
	}
	return true
}

func (validator *mcpUTF8Validator) complete() bool {
	return validator != nil && len(validator.pending) == 0
}

type limitedMCPBody struct {
	reader    *io.LimitedReader
	closer    io.Closer
	validator mcpUTF8Validator
}

func (body *limitedMCPBody) Read(p []byte) (n int, err error) {
	defer func() {
		if recover() != nil {
			n = 0
			err = errMCPWireTooLarge
		}
	}()
	if len(p) == 0 {
		return 0, nil
	}
	if body == nil || body.reader == nil || body.reader.R == nil {
		return 0, errMCPWireTooLarge
	}
	if body.reader.N <= 0 {
		// Probe the underlying stream once at the boundary. Returning an error
		// merely because exactly maxMCPWireBytes were read would reject valid
		// payloads at the limit; a byte beyond the limit is the actual overflow.
		var probe [1]byte
		n, err := body.reader.R.Read(probe[:])
		if n > 0 {
			return 0, errMCPWireTooLarge
		}
		if err == io.EOF {
			if !body.validator.complete() {
				return 0, errMCPWireInvalidUTF8
			}
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		return 0, io.ErrNoProgress
	}
	n, err = body.reader.Read(p)
	if n > 0 && !body.validator.accept(p[:n]) {
		return n, errMCPWireInvalidUTF8
	}
	if err == io.EOF && !body.validator.complete() {
		return n, errMCPWireInvalidUTF8
	}
	return n, err
}

func (body *limitedMCPBody) Close() error {
	if body == nil || body.closer == nil {
		return nil
	}
	return closeMCPCloser(body.closer)
}

func limitMCPHTTPResponse(response *http.Response) error {
	return limitMCPHTTPResponseWithEndpoint(response, nil)
}

func limitMCPHTTPResponseWithEndpoint(response *http.Response, endpointCallback func(string)) error {
	if response == nil {
		return errMCPWireTooLarge
	}
	closeResponse := func() {
		if response.Body != nil {
			_ = closeMCPCloser(response.Body)
		}
	}
	if response.Body == nil || response.StatusCode < 100 || response.StatusCode > 599 {
		closeResponse()
		return errMCPWireTooLarge
	}
	if err := validateMCPHTTPHeaders(response.Header); err != nil {
		closeResponse()
		return err
	}
	if response.StatusCode >= http.StatusMultipleChoices && response.StatusCode < http.StatusBadRequest {
		closeResponse()
		return fmt.Errorf("%w: MCP redirects are forbidden", ErrInvalidMCPBinding)
	}
	if response.ContentLength > maxMCPWireBytes {
		closeResponse()
		return errMCPWireTooLarge
	}
	contentType := strings.ToLower(response.Header.Get("Content-Type"))
	if strings.Contains(contentType, "text/event-stream") {
		response.Body = &limitedMCPSSEBody{reader: response.Body, endpointCallback: endpointCallback}
	} else {
		response.Body = &limitedMCPBody{reader: &io.LimitedReader{R: response.Body, N: maxMCPWireBytes}, closer: response.Body}
	}
	return nil
}

type limitedMCPSSEBody struct {
	reader           io.ReadCloser
	lineBytes        int64
	eventBytes       int64
	totalBytes       int64
	line             []byte
	eventType        string
	eventData        string
	endpointCallback func(string)
	validator        mcpUTF8Validator
	finished         bool
}

func (body *limitedMCPSSEBody) Read(p []byte) (n int, err error) {
	defer func() {
		if recover() != nil {
			n = 0
			err = errMCPWireTooLarge
		}
	}()
	if len(p) == 0 {
		return 0, nil
	}
	if body == nil || body.reader == nil {
		return 0, errMCPWireTooLarge
	}
	if body.finished {
		return 0, io.EOF
	}
	readBuffer := p
	if remaining := maxMCPWireBytes - body.totalBytes; remaining <= 0 {
		var probe [1]byte
		n, err := body.reader.Read(probe[:])
		if n > 0 {
			return 0, errMCPWireTooLarge
		}
		if err == io.EOF {
			if !body.validator.complete() {
				return 0, errMCPWireInvalidUTF8
			}
			body.processLine()
			body.finishEvent()
			body.finished = true
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		return 0, io.ErrNoProgress
	} else if int64(len(readBuffer)) > remaining {
		readBuffer = readBuffer[:remaining]
	}
	n, err = body.reader.Read(readBuffer)
	if n > 0 && !body.validator.accept(readBuffer[:n]) {
		return n, errMCPWireInvalidUTF8
	}
	body.totalBytes += int64(n)
	for _, value := range readBuffer[:n] {
		if value == '\n' {
			blank := len(body.line) == 0
			body.processLine()
			body.lineBytes = 0
			// A blank line terminates the current SSE event. The event size
			// is reset only after the terminating blank line, not after every
			// data line.
			if blank {
				body.finishEvent()
				body.eventBytes = 0
			}
			continue
		}
		if value == '\r' {
			// The upstream parser treats CR as a line suffix. It is still
			// counted by totalBytes, while the logical line limit excludes it.
			continue
		}
		body.line = append(body.line, value)
		body.lineBytes++
		body.eventBytes++
		if body.lineBytes > maxMCPWireBytes || body.eventBytes > maxMCPWireBytes {
			return n, errMCPWireTooLarge
		}
	}
	if err == io.EOF {
		if !body.validator.complete() {
			return n, errMCPWireInvalidUTF8
		}
		body.processLine()
		body.finishEvent()
		body.finished = true
	}
	return n, err
}

func (body *limitedMCPSSEBody) processLine() {
	if body == nil {
		return
	}
	line := strings.TrimSuffix(string(body.line), "\r")
	body.line = body.line[:0]
	if line == "" {
		return
	}
	if strings.HasPrefix(line, "event:") {
		body.eventType = strings.TrimSpace(line[6:])
	} else if strings.HasPrefix(line, "data:") {
		body.eventData = strings.TrimSpace(line[5:])
	}
}

func (body *limitedMCPSSEBody) finishEvent() {
	if body == nil {
		return
	}
	if body.eventType == "endpoint" && body.eventData != "" && body.endpointCallback != nil {
		func() {
			defer func() { _ = recover() }()
			body.endpointCallback(body.eventData)
		}()
	}
	body.eventType, body.eventData = "", ""
}

func (body *limitedMCPSSEBody) Close() error {
	if body == nil || body.reader == nil {
		return nil
	}
	return closeMCPCloser(body.reader)
}

func validMCPSecretValue(value string) bool {
	if strings.TrimSpace(value) == "" || !utf8.ValidString(value) {
		return false
	}
	return strings.IndexFunc(value, unicode.IsControl) < 0
}

func restrictedMCPIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ipv4 := ip.To4(); ipv4 != nil {
		ip = ipv4
	}
	if !ip.IsGlobalUnicast() || ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() || ip.IsMulticast() {
		return true
	}
	// net.IP.IsGlobalUnicast intentionally includes several special-use
	// networks. Do not permit documentation, benchmarking, carrier-grade NAT,
	// or other non-public ranges as an MCP egress target.
	for _, network := range mcpRestrictedNetworks {
		if network.Contains(ip) {
			return true
		}
	}
	return false
}

func mustMCPRestrictedNetworks() []*net.IPNet {
	values := []string{
		"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24",
		"192.88.99.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24",
		"2001:0::/32", "2001:2::/48", "2001:10::/28", "2001:db8::/32",
	}
	networks := make([]*net.IPNet, 0, len(values))
	for _, cidr := range values {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			panic("invalid MCP restricted network: " + cidr)
		}
		networks = append(networks, network)
	}
	return networks
}

// NamespaceMCPToolSet wraps a runner-owned MCP ToolSet with stable
// revision-scoped tool names. The server's tool names remain the allowlist
// source; only the model-visible declaration is namespaced.
func NamespaceMCPToolSet(set trpctool.ToolSet, bindingName string) (trpctool.ToolSet, error) {
	if isNilMCPValue(set) || strings.TrimSpace(bindingName) == "" {
		return nil, fmt.Errorf("%w: MCP ToolSet and binding name are required", ErrInvalidMCPBinding)
	}
	bindingName = strings.TrimSpace(bindingName)
	if !validMCPName(bindingName) {
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
func (set namespacedMCPToolSet) Close() error {
	if isNilMCPValue(set.delegate) {
		return nil
	}
	return safeCloseMCPToolSet(set.delegate)
}

func callMCPCallable(ctx context.Context, callable trpctool.CallableTool, args []byte) (result any, err error) {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil, contextError(ctx)
	}
	if isNilMCPValue(callable) {
		return nil, ErrMCPExecutionUnavailable
	}
	if !validMCPArguments(args) {
		return nil, ErrMCPInvalidArguments
	}
	defer func() {
		if recover() != nil {
			result = nil
			err = ErrMCPExecutionUnavailable
		}
		if err == nil && !validMCPResult(result) {
			result = nil
			err = errMCPWireTooLarge
		}
	}()
	return callable.Call(ctx, args)
}

func callMCPStreamable(ctx context.Context, streamable trpctool.StreamableTool, args []byte) (reader *trpctool.StreamReader, err error) {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil, contextError(ctx)
	}
	if isNilMCPValue(streamable) || !validMCPArguments(args) {
		return nil, ErrMCPInvalidArguments
	}
	defer func() {
		if recover() != nil {
			if reader != nil {
				safeCloseMCPStreamReader(reader)
			}
			reader = nil
			err = ErrMCPExecutionUnavailable
		}
		if err != nil && reader != nil {
			safeCloseMCPStreamReader(reader)
			reader = nil
		}
	}()
	return streamable.StreamableCall(ctx, args)
}

func mcpToolCapabilities(candidate trpctool.Tool) (trpctool.CallableTool, bool, trpctool.StreamableTool, bool) {
	if isNilMCPValue(candidate) {
		return nil, false, nil, false
	}
	callable, callableOK := candidate.(trpctool.CallableTool)
	if callableOK && isNilMCPValue(callable) {
		callable, callableOK = nil, false
	}
	streamable, streamableOK := candidate.(trpctool.StreamableTool)
	if streamableOK && isNilMCPValue(streamable) {
		streamable, streamableOK = nil, false
	}
	return callable, callableOK, streamable, streamableOK
}

func (set namespacedMCPToolSet) Tools(ctx context.Context) []trpctool.Tool {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil
	}
	candidates, ok := safeMCPToolSetTools(ctx, set.delegate)
	if !ok {
		return nil
	}
	tools := make([]trpctool.Tool, 0, len(candidates))
	for _, candidate := range candidates {
		candidateDeclaration, declarationOK := safeMCPToolDeclaration(candidate)
		if !declarationOK || !validMCPName(candidateDeclaration.Name) {
			continue
		}
		declaration := *candidateDeclaration
		declaration.Name = set.prefix + declaration.Name
		if len([]rune(declaration.Name)) > 256 {
			continue
		}
		base := namespacedMCPTool{delegate: candidate, declaration: &declaration}
		callable, callableOK, streamable, streamableOK := mcpToolCapabilities(candidate)
		switch {
		case callableOK && streamableOK:
			tools = append(tools, namespacedMCPStreamableTool{namespacedMCPTool: base, callable: callable, streamable: streamable})
		case callableOK:
			tools = append(tools, namespacedMCPCallableTool{namespacedMCPTool: base, callable: callable})
		case streamableOK:
			tools = append(tools, namespacedMCPStreamableOnlyTool{namespacedMCPTool: base, streamable: streamable})
		}
	}
	return tools
}

type namespacedMCPTool struct {
	delegate    trpctool.Tool
	declaration *trpctool.Declaration
}

func (tool namespacedMCPTool) Declaration() *trpctool.Declaration { return tool.declaration }
func (tool namespacedMCPTool) ToolMetadata() trpctool.ToolMetadata {
	metadata, _ := safeMCPToolMetadata(tool.delegate)
	return metadata
}

type namespacedMCPCallableTool struct {
	namespacedMCPTool
	callable trpctool.CallableTool
}

func (tool namespacedMCPCallableTool) Call(ctx context.Context, args []byte) (any, error) {
	return callMCPCallable(ctx, tool.callable, args)
}

type namespacedMCPStreamableTool struct {
	namespacedMCPTool
	callable   trpctool.CallableTool
	streamable trpctool.StreamableTool
}

func (tool namespacedMCPStreamableTool) Call(ctx context.Context, args []byte) (any, error) {
	return callMCPCallable(ctx, tool.callable, args)
}

func (tool namespacedMCPStreamableTool) StreamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	return callMCPStreamable(ctx, tool.streamable, args)
}

type namespacedMCPStreamableOnlyTool struct {
	namespacedMCPTool
	streamable trpctool.StreamableTool
}

func (tool namespacedMCPStreamableOnlyTool) StreamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	return callMCPStreamable(ctx, tool.streamable, args)
}

// WrapMCPToolSetLifecycle makes connection recovery available without
// replaying a tool call. A transport error may be reported after the remote
// provider accepted a side effect, so the failed call is returned as-is. A
// later explicit invocation refreshes the session and then performs at most
// one provider call.
func WrapMCPToolSetLifecycle(set trpctool.ToolSet) (trpctool.ToolSet, error) {
	if isNilMCPValue(set) {
		return nil, fmt.Errorf("%w: MCP ToolSet is required", ErrInvalidMCPBinding)
	}
	if name, ok := safeMCPToolSetName(set); !ok || strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: MCP ToolSet is required", ErrInvalidMCPBinding)
	}
	return resilientMCPToolSet{delegate: set}, nil
}

type resilientMCPToolSet struct {
	delegate trpctool.ToolSet
}

func (set resilientMCPToolSet) Name() string {
	name, _ := safeMCPToolSetName(set.delegate)
	return name
}
func (set resilientMCPToolSet) Close() error {
	if isNilMCPValue(set.delegate) {
		return nil
	}
	if err := safeCloseMCPToolSet(set.delegate); err != nil && !mcpBenignLifecycleError(err) {
		return err
	}
	return nil
}

func (set resilientMCPToolSet) Tools(ctx context.Context) []trpctool.Tool {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil
	}
	candidates, ok := safeMCPToolSetTools(ctx, set.delegate)
	if !ok {
		return nil
	}
	tools := make([]trpctool.Tool, 0, len(candidates))
	for _, candidate := range candidates {
		candidateDeclaration, declarationOK := safeMCPToolDeclaration(candidate)
		if !declarationOK || !validMCPName(candidateDeclaration.Name) {
			continue
		}
		callable, callableOK, streamable, streamableOK := mcpToolCapabilities(candidate)
		if !callableOK && !streamableOK {
			// A declaration without an executable capability must never be
			// advertised as a usable MCP tool.
			continue
		}
		state := &resilientMCPToolState{delegate: candidate, callable: callable, streamable: streamable}
		base := resilientMCPTool{
			owner: set.delegate, name: candidateDeclaration.Name,
			declaration: cloneToolDeclaration(candidateDeclaration), state: state,
		}
		switch {
		case callableOK && streamableOK:
			tools = append(tools, resilientMCPCallableStreamableTool{resilientMCPTool: base})
		case callableOK:
			tools = append(tools, resilientMCPCallableTool{resilientMCPTool: base})
		default:
			tools = append(tools, resilientMCPStreamableOnlyTool{resilientMCPTool: base})
		}
	}
	return tools
}

type resilientMCPToolState struct {
	mu         sync.RWMutex
	refreshMu  sync.Mutex
	delegate   trpctool.Tool
	callable   trpctool.CallableTool
	streamable trpctool.StreamableTool
	reconnect  bool
}

func (state *resilientMCPToolState) metadata() trpctool.ToolMetadata {
	if state == nil {
		return trpctool.ToolMetadata{}
	}
	state.mu.RLock()
	delegate := state.delegate
	state.mu.RUnlock()
	metadata, _ := safeMCPToolMetadata(delegate)
	return metadata
}

func (state *resilientMCPToolState) reconnectIfNeeded(ctx context.Context, owner trpctool.ToolSet, name string) error {
	if state == nil || isNilMCPValue(owner) || isNilMCPValue(ctx) || ctx.Err() != nil {
		return fmt.Errorf("%w: MCP tool reconnect context is unavailable", ErrInvalidMCPBinding)
	}
	state.refreshMu.Lock()
	defer state.refreshMu.Unlock()
	state.mu.RLock()
	needed := state.reconnect
	state.mu.RUnlock()
	if !needed {
		return nil
	}
	var refreshed trpctool.Tool
	var callable trpctool.CallableTool
	var streamable trpctool.StreamableTool
	candidates, ok := safeMCPToolSetTools(ctx, owner)
	if !ok {
		return fmt.Errorf("%w: MCP tool refresh failed", ErrInvalidMCPBinding)
	}
	for _, candidate := range candidates {
		candidateDeclaration, declarationOK := safeMCPToolDeclaration(candidate)
		if !declarationOK || candidateDeclaration.Name != name {
			continue
		}
		candidateCallable, candidateCallableOK, candidateStreamable, candidateStreamableOK := mcpToolCapabilities(candidate)
		if !candidateCallableOK && !candidateStreamableOK {
			continue
		}
		refreshed, callable, streamable = candidate, candidateCallable, candidateStreamable
		break
	}
	if isNilMCPValue(refreshed) {
		return fmt.Errorf("%w: MCP tool %q was not re-advertised", ErrInvalidMCPBinding, name)
	}
	state.mu.Lock()
	state.delegate, state.callable, state.streamable, state.reconnect = refreshed, callable, streamable, false
	state.mu.Unlock()
	return nil
}

func (state *resilientMCPToolState) callableFor(ctx context.Context, owner trpctool.ToolSet, name string) (trpctool.CallableTool, error) {
	if err := state.reconnectIfNeeded(ctx, owner, name); err != nil {
		return nil, err
	}
	state.mu.RLock()
	callable := state.callable
	state.mu.RUnlock()
	if isNilMCPValue(callable) {
		return nil, fmt.Errorf("%w: MCP tool %q is not callable", ErrInvalidMCPBinding, name)
	}
	return callable, nil
}

func (state *resilientMCPToolState) streamableFor(ctx context.Context, owner trpctool.ToolSet, name string) (trpctool.StreamableTool, error) {
	if err := state.reconnectIfNeeded(ctx, owner, name); err != nil {
		return nil, err
	}
	state.mu.RLock()
	streamable := state.streamable
	state.mu.RUnlock()
	if isNilMCPValue(streamable) {
		return nil, fmt.Errorf("%w: MCP tool %q is not streamable", ErrInvalidMCPBinding, name)
	}
	return streamable, nil
}

func (state *resilientMCPToolState) markReconnect() {
	if state == nil {
		return
	}
	state.mu.Lock()
	state.reconnect = true
	state.mu.Unlock()
}

// resetMCPConnection drops a broken session without invoking the failed tool.
// The custom stdio implementation can reset without making the ToolSet
// permanently closed; upstream ToolSets are reset through Close and recreated
// by their next Tools call.
func resetMCPConnection(set trpctool.ToolSet) {
	if isNilMCPValue(set) {
		return
	}
	defer func() { _ = recover() }()
	if resetter, ok := set.(interface{ resetMCPConnection() }); ok && !isNilMCPValue(resetter) {
		resetter.resetMCPConnection()
		return
	}
	_ = safeCloseMCPToolSet(set)
}

func safeCloseMCPToolSet(set trpctool.ToolSet) (err error) {
	if isNilMCPValue(set) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = ErrMCPExecutionUnavailable
		}
	}()
	return set.Close()
}

func closeMCPCloser(closer io.Closer) (err error) {
	if isNilMCPValue(closer) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = ErrMCPExecutionUnavailable
		}
	}()
	return closer.Close()
}

func safeCloseMCPStreamReader(reader *trpctool.StreamReader) {
	if reader == nil {
		return
	}
	defer func() { _ = recover() }()
	reader.Close()
}

func safeMCPToolSetName(set trpctool.ToolSet) (name string, ok bool) {
	if isNilMCPValue(set) {
		return "", false
	}
	defer func() {
		if recover() != nil {
			name, ok = "", false
		}
	}()
	name = set.Name()
	return name, name != ""
}

func safeMCPToolSetTools(ctx context.Context, set trpctool.ToolSet) (tools []trpctool.Tool, ok bool) {
	if isNilMCPValue(ctx) || ctx.Err() != nil || isNilMCPValue(set) {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			tools, ok = nil, false
		}
	}()
	return set.Tools(ctx), true
}

func safeMCPToolDeclaration(candidate trpctool.Tool) (declaration *trpctool.Declaration, ok bool) {
	if isNilMCPValue(candidate) {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			declaration, ok = nil, false
		}
	}()
	declaration = candidate.Declaration()
	return declaration, declaration != nil
}

func safeMCPToolMetadata(candidate trpctool.Tool) (metadata trpctool.ToolMetadata, ok bool) {
	if isNilMCPValue(candidate) {
		return trpctool.ToolMetadata{}, false
	}
	defer func() {
		if recover() != nil {
			metadata, ok = trpctool.ToolMetadata{}, false
		}
	}()
	return trpctool.MetadataOf(candidate), true
}

type resilientMCPTool struct {
	owner       trpctool.ToolSet
	name        string
	declaration *trpctool.Declaration
	state       *resilientMCPToolState
}

func (tool resilientMCPTool) Declaration() *trpctool.Declaration { return tool.declaration }
func (tool resilientMCPTool) ToolMetadata() trpctool.ToolMetadata {
	return tool.state.metadata()
}

func (tool resilientMCPTool) Call(ctx context.Context, args []byte) (any, error) {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil, contextError(ctx)
	}
	if tool.state == nil || isNilMCPValue(tool.owner) {
		return nil, ErrMCPExecutionUnavailable
	}
	delegate, err := tool.state.callableFor(ctx, tool.owner, tool.name)
	if err != nil {
		return nil, err
	}
	result, callErr := callMCPCallable(ctx, delegate, args)
	if callErr != nil && ctx.Err() == nil && mcpConnectionFailure(callErr) {
		tool.state.markReconnect()
		resetMCPConnection(tool.owner)
	}
	return result, callErr
}

type resilientMCPCallableTool struct{ resilientMCPTool }
type resilientMCPStreamableOnlyTool struct{ resilientMCPTool }
type resilientMCPCallableStreamableTool struct{ resilientMCPTool }

func (tool resilientMCPStreamableOnlyTool) StreamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	return tool.streamableCall(ctx, args)
}

func (tool resilientMCPCallableStreamableTool) StreamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	return tool.streamableCall(ctx, args)
}

func (tool resilientMCPTool) streamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil, contextError(ctx)
	}
	if tool.state == nil || isNilMCPValue(tool.owner) {
		return nil, ErrMCPExecutionUnavailable
	}
	delegate, err := tool.state.streamableFor(ctx, tool.owner, tool.name)
	if err != nil {
		return nil, err
	}
	reader, callErr := callMCPStreamable(ctx, delegate, args)
	if callErr != nil && ctx.Err() == nil && mcpConnectionFailure(callErr) {
		tool.state.markReconnect()
		resetMCPConnection(tool.owner)
	}
	if callErr != nil {
		return nil, callErr
	}
	if reader == nil {
		return nil, fmt.Errorf("%w: MCP stream reader is nil", ErrInvalidMCPBinding)
	}
	return observeMCPStream(tool.owner, tool.state, reader), nil
}

func observeMCPStream(owner trpctool.ToolSet, state *resilientMCPToolState, reader *trpctool.StreamReader) *trpctool.StreamReader {
	stream := trpctool.NewStream(1)
	var terminal sync.Once
	sendTerminal := func(err error) {
		if err == nil {
			return
		}
		terminal.Do(func() { _ = stream.Writer.Send(trpctool.StreamChunk{}, err) })
	}
	go func() {
		defer stream.Writer.Close()
		defer func() {
			if recover() != nil {
				if state != nil {
					state.markReconnect()
					resetMCPConnection(owner)
				}
				sendTerminal(ErrMCPExecutionUnavailable)
			}
		}()
		defer func() {
			if reader != nil {
				safeCloseMCPStreamReader(reader)
			}
		}()
		if reader == nil {
			sendTerminal(ErrMCPExecutionUnavailable)
			return
		}
		var streamBytes int64
		for {
			chunk, err := reader.Recv()
			if err != nil {
				if !errors.Is(err, io.EOF) && (mcpConnectionFailure(err) || errors.Is(err, errMCPWireTooLarge)) && state != nil {
					state.markReconnect()
					resetMCPConnection(owner)
				}
				if !errors.Is(err, io.EOF) {
					sendTerminal(redactedMCPStreamError(err))
				}
				return
			}
			chunkSize, valid := mcpResultSize(chunk.Content)
			if !valid || streamBytes > maxMCPWireBytes-chunkSize {
				sendTerminal(errMCPWireTooLarge)
				return
			}
			streamBytes += chunkSize
			if stream.Writer.Send(chunk, nil) {
				return
			}
		}
	}()
	return stream.Reader
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
// checks the tenant/app execution context, per-tool approval policy, reviewer,
// audit recorder, and request-local budget immediately before the remote call.
// The input set must already use the stable mcp_<binding>__<tool> namespace.
func GovernMCPToolSet(ctx context.Context, set trpctool.ToolSet, tenantID, appID string, binding MCPBinding, reviewer review.Reviewer) (trpctool.ToolSet, error) {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil, fmt.Errorf("%w: active context is required", ErrInvalidMCPBinding)
	}
	if isNilMCPValue(set) || !validMCPTenantID(tenantID) || appmodel.ValidateAppID(appID) != nil {
		return nil, fmt.Errorf("%w: MCP ToolSet, tenant, and app are required and normalized", ErrInvalidMCPBinding)
	}
	value, err := binding.Normalize()
	if err != nil {
		return nil, err
	}
	seenNames := make(map[string]struct{})
	candidates, candidatesOK := safeMCPToolSetTools(ctx, set)
	if !candidatesOK {
		return nil, fmt.Errorf("%w: MCP ToolSet could not advertise tools", ErrInvalidMCPBinding)
	}
	for _, candidate := range candidates {
		candidateDeclaration, declarationOK := safeMCPToolDeclaration(candidate)
		if !declarationOK || !validMCPPublicToolName(candidateDeclaration.Name, value.Name) {
			return nil, fmt.Errorf("%w: MCP tool declaration is invalid", ErrInvalidMCPBinding)
		}
		if !mcpToolAllowlisted(value, candidateDeclaration.Name) {
			continue
		}
		if _, duplicate := seenNames[candidateDeclaration.Name]; duplicate {
			return nil, fmt.Errorf("%w: duplicate MCP tool %q", ErrInvalidMCPBinding, candidateDeclaration.Name)
		}
		seenNames[candidateDeclaration.Name] = struct{}{}
		_, callable, _, streamable := mcpToolCapabilities(candidate)
		if !callable {
			if !streamable {
				return nil, fmt.Errorf("%w: MCP tool %q has no executable capability", ErrInvalidMCPBinding, candidateDeclaration.Name)
			}
		}
	}
	for _, allowed := range value.ToolAllow {
		publicName := "mcp_" + value.Name + "__" + allowed
		if _, found := seenNames[publicName]; !found {
			return nil, fmt.Errorf("%w: MCP tool %q was not advertised", ErrInvalidMCPBinding, publicName)
		}
	}
	governed := governedMCPToolSet{delegate: set, tenantID: strings.TrimSpace(tenantID), appID: strings.TrimSpace(appID), binding: value, reviewer: reviewer}
	governedTools, governedOK := safeMCPToolSetTools(ctx, governed)
	if !governedOK {
		return nil, fmt.Errorf("%w: governed MCP tools are unavailable", ErrInvalidMCPBinding)
	}
	for _, candidate := range governedTools {
		candidateDeclaration, declarationOK := safeMCPToolDeclaration(candidate)
		metadata, metadataOK := safeMCPToolMetadata(candidate)
		if !declarationOK || !metadataOK {
			return nil, fmt.Errorf("%w: MCP tool declaration is invalid", ErrInvalidMCPBinding)
		}
		if mcpToolDecision(value, candidateDeclaration.Name, metadata) == ApprovalRequired && isNilMCPValue(reviewer) {
			return nil, fmt.Errorf("%w: reviewer is required for MCP tool %q", ErrApprovalRequired, candidateDeclaration.Name)
		}
	}
	return governed, nil
}

type governedMCPToolSet struct {
	delegate trpctool.ToolSet
	tenantID string
	appID    string
	binding  MCPBinding
	reviewer review.Reviewer
}

func (set governedMCPToolSet) Name() string {
	name, _ := safeMCPToolSetName(set.delegate)
	return name
}
func (set governedMCPToolSet) Close() error {
	if isNilMCPValue(set.delegate) {
		return nil
	}
	return safeCloseMCPToolSet(set.delegate)
}

func (set governedMCPToolSet) Tools(ctx context.Context) []trpctool.Tool {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return nil
	}
	candidates, ok := safeMCPToolSetTools(ctx, set.delegate)
	if !ok {
		return nil
	}
	tools := make([]trpctool.Tool, 0, len(candidates))
	for _, candidate := range candidates {
		candidateDeclaration, declarationOK := safeMCPToolDeclaration(candidate)
		if !declarationOK || !validMCPPublicToolName(candidateDeclaration.Name, set.binding.Name) || !mcpToolAllowlisted(set.binding, candidateDeclaration.Name) {
			continue
		}
		base := governedMCPToolBase{
			delegate: candidate, owner: set,
			declaration: cloneToolDeclaration(candidateDeclaration),
		}
		callable, callableOK, streamable, streamableOK := mcpToolCapabilities(candidate)
		switch {
		case callableOK && streamableOK:
			tools = append(tools, governedMCPCallableStreamableTool{governedMCPToolBase: base, callable: callable, streamable: streamable})
		case callableOK:
			tools = append(tools, governedMCPCallableTool{governedMCPToolBase: base, callable: callable})
		case streamableOK:
			tools = append(tools, governedMCPStreamableTool{governedMCPToolBase: base, streamable: streamable})
		}
	}
	return tools
}

type governedMCPToolBase struct {
	delegate    trpctool.Tool
	owner       governedMCPToolSet
	declaration *trpctool.Declaration
}

func (tool governedMCPToolBase) Declaration() *trpctool.Declaration { return tool.declaration }
func (tool governedMCPToolBase) ToolMetadata() trpctool.ToolMetadata {
	metadata, _ := safeMCPToolMetadata(tool.delegate)
	return metadata
}

func (tool governedMCPToolBase) admit(ctx context.Context, args []byte) (ExecutionContext, string, error) {
	if isNilMCPValue(ctx) || ctx.Err() != nil {
		return ExecutionContext{}, "", contextError(ctx)
	}
	execution, err := mcpExecutionContextFromContext(ctx)
	if err != nil || execution.TenantID != tool.owner.tenantID || execution.AppID != tool.owner.appID || execution.UserID == "" || execution.SessionID == "" {
		return ExecutionContext{}, "", ErrMCPExecutionUnavailable
	}
	if !execution.Audit.Configured() {
		return ExecutionContext{}, "", ErrMCPExecutionUnavailable
	}
	if !validMCPArguments(args) {
		return ExecutionContext{}, "", ErrMCPInvalidArguments
	}
	if tool.declaration == nil || !validMCPPublicToolName(tool.declaration.Name, tool.owner.binding.Name) {
		return ExecutionContext{}, "", ErrMCPExecutionUnavailable
	}
	name := tool.declaration.Name
	metadata, metadataOK := safeMCPToolMetadata(tool.delegate)
	if !metadataOK {
		return ExecutionContext{}, "", ErrMCPExecutionUnavailable
	}
	decision := mcpToolDecision(tool.owner.binding, name, metadata)
	policy := Policy{Recorder: execution.Audit, Allowed: map[string]Decision{name: decision}}
	if _, err := policy.Decide(ctx, execution.RequestID, execution.TraceID, name); err != nil {
		if !errors.Is(err, ErrApprovalRequired) {
			return ExecutionContext{}, "", err
		}
		if isNilMCPValue(tool.owner.reviewer) {
			return ExecutionContext{}, "", ErrApprovalRequired
		}
		reviewDecision, reviewErr := callMCPReviewer(ctx, tool.owner.reviewer, &review.Request{Action: review.Action{
			ToolName: name, ToolDescription: tool.declaration.Description, Arguments: append(json.RawMessage(nil), args...),
		}})
		if reviewErr != nil || reviewDecision == nil || !reviewDecision.Approved {
			return ExecutionContext{}, "", ErrApprovalDenied
		}
		allowPolicy := Policy{Recorder: execution.Audit, Allowed: map[string]Decision{name: Allow}}
		if _, err := allowPolicy.Decide(ctx, execution.RequestID, execution.TraceID, name); err != nil {
			return ExecutionContext{}, "", err
		}
	}
	if !isNilMCPValue(execution.ToolBudget) {
		if err := execution.ToolBudget.Consume(); err != nil {
			_ = execution.Audit.BudgetRejected(ctx, execution.RequestID, execution.TraceID)
			return ExecutionContext{}, "", err
		}
	}
	return execution, name, nil
}

func (tool governedMCPToolBase) executed(ctx context.Context, execution ExecutionContext, name string) error {
	return execution.Audit.ToolExecuted(ctx, execution.RequestID, execution.TraceID, name)
}

type governedMCPCallableTool struct {
	governedMCPToolBase
	callable trpctool.CallableTool
}

func (tool governedMCPCallableTool) Call(ctx context.Context, args []byte) (any, error) {
	execution, name, err := tool.admit(ctx, args)
	if err != nil {
		return nil, err
	}
	result, callErr := callMCPCallable(ctx, tool.callable, args)
	if callErr != nil {
		return nil, redactedToolError(callErr)
	}
	if !validMCPResult(result) {
		return nil, errMCPWireTooLarge
	}
	if err := tool.executed(ctx, execution, name); err != nil {
		return nil, err
	}
	return result, nil
}

type governedMCPStreamableTool struct {
	governedMCPToolBase
	streamable trpctool.StreamableTool
}

func (tool governedMCPStreamableTool) StreamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	execution, name, err := tool.admit(ctx, args)
	if err != nil {
		return nil, err
	}
	reader, callErr := callMCPStreamable(ctx, tool.streamable, args)
	if callErr != nil {
		return nil, redactedToolError(callErr)
	}
	if reader == nil {
		return nil, ErrMCPExecutionUnavailable
	}
	return auditMCPStream(ctx, reader, execution, name), nil
}

type governedMCPCallableStreamableTool struct {
	governedMCPToolBase
	callable   trpctool.CallableTool
	streamable trpctool.StreamableTool
}

func (tool governedMCPCallableStreamableTool) Call(ctx context.Context, args []byte) (any, error) {
	execution, name, err := tool.admit(ctx, args)
	if err != nil {
		return nil, err
	}
	result, callErr := callMCPCallable(ctx, tool.callable, args)
	if callErr != nil {
		return nil, redactedToolError(callErr)
	}
	if !validMCPResult(result) {
		return nil, errMCPWireTooLarge
	}
	if err := tool.executed(ctx, execution, name); err != nil {
		return nil, err
	}
	return result, nil
}

func (tool governedMCPCallableStreamableTool) StreamableCall(ctx context.Context, args []byte) (*trpctool.StreamReader, error) {
	execution, name, err := tool.admit(ctx, args)
	if err != nil {
		return nil, err
	}
	reader, callErr := callMCPStreamable(ctx, tool.streamable, args)
	if callErr != nil {
		return nil, redactedToolError(callErr)
	}
	if reader == nil {
		return nil, ErrMCPExecutionUnavailable
	}
	return auditMCPStream(ctx, reader, execution, name), nil
}

func auditMCPStream(ctx context.Context, reader *trpctool.StreamReader, execution ExecutionContext, name string) *trpctool.StreamReader {
	stream := trpctool.NewStream(1)
	var terminal sync.Once
	sendTerminal := func(err error) {
		if err == nil {
			return
		}
		terminal.Do(func() { _ = stream.Writer.Send(trpctool.StreamChunk{}, err) })
	}
	go func() {
		defer stream.Writer.Close()
		defer func() {
			if recover() != nil {
				sendTerminal(ErrMCPExecutionUnavailable)
			}
		}()
		defer func() {
			if reader != nil {
				safeCloseMCPStreamReader(reader)
			}
		}()
		if reader == nil {
			sendTerminal(ErrMCPExecutionUnavailable)
			return
		}
		var streamBytes int64
		for {
			chunk, err := reader.Recv()
			if errors.Is(err, io.EOF) {
				if !isNilMCPValue(ctx) && ctx.Err() == nil {
					if auditErr := execution.Audit.ToolExecuted(ctx, execution.RequestID, execution.TraceID, name); auditErr != nil {
						sendTerminal(redactedToolError(auditErr))
					}
				}
				return
			}
			if err != nil {
				sendTerminal(redactedToolError(err))
				return
			}
			chunkSize, valid := mcpResultSize(chunk.Content)
			if !valid || streamBytes > maxMCPWireBytes-chunkSize {
				sendTerminal(errMCPWireTooLarge)
				return
			}
			streamBytes += chunkSize
			if stream.Writer.Send(chunk, nil) {
				return
			}
		}
	}()
	return stream.Reader
}

func callMCPReviewer(ctx context.Context, reviewer review.Reviewer, request *review.Request) (decision *review.Decision, err error) {
	if isNilMCPValue(reviewer) || isNilMCPValue(ctx) || request == nil {
		return nil, ErrApprovalDenied
	}
	defer func() {
		if recover() != nil {
			decision = nil
			err = ErrApprovalDenied
		}
	}()
	return reviewer.Review(ctx, request)
}

func mcpExecutionContextFromContext(ctx context.Context) (ExecutionContext, error) {
	if isNilMCPValue(ctx) {
		return ExecutionContext{}, ErrMCPExecutionUnavailable
	}
	execution, ok := ctx.Value(executionContextKey{}).(ExecutionContext)
	if !ok || isNilMCPValue(execution.ToolInvocations) || !validMCPTenantID(execution.TenantID) || appmodel.ValidateAppID(execution.AppID) != nil || !validMCPExecutionID(execution.UserID, true) || !validMCPExecutionID(execution.SessionID, true) || !validMCPExecutionID(execution.EventID, true) || !validMCPExecutionID(execution.RequestID, true) || !validMCPExecutionID(execution.TraceID, false) {
		return ExecutionContext{}, ErrMCPExecutionUnavailable
	}
	return execution, nil
}

func validMCPTenantID(value string) bool {
	return modelprofile.ValidateTenantID(value) == nil
}

func validMCPExecutionID(value string, required bool) bool {
	if !utf8.ValidString(value) || value != strings.TrimSpace(value) || strings.Contains(value, "://") || strings.IndexFunc(value, unicode.IsControl) >= 0 || len([]rune(value)) > 256 {
		return false
	}
	return !required || value != ""
}

func validMCPText(value string) bool {
	if !utf8.ValidString(value) || len([]rune(value)) > 2048 {
		return false
	}
	return strings.IndexFunc(value, unicode.IsControl) < 0
}

func validMCPName(value string) bool {
	if value == "" || !utf8.ValidString(value) || len([]rune(value)) > 128 {
		return false
	}
	for _, char := range value {
		if !(char == '-' || char == '_' || char == '.' || char >= '0' && char <= '9' || char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z') {
			return false
		}
	}
	return true
}

func validMCPPublicToolName(value, bindingName string) bool {
	if !validMCPName(bindingName) || len([]rune(value)) > 256 {
		return false
	}
	prefix := "mcp_" + bindingName + "__"
	return strings.HasPrefix(value, prefix) && validMCPName(strings.TrimPrefix(value, prefix))
}

func validMCPArguments(args []byte) bool {
	if len(args) > 1<<20 {
		return false
	}
	// MCP tool arguments are always object-shaped. Empty arguments are
	// normalized to an empty object by the stdio transport. Once bytes are
	// present, validate the original document so non-JSON Unicode whitespace
	// cannot be silently stripped at the edge.
	return len(args) == 0 || jsonstrict.Validate(args, true) == nil && decoderInputIsObject(args)
}

func decoderInputIsObject(value []byte) bool {
	value = bytes.TrimSpace(value)
	return len(value) > 1 && value[0] == '{' && value[len(value)-1] == '}'
}

func validateMCPJSONValue(decoder *json.Decoder, depth int) error {
	if decoder == nil || depth > 128 {
		return errMCPWireTooLarge
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	switch delimiter := token.(type) {
	case json.Delim:
		switch delimiter {
		case '{':
			seen := make(map[string]struct{})
			for decoder.More() {
				key, ok := mustJSONTokenString(decoder)
				if !ok {
					return errMCPWireInvalidUTF8
				}
				if _, duplicate := seen[key]; duplicate {
					return fmt.Errorf("duplicate JSON object key")
				}
				seen[key] = struct{}{}
				if err := validateMCPJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim('}') {
				return errMCPWireInvalidUTF8
			}
		case '[':
			for decoder.More() {
				if err := validateMCPJSONValue(decoder, depth+1); err != nil {
					return err
				}
			}
			end, err := decoder.Token()
			if err != nil || end != json.Delim(']') {
				return errMCPWireInvalidUTF8
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
	}
	return nil
}

func mustJSONTokenString(decoder *json.Decoder) (string, bool) {
	if decoder == nil {
		return "", false
	}
	token, err := decoder.Token()
	value, ok := token.(string)
	return value, err == nil && ok && utf8.ValidString(value)
}

func validMCPResult(result any) bool {
	_, ok := mcpResultSize(result)
	return ok
}

func mcpResultSize(result any) (size int64, ok bool) {
	defer func() {
		if recover() != nil {
			size, ok = 0, false
		}
	}()
	if !validMCPResultValue(reflect.ValueOf(result), 0) {
		return 0, false
	}
	encoded, err := json.Marshal(result)
	if err != nil || !utf8.Valid(encoded) || int64(len(encoded)) > maxMCPWireBytes || jsonstrict.Validate(encoded, false) != nil {
		return 0, false
	}
	return int64(len(encoded)), true
}

var mcpRawMessageType = reflect.TypeOf(json.RawMessage(nil))

// validMCPResultValue rejects invalid UTF-8 before encoding/json can silently
// replace it in a JSON string. The final Marshal below remains authoritative
// for unsupported values, cycles, NaN, and other JSON serialization errors.
func validMCPResultValue(value reflect.Value, depth int) bool {
	if !value.IsValid() {
		return true
	}
	if depth > 128 {
		return false
	}
	if value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return true
		}
		return validMCPResultValue(value.Elem(), depth+1)
	}
	if value.Type() == mcpRawMessageType {
		if value.IsNil() {
			return true
		}
		raw := value.Bytes()
		return jsonstrict.Validate(raw, false) == nil
	}
	switch value.Kind() {
	case reflect.String:
		return utf8.ValidString(value.String())
	case reflect.Map:
		if value.IsNil() {
			return true
		}
		if value.Type().Key().Kind() == reflect.String {
			iter := value.MapRange()
			for iter.Next() {
				if !utf8.ValidString(iter.Key().String()) || !validMCPResultValue(iter.Value(), depth+1) {
					return false
				}
			}
			return true
		}
		iter := value.MapRange()
		for iter.Next() {
			if !validMCPResultValue(iter.Value(), depth+1) {
				return false
			}
		}
		return true
	case reflect.Array, reflect.Slice:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return true
		}
		for index := 0; index < value.Len(); index++ {
			if !validMCPResultValue(value.Index(index), depth+1) {
				return false
			}
		}
		return true
	case reflect.Struct:
		typeInfo := value.Type()
		for index := 0; index < value.NumField(); index++ {
			field := typeInfo.Field(index)
			if field.PkgPath != "" { // unexported; encoding/json cannot emit it
				continue
			}
			if tag, ok := field.Tag.Lookup("json"); ok && strings.Split(tag, ",")[0] == "-" {
				continue
			}
			if !validMCPResultValue(value.Field(index), depth+1) {
				return false
			}
		}
		return true
	default:
		return true
	}
}

func mcpToolAllowlisted(binding MCPBinding, publicName string) bool {
	prefix := "mcp_" + binding.Name + "__"
	if !strings.HasPrefix(publicName, prefix) {
		return false
	}
	remoteName := strings.TrimPrefix(publicName, prefix)
	for _, allowed := range binding.ToolAllow {
		if remoteName == allowed {
			return true
		}
	}
	return false
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
	if metadata.ReadOnly && !metadata.Destructive && !metadata.OpenWorld {
		return Allow
	}
	return ApprovalRequired
}

func redactedMCPStreamError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errMCPWireTooLarge) {
		return errMCPWireTooLarge
	}
	if errors.Is(err, errMCPWireInvalidUTF8) {
		return errMCPWireInvalidUTF8
	}
	return redactedToolError(err)
}

func mcpConnectionFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errMCPWireTooLarge) || errors.Is(err, errMCPWireInvalidUTF8) {
		return true
	}
	value := strings.ToLower(err.Error())
	for _, marker := range []string{"transport closed", "transport is closed", "broken pipe", "connection reset", "file already closed", "eof", "process already finished", "process closed", "client not initialized", "session not found"} {
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
		declaration, ok := safeMCPToolDeclaration(candidate)
		if !ok {
			return false
		}
		_, ok = allow[declaration.Name]
		return ok
	}
}
