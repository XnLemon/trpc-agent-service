package tool

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
	trpcmcp "trpc.group/trpc-go/trpc-mcp-go"
)

func TestNewMCPToolSetInitializesFiltersAndCallsUpstreamServer(t *testing.T) {
	handler := &mcpProbeHandler{}
	binding := MCPBinding{
		Name: "files", Transport: "streamable", ServerURL: "https://mcp.example.test/tools",
		SecretRef: "secret://tenant/mcp", ToolAllow: []string{"read_file"},
	}
	set, err := newMCPToolSet(context.Background(), "tenant-a", binding, mcpProbeSecrets{}, mcpNetworkOptions{
		resolver:       mcpProbeResolver{addresses: []net.IPAddr{{IP: net.ParseIP("192.0.2.10")}}},
		requestHandler: handler,
		clientOptions:  []trpcmcp.ClientOption{trpcmcp.WithClientGetSSEEnabled(false)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer set.Close()
	tools := set.Tools(context.Background())
	if len(tools) != 1 || tools[0].Declaration() == nil || tools[0].Declaration().Name != "read_file" {
		t.Fatalf("filtered MCP tools = %#v", tools)
	}
	callable, ok := tools[0].(trpctool.CallableTool)
	if !ok {
		t.Fatal("MCP tool is not callable")
	}
	result, err := callable.Call(context.Background(), []byte(`{"path":"/tmp/report.txt"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(resultString(result), "read-file-result") {
		t.Fatalf("MCP call result = %#v", result)
	}
	if handler.count("initialize") != 1 || handler.count("tools/list") < 1 || handler.count("tools/call") != 1 {
		t.Fatalf("MCP request counts = %+v", handler.counts())
	}
	if handler.authorization() != "Bearer mcp-secret" {
		t.Fatalf("MCP authorization header = %q", handler.authorization())
	}
}

func TestNewMCPToolSetRejectsPrivateDNSBeforeConnecting(t *testing.T) {
	binding := MCPBinding{
		Name: "private", Transport: "streamable", ServerURL: "https://mcp.example.test/tools",
		SecretRef: "secret://tenant/mcp", ToolAllow: []string{"read_file"},
	}
	for _, addresses := range [][]net.IPAddr{
		{{IP: net.ParseIP("10.0.0.8")}},
		{{IP: net.ParseIP("192.0.2.10")}, {IP: net.ParseIP("fd00::8")}},
	} {
		_, err := newMCPToolSet(context.Background(), "tenant-a", binding, mcpProbeSecrets{}, mcpNetworkOptions{
			resolver: mcpProbeResolver{addresses: addresses},
		})
		if err == nil {
			t.Fatalf("restricted DNS addresses accepted: %v", addresses)
		}
	}
}

type mcpProbeResolver struct {
	addresses []net.IPAddr
	err       error
}

func (resolver mcpProbeResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return resolver.addresses, resolver.err
}

type mcpProbeSecrets struct{}

func (mcpProbeSecrets) Resolve(context.Context, modelprofile.SecretScope) (modelprofile.SecretValue, error) {
	return modelprofile.NewSecretValue("mcp-secret")
}

type mcpProbeHandler struct {
	mu      sync.Mutex
	methods []string
	auth    string
}

func (handler *mcpProbeHandler) Handle(_ context.Context, _ *http.Client, request *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	var envelope struct {
		ID     any    `json:"id"`
		Method string `json:"method"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, err
	}
	handler.mu.Lock()
	handler.methods = append(handler.methods, envelope.Method)
	if value := request.Header.Get("Authorization"); value != "" {
		handler.auth = value
	}
	handler.mu.Unlock()
	if envelope.ID == nil {
		return &http.Response{StatusCode: http.StatusAccepted, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	var result any
	switch envelope.Method {
	case "initialize":
		result = map[string]any{"protocolVersion": "2025-03-26", "serverInfo": map[string]any{"name": "probe", "version": "1"}, "capabilities": map[string]any{"tools": map[string]any{}}}
	case "tools/list":
		result = map[string]any{"tools": []any{
			map[string]any{"name": "read_file", "description": "Read a file", "inputSchema": map[string]any{"type": "object", "properties": map[string]any{"path": map[string]any{"type": "string"}}}},
			map[string]any{"name": "delete_file", "description": "Delete a file", "inputSchema": map[string]any{"type": "object"}},
		}}
	case "tools/call":
		result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "read-file-result"}}}
	default:
		result = map[string]any{}
	}
	encoded, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": envelope.ID, "result": result})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(encoded)))}, nil
}

func (handler *mcpProbeHandler) count(method string) int {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	count := 0
	for _, value := range handler.methods {
		if value == method {
			count++
		}
	}
	return count
}

func (handler *mcpProbeHandler) counts() map[string]int {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	result := make(map[string]int)
	for _, value := range handler.methods {
		result[value]++
	}
	return result
}

func (handler *mcpProbeHandler) authorization() string {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return handler.auth
}

func resultString(value any) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
