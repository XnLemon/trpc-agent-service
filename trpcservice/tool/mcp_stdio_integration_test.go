package tool

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// TestMCPStdioServerProcess is the deterministic in-process MCP server used
// by the lifecycle tests below. It is launched as a child copy of this test
// binary, so the probe does not require a shell, Node, Python, or a network.
func TestMCPStdioServerProcess(t *testing.T) {
	if !stdioProbeChild() {
		return
	}
	server := newStdioProbeServer(os.Stdin, os.Stdout, os.Args[1:])
	server.installSignalHandler()
	server.serve()
	// The Go test harness writes PASS to stdout after the test returns. Exit
	// before it can append harness output to the MCP JSON-RPC stream.
	os.Exit(0)
}

func TestMCPStdioToolSetRunsAndClosesAnIndependentProcess(t *testing.T) {
	skipUnsupportedStdio(t)
	command := stdioProbeCommand(t)
	binding := MCPBinding{
		Name: "local", Transport: "stdio", Command: command,
		Args: stdioProbeArgs(), ToolAllow: []string{"echo", "pid"}, TimeoutSeconds: 2,
	}
	set, err := newMCPToolSet(context.Background(), mcpTestTenantID, binding, nil, mcpNetworkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, err := WrapMCPToolSetLifecycle(set)
	if err != nil {
		t.Fatal(err)
	}
	namespaced, err := NamespaceMCPToolSet(lifecycle, binding.Name)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = namespaced.Close() })

	tools := namespaced.Tools(context.Background())
	if len(tools) != 2 {
		t.Fatalf("stdio tools = %d", len(tools))
	}
	byName := make(map[string]trpctool.CallableTool, len(tools))
	for _, candidate := range tools {
		callable, ok := candidate.(trpctool.CallableTool)
		if !ok {
			t.Fatalf("stdio tool is not callable: %T", candidate)
		}
		byName[candidate.Declaration().Name] = callable
	}
	echo, ok := byName["mcp_local__echo"]
	if !ok {
		t.Fatal("missing namespaced stdio echo tool")
	}
	result, err := echo.Call(context.Background(), []byte(`{"value":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(fmt.Sprint(result), "hello") {
		t.Fatalf("stdio echo result = %#v", result)
	}
	pid, ok := byName["mcp_local__pid"]
	if !ok {
		t.Fatal("missing namespaced stdio pid tool")
	}
	firstPID, err := pid.Call(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(firstPID) == "" {
		t.Fatal("stdio pid result is empty")
	}

	secondSet, err := newMCPToolSet(context.Background(), mcpTestTenantID, binding, nil, mcpNetworkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	secondLifecycle, err := WrapMCPToolSetLifecycle(secondSet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondLifecycle.Close() })
	secondTools := secondLifecycle.Tools(context.Background())
	secondPIDTool, ok := findCallableTool(secondTools, "pid")
	if !ok {
		t.Fatal("second stdio process did not advertise pid")
	}
	secondPID, err := secondPIDTool.Call(context.Background(), []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(firstPID) == fmt.Sprint(secondPID) {
		t.Fatalf("stdio ToolSets unexpectedly shared process pid %v", firstPID)
	}
}

func TestMCPStdioToolSetHonorsCancellationAndReconnectsAfterProcessExit(t *testing.T) {
	skipUnsupportedStdio(t)
	command := stdioProbeCommand(t)

	slowBinding := MCPBinding{
		Name: "slow", Transport: "stdio", Command: command,
		Args: stdioProbeArgs(), ToolAllow: []string{"slow"}, TimeoutSeconds: 1,
	}
	slowSet, err := newMCPToolSet(context.Background(), mcpTestTenantID, slowBinding, nil, mcpNetworkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	slowLifecycle, err := WrapMCPToolSetLifecycle(slowSet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = slowLifecycle.Close() })
	slowTool, ok := findCallableTool(slowLifecycle.Tools(context.Background()), "slow")
	if !ok {
		t.Fatal("missing slow stdio tool")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := slowTool.Call(ctx, []byte(`{}`)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stdio cancellation error = %v", err)
	}

	marker := filepath.Join(t.TempDir(), "exited")
	reconnectBinding := MCPBinding{
		Name: "restart", Transport: "stdio", Command: command,
		Args: stdioProbeArgs("exit-marker=" + marker), ToolAllow: []string{"echo"}, TimeoutSeconds: 2,
	}
	reconnectSet, err := newMCPToolSet(context.Background(), mcpTestTenantID, reconnectBinding, nil, mcpNetworkOptions{})
	if err != nil {
		t.Fatal(err)
	}
	reconnectLifecycle, err := WrapMCPToolSetLifecycle(reconnectSet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reconnectLifecycle.Close() })
	echo, ok := findCallableTool(reconnectLifecycle.Tools(context.Background()), "echo")
	if !ok {
		t.Fatal("missing reconnect echo tool")
	}
	if _, err := echo.Call(context.Background(), []byte(`{"value":"first"}`)); err != nil {
		t.Fatal(err)
	}
	waitForFile(t, marker)
	if _, err := echo.Call(context.Background(), []byte(`{"value":"second"}`)); err != nil {
		// The marker can be written just before the wait goroutine observes
		// process exit. The failed call is deliberately not replayed; this
		// second explicit invocation is the only permitted reconnect attempt.
		if _, retryErr := echo.Call(context.Background(), []byte(`{"value":"second"}`)); retryErr != nil {
			t.Fatalf("stdio explicit reconnect call error = %v (initial error: %v)", retryErr, err)
		}
	}
}

func skipUnsupportedStdio(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("stdio process probe uses Unix signal semantics")
	}
}

func stdioProbeCommand(t *testing.T) string {
	t.Helper()
	command, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	if !filepath.IsAbs(command) {
		t.Fatal("test executable path is not absolute")
	}
	return command
}

func stdioProbeArgs(options ...string) []string {
	pattern := "^TestMCPStdioServerProcess$"
	for _, option := range options {
		pattern += "|mcp-" + option
	}
	return []string{"-test.run=" + pattern}
}

func stdioProbeChild() bool {
	for _, arg := range os.Args[1:] {
		if strings.HasPrefix(arg, "-test.run=") && strings.Contains(strings.TrimPrefix(arg, "-test.run="), "^TestMCPStdioServerProcess$") {
			return true
		}
	}
	return false
}

func findCallableTool(tools []trpctool.Tool, remoteName string) (trpctool.CallableTool, bool) {
	for _, candidate := range tools {
		if candidate == nil || candidate.Declaration() == nil {
			continue
		}
		if candidate.Declaration().Name == remoteName || strings.HasSuffix(candidate.Declaration().Name, "__"+remoteName) {
			callable, ok := candidate.(trpctool.CallableTool)
			return callable, ok
		}
	}
	return nil, false
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

type stdioProbeServer struct {
	decoder     *json.Decoder
	encoder     *json.Encoder
	exitMarker  string
	closeMarker string
	exitOnce    bool
}

func newStdioProbeServer(input io.Reader, output io.Writer, args []string) *stdioProbeServer {
	server := &stdioProbeServer{decoder: json.NewDecoder(bufio.NewReader(input)), encoder: json.NewEncoder(output)}
	for _, arg := range args {
		if !strings.HasPrefix(arg, "-test.run=") {
			continue
		}
		for _, option := range strings.Split(strings.TrimPrefix(arg, "-test.run="), "|") {
			switch {
			case strings.HasPrefix(option, "mcp-exit-marker="):
				server.exitMarker = strings.TrimPrefix(option, "mcp-exit-marker=")
			case strings.HasPrefix(option, "mcp-close-marker="):
				server.closeMarker = strings.TrimPrefix(option, "mcp-close-marker=")
			}
		}
	}
	if server.exitMarker != "" {
		_, err := os.Stat(server.exitMarker)
		server.exitOnce = err == nil
	}
	return server
}

func (server *stdioProbeServer) installSignalHandler() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signals
		if server.closeMarker != "" {
			_ = os.WriteFile(server.closeMarker, []byte("closed"), 0o600)
		}
		os.Exit(0)
	}()
}

func (server *stdioProbeServer) serve() {
	for {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := server.decoder.Decode(&request); err != nil {
			if errors.Is(err, io.EOF) && server.closeMarker != "" {
				_ = os.WriteFile(server.closeMarker, []byte("closed"), 0o600)
			}
			return
		}
		if len(request.ID) == 0 || request.Method == "notifications/initialized" {
			continue
		}
		var result any
		switch request.Method {
		case "initialize":
			result = map[string]any{
				"protocolVersion": "2025-03-26",
				"serverInfo":      map[string]any{"name": "stdio-probe", "version": "1"},
				"capabilities":    map[string]any{},
			}
		case "tools/list":
			result = map[string]any{"tools": []any{
				map[string]any{"name": "echo", "description": "echo input", "inputSchema": map[string]any{"type": "object"}},
				map[string]any{"name": "pid", "description": "return process id", "inputSchema": map[string]any{"type": "object"}},
				map[string]any{"name": "slow", "description": "wait before replying", "inputSchema": map[string]any{"type": "object"}},
			}}
		case "tools/call":
			var params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			}
			_ = json.Unmarshal(request.Params, &params)
			switch params.Name {
			case "slow":
				time.Sleep(250 * time.Millisecond)
			case "pid":
				result = map[string]any{"content": []any{map[string]any{"type": "text", "text": fmt.Sprint(os.Getpid())}}}
			default:
				value := ""
				if raw, ok := params.Arguments["value"]; ok {
					value = fmt.Sprint(raw)
				}
				result = map[string]any{"content": []any{map[string]any{"type": "text", "text": value}}}
			}
			if params.Name == "slow" {
				result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "slow"}}}
			}
		default:
			result = map[string]any{}
		}
		response := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(request.ID), "result": result}
		if err := server.encoder.Encode(response); err != nil {
			return
		}
		if request.Method == "tools/call" && server.exitMarker != "" && !server.exitOnce {
			server.exitOnce = true
			_ = os.WriteFile(server.exitMarker, []byte("exited"), 0o600)
			return
		}
	}
}
