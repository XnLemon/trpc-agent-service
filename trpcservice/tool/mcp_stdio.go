package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	stdioMCPReconnectAttempts = 2
	stdioMCPCloseWait         = 2 * time.Second
)

// stdioMCPToolSet is the platform lifecycle boundary for command-backed MCP.
// It deliberately does not use a shell or inherit a caller context for the
// child process. A process belongs to one Runner ToolSet and is killed when
// that ToolSet closes; individual request contexts only cancel their request.
type stdioMCPToolSet struct {
	name    string
	command string
	args    []string
	timeout time.Duration
	allowed map[string]struct{}

	mu        sync.RWMutex
	process   *stdioMCPProcess
	tools     map[string]stdioMCPToolDefinition
	closed    bool
	requestMu sync.Mutex
}

func newStdioMCPToolSet(ctx context.Context, binding MCPBinding) (*stdioMCPToolSet, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, fmt.Errorf("%w: active context is required", ErrInvalidMCPBinding)
	}
	allowed := make(map[string]struct{}, len(binding.ToolAllow))
	for _, name := range binding.ToolAllow {
		allowed[name] = struct{}{}
	}
	set := &stdioMCPToolSet{
		name: binding.Name, command: binding.Command, args: append([]string(nil), binding.Args...),
		timeout: time.Duration(binding.TimeoutSeconds) * time.Second, allowed: allowed,
		tools: make(map[string]stdioMCPToolDefinition, len(allowed)),
	}
	if err := set.Init(ctx); err != nil {
		_ = set.Close()
		return nil, err
	}
	return set, nil
}

func (set *stdioMCPToolSet) Name() string {
	if set == nil {
		return ""
	}
	return set.name
}

func (set *stdioMCPToolSet) Init(ctx context.Context) error {
	if set == nil {
		return fmt.Errorf("%w: MCP stdio ToolSet is nil", ErrInvalidMCPBinding)
	}
	set.requestMu.Lock()
	defer set.requestMu.Unlock()
	if err := set.ensureConnectedLocked(ctx); err != nil {
		return fmt.Errorf("%w: initialize MCP stdio ToolSet: %w", ErrInvalidMCPBinding, err)
	}
	return nil
}

func (set *stdioMCPToolSet) Tools(ctx context.Context) []trpctool.Tool {
	if set == nil {
		return nil
	}
	set.requestMu.Lock()
	defer set.requestMu.Unlock()
	if err := set.ensureConnectedLocked(ctx); err != nil {
		return set.cachedTools()
	}
	return set.cachedTools()
}

func (set *stdioMCPToolSet) Close() error {
	if set == nil {
		return nil
	}
	set.requestMu.Lock()
	set.mu.Lock()
	if set.closed {
		set.mu.Unlock()
		set.requestMu.Unlock()
		return nil
	}
	set.closed = true
	process := set.process
	set.process = nil
	set.mu.Unlock()
	set.requestMu.Unlock()
	if process == nil {
		return nil
	}
	return process.close()
}

func (set *stdioMCPToolSet) cachedTools() []trpctool.Tool {
	set.mu.RLock()
	defer set.mu.RUnlock()
	if len(set.tools) == 0 {
		return nil
	}
	names := make([]string, 0, len(set.tools))
	for name := range set.tools {
		names = append(names, name)
	}
	sort.Strings(names)
	result := make([]trpctool.Tool, 0, len(names))
	for _, name := range names {
		definition := set.tools[name]
		declaration := definition.declaration
		result = append(result, &stdioMCPTool{
			owner: set, remoteName: name,
			declaration: cloneToolDeclaration(declaration), metadata: definition.metadata,
		})
	}
	return result
}

type stdioMCPToolDefinition struct {
	declaration *trpctool.Declaration
	metadata    trpctool.ToolMetadata
}

type stdioMCPTool struct {
	owner       *stdioMCPToolSet
	remoteName  string
	declaration *trpctool.Declaration
	metadata    trpctool.ToolMetadata
}

func (tool *stdioMCPTool) Declaration() *trpctool.Declaration {
	if tool == nil {
		return nil
	}
	return tool.declaration
}

func (tool *stdioMCPTool) ToolMetadata() trpctool.ToolMetadata {
	if tool == nil {
		return trpctool.ToolMetadata{}
	}
	return tool.metadata
}

func (tool *stdioMCPTool) Call(ctx context.Context, args []byte) (any, error) {
	if tool == nil || tool.owner == nil {
		return nil, fmt.Errorf("%w: MCP stdio tool is unavailable", ErrInvalidMCPBinding)
	}
	return tool.owner.callTool(ctx, tool.remoteName, args)
}

func cloneToolDeclaration(declaration *trpctool.Declaration) *trpctool.Declaration {
	if declaration == nil {
		return nil
	}
	clone := *declaration
	return &clone
}

func (set *stdioMCPToolSet) ensureConnectedLocked(ctx context.Context) error {
	if ctx == nil || ctx.Err() != nil {
		return fmt.Errorf("active context is required")
	}
	set.mu.RLock()
	closed := set.closed
	process := set.process
	set.mu.RUnlock()
	if closed {
		return fmt.Errorf("MCP stdio ToolSet is closed")
	}
	if process != nil && !process.doneNow() {
		return nil
	}
	if process != nil {
		set.dropProcess(process)
	}

	process, err := newStdioMCPProcess(set.command, set.args)
	if err != nil {
		return err
	}
	set.mu.Lock()
	if set.closed {
		set.mu.Unlock()
		_ = process.close()
		return fmt.Errorf("MCP stdio ToolSet is closed")
	}
	set.process = process
	set.mu.Unlock()

	if err := set.initializeProcess(ctx, process); err != nil {
		set.dropProcess(process)
		return err
	}
	return nil
}

func (set *stdioMCPToolSet) initializeProcess(ctx context.Context, process *stdioMCPProcess) error {
	params := map[string]any{
		"protocolVersion": "2025-03-26",
		"clientInfo":      map[string]string{"name": "trpc-agent-service", "version": "1"},
		"capabilities":    map[string]any{},
	}
	if _, err := process.request(ctx, set.timeout, "initialize", params); err != nil {
		return err
	}
	if err := process.notify(ctx, map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized", "params": map[string]any{}}); err != nil {
		return err
	}
	result, err := process.request(ctx, set.timeout, "tools/list", map[string]any{})
	if err != nil {
		return err
	}
	definitions, err := parseStdioMCPTools(result, set.allowed)
	if err != nil {
		return err
	}
	set.mu.Lock()
	set.tools = definitions
	set.mu.Unlock()
	return nil
}

func parseStdioMCPTools(raw json.RawMessage, allowed map[string]struct{}) (map[string]stdioMCPToolDefinition, error) {
	var result struct {
		Tools []struct {
			Name         string          `json:"name"`
			Description  string          `json:"description"`
			InputSchema  json.RawMessage `json:"inputSchema"`
			OutputSchema json.RawMessage `json:"outputSchema"`
			Annotations  struct {
				ReadOnlyHint    *bool `json:"readOnlyHint"`
				DestructiveHint *bool `json:"destructiveHint"`
				OpenWorldHint   *bool `json:"openWorldHint"`
			} `json:"annotations"`
		} `json:"tools"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode MCP tools/list: %w", err)
	}
	definitions := make(map[string]stdioMCPToolDefinition, len(result.Tools))
	for _, remote := range result.Tools {
		if _, ok := allowed[remote.Name]; !ok {
			continue
		}
		if remote.Name == "" {
			return nil, fmt.Errorf("MCP tools/list returned an unnamed allowed tool")
		}
		if _, duplicate := definitions[remote.Name]; duplicate {
			return nil, fmt.Errorf("MCP tools/list returned duplicate tool %q", remote.Name)
		}
		metadata := trpctool.ToolMetadata{}
		if remote.Annotations.ReadOnlyHint != nil {
			metadata.ReadOnly = *remote.Annotations.ReadOnlyHint
		}
		if remote.Annotations.DestructiveHint != nil {
			metadata.Destructive = *remote.Annotations.DestructiveHint
		}
		if remote.Annotations.OpenWorldHint != nil {
			metadata.OpenWorld = *remote.Annotations.OpenWorldHint
		}
		definitions[remote.Name] = stdioMCPToolDefinition{
			declaration: &trpctool.Declaration{
				Name: remote.Name, Description: remote.Description,
				InputSchema: decodeStdioSchema(remote.InputSchema), OutputSchema: decodeStdioSchema(remote.OutputSchema),
			}, metadata: metadata,
		}
	}
	for name := range allowed {
		if _, ok := definitions[name]; !ok {
			return nil, fmt.Errorf("MCP tools/list did not advertise allowed tool %q", name)
		}
	}
	return definitions, nil
}

func decodeStdioSchema(raw json.RawMessage) *trpctool.Schema {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil
	}
	var schema trpctool.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil
	}
	return &schema
}

func (set *stdioMCPToolSet) callTool(ctx context.Context, name string, args []byte) (any, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, contextError(ctx)
	}
	var arguments map[string]any
	if len(bytes.TrimSpace(args)) == 0 {
		arguments = map[string]any{}
	} else if err := json.Unmarshal(args, &arguments); err != nil {
		return nil, fmt.Errorf("invalid MCP tool arguments: %w", err)
	}

	set.requestMu.Lock()
	defer set.requestMu.Unlock()
	var lastErr error
	for attempt := 0; attempt <= stdioMCPReconnectAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err := set.ensureConnectedLocked(ctx); err != nil {
			lastErr = err
			if !mcpConnectionFailure(err) || attempt == stdioMCPReconnectAttempts {
				return nil, err
			}
			continue
		}
		set.mu.RLock()
		process := set.process
		_, allowed := set.allowed[name]
		set.mu.RUnlock()
		if !allowed {
			return nil, fmt.Errorf("MCP tool %q is not authorized", name)
		}
		result, err := process.request(ctx, set.timeout, "tools/call", map[string]any{"name": name, "arguments": arguments})
		if err == nil {
			var value any
			if len(result) == 0 || bytes.Equal(bytes.TrimSpace(result), []byte("null")) {
				return nil, nil
			}
			if decodeErr := json.Unmarshal(result, &value); decodeErr != nil {
				return nil, fmt.Errorf("decode MCP tools/call result: %w", decodeErr)
			}
			return value, nil
		}
		lastErr = err
		if ctx.Err() != nil || !mcpConnectionFailure(err) || attempt == stdioMCPReconnectAttempts {
			return nil, err
		}
		set.dropProcess(process)
	}
	return nil, lastErr
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	return ctx.Err()
}

func (set *stdioMCPToolSet) dropProcess(process *stdioMCPProcess) {
	if process == nil {
		return
	}
	set.mu.Lock()
	if set.process == process {
		set.process = nil
	}
	set.mu.Unlock()
	_ = process.close()
}

type stdioMCPProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.ReadCloser
	stderr io.ReadCloser
	enc    *json.Encoder

	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	doneOnce    sync.Once
	finishErrMu sync.RWMutex
	finishErr   error

	pendingMu sync.Mutex
	pending   map[string]chan stdioMCPResponse
	writeMu   sync.Mutex
	nextID    atomic.Int64
}

type stdioMCPResponse struct {
	result json.RawMessage
	err    error
}

type stdioMCPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func newStdioMCPProcess(command string, args []string) (*stdioMCPProcess, error) {
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, command, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create MCP stdio stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		cancel()
		return nil, fmt.Errorf("create MCP stdio stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		cancel()
		return nil, fmt.Errorf("create MCP stdio stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		cancel()
		return nil, fmt.Errorf("start MCP stdio process: %w", err)
	}
	process := &stdioMCPProcess{
		cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr, enc: json.NewEncoder(stdin),
		ctx: ctx, cancel: cancel, done: make(chan struct{}), pending: make(map[string]chan stdioMCPResponse),
	}
	go process.readLoop()
	go process.stderrLoop()
	go process.waitLoop()
	return process, nil
}

func (process *stdioMCPProcess) doneNow() bool {
	select {
	case <-process.done:
		return true
	default:
		return false
	}
}

func (process *stdioMCPProcess) request(ctx context.Context, timeout time.Duration, method string, params any) (json.RawMessage, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, contextError(ctx)
	}
	requestCtx, cancel := withMCPTimeout(ctx, timeout)
	defer cancel()
	id := process.nextID.Add(1)
	idBytes := []byte(strconv.FormatInt(id, 10))
	key := string(idBytes)
	responseCh := make(chan stdioMCPResponse, 1)
	process.pendingMu.Lock()
	if process.doneNow() {
		process.pendingMu.Unlock()
		return nil, process.failure()
	}
	process.pending[key] = responseCh
	process.pendingMu.Unlock()
	defer func() {
		process.pendingMu.Lock()
		delete(process.pending, key)
		process.pendingMu.Unlock()
	}()

	request := map[string]any{"jsonrpc": "2.0", "id": json.RawMessage(idBytes), "method": method, "params": params}
	process.writeMu.Lock()
	writeErr := process.enc.Encode(request)
	process.writeMu.Unlock()
	if writeErr != nil {
		return nil, fmt.Errorf("send MCP stdio request: %w", writeErr)
	}
	select {
	case response := <-responseCh:
		if response.err != nil {
			return nil, response.err
		}
		return response.result, nil
	case <-requestCtx.Done():
		return nil, requestCtx.Err()
	case <-process.done:
		return nil, process.failure()
	}
}

func (process *stdioMCPProcess) notify(ctx context.Context, notification any) error {
	if ctx == nil || ctx.Err() != nil {
		return contextError(ctx)
	}
	process.writeMu.Lock()
	defer process.writeMu.Unlock()
	if process.doneNow() {
		return process.failure()
	}
	if err := process.enc.Encode(notification); err != nil {
		return fmt.Errorf("send MCP stdio notification: %w", err)
	}
	return nil
}

func withMCPTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return ctx, func() {}
	}
	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, timeout)
}

func (process *stdioMCPProcess) readLoop() {
	decoder := json.NewDecoder(process.stdout)
	for {
		var envelope struct {
			ID     json.RawMessage `json:"id"`
			Result json.RawMessage `json:"result"`
			Error  *stdioMCPError  `json:"error"`
		}
		if err := decoder.Decode(&envelope); err != nil {
			if !process.doneNow() {
				process.finish(fmt.Errorf("read MCP stdio response: %w", err))
			}
			return
		}
		if len(bytes.TrimSpace(envelope.ID)) == 0 {
			continue
		}
		key := string(bytes.TrimSpace(envelope.ID))
		process.pendingMu.Lock()
		responseCh := process.pending[key]
		process.pendingMu.Unlock()
		if responseCh == nil {
			continue
		}
		response := stdioMCPResponse{result: append(json.RawMessage(nil), envelope.Result...)}
		if envelope.Error != nil {
			response.err = fmt.Errorf("MCP JSON-RPC error %d: %s", envelope.Error.Code, envelope.Error.Message)
		}
		select {
		case responseCh <- response:
		default:
		}
	}
}

func (process *stdioMCPProcess) stderrLoop() {
	_, _ = io.Copy(io.Discard, process.stderr)
}

func (process *stdioMCPProcess) waitLoop() {
	err := process.cmd.Wait()
	process.finish(err)
}

func (process *stdioMCPProcess) failure() error {
	process.finishErrMu.RLock()
	err := process.finishErr
	process.finishErrMu.RUnlock()
	if err == nil {
		return errors.New("MCP stdio process closed")
	}
	return err
}

func (process *stdioMCPProcess) finish(err error) {
	process.doneOnce.Do(func() {
		process.finishErrMu.Lock()
		process.finishErr = err
		process.finishErrMu.Unlock()
		close(process.done)
		process.pendingMu.Lock()
		pending := make([]chan stdioMCPResponse, 0, len(process.pending))
		for key, responseCh := range process.pending {
			pending = append(pending, responseCh)
			delete(process.pending, key)
		}
		process.pendingMu.Unlock()
		response := stdioMCPResponse{err: process.failureWithoutRecursion()}
		for _, responseCh := range pending {
			select {
			case responseCh <- response:
			default:
			}
		}
	})
}

func (process *stdioMCPProcess) failureWithoutRecursion() error {
	process.finishErrMu.RLock()
	err := process.finishErr
	process.finishErrMu.RUnlock()
	if err == nil {
		return errors.New("MCP stdio process closed")
	}
	return err
}

func (process *stdioMCPProcess) close() error {
	if process == nil {
		return nil
	}
	process.cancel()
	if process.cmd != nil && process.cmd.Process != nil {
		_ = process.cmd.Process.Kill()
	}
	select {
	case <-process.done:
		return nil
	case <-time.After(stdioMCPCloseWait):
		return fmt.Errorf("MCP stdio process did not stop")
	}
}
