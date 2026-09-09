package tool

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/jsonstrict"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const stdioMCPCloseWait = 2 * time.Second

// isolatedMCPEnvironment deliberately excludes inherited credentials, proxy
// settings, loader hooks, and other process-global secrets. The command is
// already validated as an absolute executable path, so it does not need the
// caller's PATH to resolve itself.
func isolatedMCPEnvironment() []string {
	if runtime.GOOS == "windows" {
		return []string{
			"PATH=C:\\Windows\\System32;C:\\Windows",
			"SystemRoot=C:\\Windows",
			"SYSTEMROOT=C:\\Windows",
			"LANG=C",
			"LC_ALL=C",
		}
	}
	return []string{
		"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
		"LANG=C",
		"LC_ALL=C",
	}
}

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
	if contextError(ctx) != nil {
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
		// Do not advertise stale declarations after a failed reconnect. A
		// caller may make a later explicit refresh attempt.
		return nil
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

// resetMCPConnection is intentionally independent of requestMu. It is called
// after a failed tools/call while the caller may still hold that mutex; it
// drops only the broken child and leaves the ToolSet available for the next
// explicit invocation.
func (set *stdioMCPToolSet) resetMCPConnection() {
	if set == nil {
		return
	}
	set.mu.Lock()
	process := set.process
	set.process = nil
	closed := set.closed
	set.mu.Unlock()
	if process != nil && !closed {
		_ = process.close()
	}
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
	if tool == nil || tool.owner == nil || !validMCPName(tool.remoteName) {
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
	if contextError(ctx) != nil {
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
	if !utf8.Valid(raw) {
		return nil, errMCPWireInvalidUTF8
	}
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
	if err := jsonstrict.Validate(raw, true); err != nil {
		return nil, fmt.Errorf("decode MCP tools/list: %w", err)
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("decode MCP tools/list: %w", err)
	}
	definitions := make(map[string]stdioMCPToolDefinition, len(result.Tools))
	seen := make(map[string]struct{}, len(result.Tools))
	for _, remote := range result.Tools {
		if !validMCPName(remote.Name) {
			return nil, fmt.Errorf("MCP tools/list returned an invalid tool name")
		}
		if _, duplicate := seen[remote.Name]; duplicate {
			return nil, fmt.Errorf("MCP tools/list returned duplicate tool %q", remote.Name)
		}
		seen[remote.Name] = struct{}{}
		if _, ok := allowed[remote.Name]; !ok {
			continue
		}
		inputSchema, schemaErr := decodeStdioSchema(remote.InputSchema)
		if schemaErr != nil || inputSchema == nil {
			return nil, fmt.Errorf("MCP tools/list returned an invalid input schema for %q", remote.Name)
		}
		if !validMCPText(remote.Description) {
			return nil, fmt.Errorf("MCP tools/list returned an invalid description for %q", remote.Name)
		}
		outputSchema, schemaErr := decodeStdioSchema(remote.OutputSchema)
		if schemaErr != nil {
			return nil, fmt.Errorf("MCP tools/list returned an invalid output schema for %q", remote.Name)
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
				InputSchema: inputSchema, OutputSchema: outputSchema,
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

func decodeStdioSchema(raw json.RawMessage) (*trpctool.Schema, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if err := jsonstrict.Validate(raw, false); err != nil {
		return nil, err
	}
	trimmed := bytes.Trim(raw, " \t\r\n")
	if bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if err := jsonstrict.Validate(raw, true); err != nil {
		return nil, err
	}
	var schema trpctool.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, err
	}
	return &schema, nil
}

func (set *stdioMCPToolSet) callTool(ctx context.Context, name string, args []byte) (any, error) {
	if set == nil {
		return nil, fmt.Errorf("%w: MCP stdio ToolSet is unavailable", ErrInvalidMCPBinding)
	}
	if contextError(ctx) != nil {
		return nil, contextError(ctx)
	}
	if !validMCPArguments(args) {
		return nil, ErrMCPInvalidArguments
	}
	var arguments map[string]any
	if len(bytes.Trim(args, " \t\r\n")) == 0 {
		arguments = map[string]any{}
	} else if err := json.Unmarshal(args, &arguments); err != nil {
		return nil, ErrMCPInvalidArguments
	}

	set.requestMu.Lock()
	defer set.requestMu.Unlock()
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if err := set.ensureConnectedLocked(ctx); err != nil {
		return nil, err
	}
	set.mu.RLock()
	process := set.process
	_, allowed := set.allowed[name]
	set.mu.RUnlock()
	if !allowed || process == nil {
		return nil, fmt.Errorf("MCP tool %q is not authorized", name)
	}
	result, err := process.request(ctx, set.timeout, "tools/call", map[string]any{"name": name, "arguments": arguments})
	if err != nil {
		// Close a broken transport, but never send tools/call again here. The
		// lifecycle wrapper may refresh it for a later explicit call.
		if contextError(ctx) == nil && mcpConnectionFailure(err) {
			set.dropProcess(process)
		}
		return nil, err
	}
	var value any
	if len(result) == 0 || bytes.Equal(bytes.Trim(result, " \t\r\n"), []byte("null")) {
		return nil, nil
	}
	if err := jsonstrict.Validate(result, false); err != nil {
		return nil, fmt.Errorf("decode MCP tools/call result: %w", err)
	}
	if decodeErr := json.Unmarshal(result, &value); decodeErr != nil {
		return nil, fmt.Errorf("decode MCP tools/call result: %w", decodeErr)
	}
	return value, nil
}

func contextError(ctx context.Context) error {
	if err := nilvalue.ContextErr(ctx); err != nil {
		if errors.Is(err, nilvalue.ErrInvalidContext) {
			return errors.New("context is unavailable")
		}
		return err
	}
	return nil
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
	workDir     string
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
	workDir, err := os.MkdirTemp("", "trpc-mcp-")
	if err != nil {
		cancel()
		return nil, fmt.Errorf("create MCP stdio work directory: %w", err)
	}
	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Dir = workDir
	cmd.Env = isolatedMCPEnvironment()
	configureMCPProcess(cmd)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		_ = os.RemoveAll(workDir)
		cancel()
		return nil, fmt.Errorf("create MCP stdio stdin: %w", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		_ = os.RemoveAll(workDir)
		cancel()
		return nil, fmt.Errorf("create MCP stdio stdout: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = os.RemoveAll(workDir)
		cancel()
		return nil, fmt.Errorf("create MCP stdio stderr: %w", err)
	}
	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		_ = os.RemoveAll(workDir)
		cancel()
		return nil, fmt.Errorf("start MCP stdio process: %w", err)
	}
	process := &stdioMCPProcess{
		cmd: cmd, stdin: stdin, stdout: stdout, stderr: stderr, enc: json.NewEncoder(stdin),
		ctx: ctx, cancel: cancel, workDir: workDir, done: make(chan struct{}), pending: make(map[string]chan stdioMCPResponse),
	}
	go process.readLoop()
	go process.stderrLoop()
	go process.waitLoop()
	return process, nil
}

func (process *stdioMCPProcess) doneNow() bool {
	if process == nil || process.done == nil {
		return true
	}
	select {
	case <-process.done:
		return true
	default:
		return false
	}
}

func (process *stdioMCPProcess) request(ctx context.Context, timeout time.Duration, method string, params any) (json.RawMessage, error) {
	if process == nil {
		return nil, errors.New("MCP stdio process is unavailable")
	}
	if contextError(ctx) != nil {
		return nil, contextError(ctx)
	}
	requestCtx, cancel, timeoutErr := withMCPTimeout(ctx, timeout)
	if timeoutErr != nil {
		return nil, timeoutErr
	}
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
	requestDone, requestDoneErr := nilvalue.ContextDone(requestCtx)
	if requestDoneErr != nil {
		return nil, requestDoneErr
	}
	select {
	case response := <-responseCh:
		if response.err != nil {
			return nil, response.err
		}
		return response.result, nil
	case <-requestDone:
		return nil, nilvalue.ContextErr(requestCtx)
	case <-process.done:
		return nil, process.failure()
	}
}

func (process *stdioMCPProcess) notify(ctx context.Context, notification any) error {
	if process == nil {
		return errors.New("MCP stdio process is unavailable")
	}
	if contextError(ctx) != nil {
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

func withMCPTimeout(ctx context.Context, timeout time.Duration) (requestCtx context.Context, cancel context.CancelFunc, err error) {
	if nilvalue.Is(ctx) {
		return nil, func() {}, errors.New("context is unavailable")
	}
	if contextErr := contextError(ctx); contextErr != nil {
		return nil, func() {}, contextErr
	}
	defer func() {
		if recover() != nil {
			requestCtx = nil
			cancel = func() {}
			err = errors.New("context is unavailable")
		}
	}()
	// Always derive a standard context. Besides giving the request a stable
	// Done channel, this turns a panic from a custom parent Deadline/Done
	// implementation into the error returned above rather than allowing it to
	// escape from the stdio request boundary.
	base, baseCancel := context.WithCancel(ctx)
	if timeout <= 0 {
		return base, baseCancel, nil
	}
	if _, hasDeadline := base.Deadline(); hasDeadline {
		return base, baseCancel, nil
	}
	requestCtx, timeoutCancel := context.WithTimeout(base, timeout)
	return requestCtx, func() {
		timeoutCancel()
		baseCancel()
	}, nil
}

func (process *stdioMCPProcess) readLoop() {
	reader := bufio.NewReaderSize(process.stdout, 64<<10)
	for {
		line, err := readMCPStdioLine(reader)
		if err != nil {
			if !process.doneNow() {
				process.finish(fmt.Errorf("read MCP stdio response: %w", err))
			}
			return
		}
		if len(bytes.Trim(line, " \t\r\n")) == 0 {
			continue
		}
		if !utf8.Valid(line) {
			if !process.doneNow() {
				process.finish(fmt.Errorf("decode MCP stdio response: %w", errMCPWireInvalidUTF8))
			}
			return
		}
		if err := jsonstrict.Validate(line, true); err != nil {
			if !process.doneNow() {
				process.finish(fmt.Errorf("decode MCP stdio response: %w", err))
			}
			return
		}
		var envelope struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      json.RawMessage `json:"id"`
			Result  json.RawMessage `json:"result"`
			Error   *stdioMCPError  `json:"error"`
		}
		if err := json.Unmarshal(line, &envelope); err != nil {
			if !process.doneNow() {
				process.finish(fmt.Errorf("decode MCP stdio response: %w", err))
			}
			return
		}
		if envelope.JSONRPC != "2.0" || (len(bytes.Trim(envelope.Result, " \t\r\n")) == 0 && envelope.Error == nil) || (len(bytes.Trim(envelope.Result, " \t\r\n")) > 0 && envelope.Error != nil) {
			if !process.doneNow() {
				process.finish(errors.New("MCP stdio response has an invalid JSON-RPC envelope"))
			}
			return
		}
		if len(bytes.Trim(envelope.ID, " \t\r\n")) == 0 {
			continue
		}
		key := string(bytes.Trim(envelope.ID, " \t\r\n"))
		process.pendingMu.Lock()
		responseCh := process.pending[key]
		process.pendingMu.Unlock()
		if responseCh == nil {
			continue
		}
		response := stdioMCPResponse{result: append(json.RawMessage(nil), envelope.Result...)}
		if envelope.Error != nil {
			// Provider-controlled error messages can contain credentials or
			// arbitrary payloads. Keep the protocol-facing error stable; the
			// governed adapter redacts it further for the model.
			response.err = fmt.Errorf("MCP JSON-RPC error %d", envelope.Error.Code)
		}
		select {
		case responseCh <- response:
		default:
		}
	}
}

func readMCPStdioLine(reader *bufio.Reader) ([]byte, error) {
	if reader == nil {
		return nil, errors.New("MCP stdio reader is unavailable")
	}
	line := make([]byte, 0, 256)
	for {
		part, isPrefix, err := reader.ReadLine()
		if len(line)+len(part) > int(maxMCPWireBytes) {
			return nil, errMCPWireTooLarge
		}
		line = append(line, part...)
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) > 0 {
				return line, nil
			}
			return nil, err
		}
		if !isPrefix {
			return line, nil
		}
	}
}

func (process *stdioMCPProcess) stderrLoop() {
	_, _ = io.Copy(io.Discard, process.stderr)
}

func (process *stdioMCPProcess) waitLoop() {
	err := process.cmd.Wait()
	process.finish(err)
	if process.workDir != "" {
		_ = os.RemoveAll(process.workDir)
	}
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
		killMCPProcess(process.cmd)
	}
	select {
	case <-process.done:
		return nil
	case <-time.After(stdioMCPCloseWait):
		return fmt.Errorf("MCP stdio process did not stop")
	}
}
