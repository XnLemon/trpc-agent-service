package agent

import (
	"context"
	"errors"
	"sync"
	"time"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimebudget "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"github.com/google/uuid"
	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

// RunnerInput is the dependency-free execution contract consumed by the
// external-agent adapter. Runtime builds it from its complete execution plan;
// this package owns the tRPC-Agent-Go materialization after that boundary.
type RunnerInput struct {
	Tenant  tenant.Tenant
	Agent   LLMAgentFactoryInput
	Model   modelprofile.ModelFactoryInput
	Storage backend.StorageFactoryInput
}

func llmAgentOptions(input LLMAgentFactoryInput, model trpcmodel.Model, toolSets ...[]trpctool.Tool) []llmagent.Option {
	options := []llmagent.Option{
		llmagent.WithDescription(input.Description),
		llmagent.WithInstruction(input.Instruction),
		llmagent.WithGlobalInstruction(input.GlobalInstruction),
		llmagent.WithModel(model),
		llmagent.WithGenerationConfig(toTRPCGenerationConfig(input.Generation)),
		llmagent.WithMaxLLMCalls(input.Runtime.MaxLLMCalls),
		llmagent.WithMaxToolIterations(input.Runtime.MaxToolCalls),
		llmagent.WithEnableParallelTools(input.Runtime.EnableParallelTools),
		llmagent.WithToolConcurrencyConfig(trpctool.ConcurrencyConfig{MaxConcurrency: input.Runtime.MaxParallelTools}),
	}
	if len(toolSets) > 0 && len(toolSets[0]) > 0 {
		options = append(options, llmagent.WithTools(toolSets[0]))
	}
	return options
}

type callbackStateKey struct{}
type usageObserverContextKey struct{}

// UsageObserver receives one terminal provider usage sample for each model
// call. It is carried by execution context so cached Runners can be reused
// without sharing accounting state between requests.
type UsageObserver func(context.Context, runtimebudget.Usage)

// WithUsageObserver attaches per-execution usage accounting to a context.
func WithUsageObserver(ctx context.Context, observer UsageObserver) context.Context {
	if isNilAgentValue(ctx) || observer == nil {
		return ctx
	}
	return context.WithValue(ctx, usageObserverContextKey{}, observer)
}

func usageObserverFromContext(ctx context.Context) UsageObserver {
	if isNilAgentValue(ctx) {
		return nil
	}
	observer, _ := ctx.Value(usageObserverContextKey{}).(UsageObserver)
	return observer
}

type callbackState struct {
	finishSpan func(error)
	started    time.Time
	ctx        context.Context
	catalog    metrics.Catalog
	labels     map[string]string
	mu         sync.Mutex
	once       sync.Once
	usage      *trpcmodel.Usage
}

var errModelResponseIncomplete = errors.New("model response stream incomplete")

func (state *callbackState) observe(response *trpcmodel.Response) {
	if state == nil || response == nil || response.Usage == nil {
		return
	}
	usage := *response.Usage
	state.mu.Lock()
	state.usage = &usage
	state.mu.Unlock()
}

func (state *callbackState) finish(err error) {
	if state == nil {
		return
	}
	state.once.Do(func() {
		state.mu.Lock()
		usage := state.usage
		state.mu.Unlock()
		if usage != nil {
			labels := make(map[string]string, len(state.labels))
			for key, value := range state.labels {
				if key != "operation" && key != "status" && key != "error_class" {
					labels[key] = value
				}
			}
			_ = state.catalog.Tokens(state.ctx, int64(usage.PromptTokens), labels)
			_ = state.catalog.Tokens(state.ctx, int64(usage.CompletionTokens), labels)
			if observer := usageObserverFromContext(state.ctx); observer != nil {
				safeUsageObserver(observer, state.ctx, runtimebudget.Usage{InputTokens: int64(usage.PromptTokens), OutputTokens: int64(usage.CompletionTokens)})
			}
		}
		if state.finishSpan != nil {
			safeFinishSpan(state.finishSpan, err)
		}
		_ = state.catalog.Operation(state.ctx, state.started, state.labels, err)
	})
}

func stateFromContext(ctx context.Context) *callbackState {
	if isNilAgentValue(ctx) {
		return nil
	}
	state, _ := ctx.Value(callbackStateKey{}).(*callbackState)
	return state
}

func safeUsageObserver(observer UsageObserver, ctx context.Context, usage runtimebudget.Usage) {
	defer func() { _ = recover() }()
	observer(ctx, usage)
}

func safeFinishSpan(finish func(error), err error) {
	defer func() { _ = recover() }()
	finish(err)
}

func isTerminalModelResponse(response *trpcmodel.Response) bool {
	return response != nil && (response.Error != nil || response.Done || !response.IsPartial)
}

func modelResponseError(response *trpcmodel.Response) error {
	if response != nil && response.Error != nil {
		if errors.Is(response.Error, context.Canceled) {
			return context.Canceled
		}
		if errors.Is(response.Error, context.DeadlineExceeded) {
			return context.DeadlineExceeded
		}
		return errModelResponse
	}
	return nil
}

var errModelResponse = errors.New("model response error")

// telemetryModel closes model spans when a provider returns a function-level
// error or a response stream ends without a terminal response. The framework's
// callbacks still own usage extraction; this wrapper only supplies the missing
// terminal signal for streaming and pre-channel failures.
type telemetryModel struct{ delegate trpcmodel.Model }

func wrapTelemetryModel(delegate trpcmodel.Model) trpcmodel.Model {
	if isNilAgentValue(delegate) {
		return nil
	}
	if iter, ok := delegate.(trpcmodel.IterModel); ok {
		return telemetryIterModel{telemetryModel: telemetryModel{delegate: delegate}, iter: iter}
	}
	return telemetryModel{delegate: delegate}
}

func (model telemetryModel) Info() (info trpcmodel.Info) {
	defer func() {
		if recover() != nil {
			info = trpcmodel.Info{}
		}
	}()
	if isNilAgentValue(model.delegate) {
		return trpcmodel.Info{}
	}
	return model.delegate.Info()
}

func (model telemetryModel) GenerateContent(ctx context.Context, request *trpcmodel.Request) (responses <-chan *trpcmodel.Response, err error) {
	if isNilAgentValue(ctx) || isNilAgentValue(model.delegate) {
		return nil, errModelResponse
	}
	defer func() {
		if recover() != nil {
			responses, err = nil, errModelResponse
		}
	}()
	responses, err = model.delegate.GenerateContent(ctx, request)
	state := stateFromContext(ctx)
	if err != nil {
		state.finish(err)
		return nil, err
	}
	if responses == nil {
		state.finish(errModelResponseIncomplete)
		return nil, errModelResponseIncomplete
	}
	source := responses
	out := make(chan *trpcmodel.Response)
	go func() {
		defer close(out)
		terminal := false
		defer func() {
			if recover() != nil && !terminal {
				state.finish(errModelResponse)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				if !terminal {
					state.finish(agentContextErr(ctx))
				}
				return
			case response, ok := <-source:
				if !ok {
					if !terminal {
						state.finish(errModelResponseIncomplete)
					}
					return
				}
				if response != nil {
					state.observe(response)
					if isTerminalModelResponse(response) {
						terminal = true
						state.finish(modelResponseError(response))
					}
				}
				select {
				case out <- response:
				case <-ctx.Done():
					if !terminal {
						state.finish(agentContextErr(ctx))
					}
					return
				}
			}
		}
	}()
	return out, nil
}

type telemetryIterModel struct {
	telemetryModel
	iter trpcmodel.IterModel
}

func (model telemetryIterModel) GenerateContentIter(ctx context.Context, request *trpcmodel.Request) (sequence trpcmodel.Seq[*trpcmodel.Response], err error) {
	if isNilAgentValue(ctx) || isNilAgentValue(model.iter) {
		return nil, errModelResponse
	}
	defer func() {
		if recover() != nil {
			sequence, err = nil, errModelResponse
		}
	}()
	seq, err := model.iter.GenerateContentIter(ctx, request)
	state := stateFromContext(ctx)
	if err != nil {
		state.finish(err)
		return nil, err
	}
	if seq == nil {
		state.finish(errModelResponseIncomplete)
		return nil, errModelResponseIncomplete
	}
	return func(yield func(*trpcmodel.Response) bool) {
		if yield == nil {
			state.finish(errModelResponse)
			return
		}
		terminal := false
		defer func() {
			if recover() != nil && !terminal {
				state.finish(errModelResponse)
			}
		}()
		seq(func(response *trpcmodel.Response) bool {
			if response != nil {
				state.observe(response)
				if isTerminalModelResponse(response) {
					terminal = true
					state.finish(modelResponseError(response))
				}
			}
			return yield(response)
		})
		if !terminal {
			if err := agentContextErr(ctx); err != nil {
				state.finish(err)
			} else {
				state.finish(errModelResponseIncomplete)
			}
		}
	}, nil
}

type modelRetryBinder interface {
	WithModelRetryCallbacks(context.Context, func(context.Context, *trpcmodel.Request) (context.Context, *trpcmodel.Response, error), func(context.Context, *trpcmodel.Request, *trpcmodel.Response) (context.Context, error)) context.Context
}

func (model telemetryModel) WithModelRetryCallbacks(ctx context.Context, before func(context.Context, *trpcmodel.Request) (context.Context, *trpcmodel.Response, error), after func(context.Context, *trpcmodel.Request, *trpcmodel.Response) (context.Context, error)) (result context.Context) {
	if isNilAgentValue(ctx) {
		return ctx
	}
	binder, ok := model.delegate.(modelRetryBinder)
	if !ok || isNilAgentValue(binder) {
		return ctx
	}
	result = ctx
	defer func() {
		if recover() != nil || isNilAgentValue(result) {
			result = ctx
		}
	}()
	return binder.WithModelRetryCallbacks(ctx, before, after)
}

func telemetryOptions(provider observability.Provider, providerName, modelFamily string) []llmagent.Option {
	catalog := metrics.New(provider)
	modelCallbacks := trpcmodel.NewCallbacks().RegisterBeforeModel(func(ctx context.Context, _ *trpcmodel.BeforeModelArgs) (*trpcmodel.BeforeModelResult, error) {
		started := time.Now()
		next, _, finish := observability.StartOperation(ctx, provider, observability.OperationModelCall, "model")
		labels := map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": providerName, "model_family": modelFamily}
		startedLabels := make(map[string]string, len(labels)+1)
		for key, value := range labels {
			startedLabels[key] = value
		}
		startedLabels["status"] = "started"
		_ = catalog.Request(next, startedLabels)
		state := &callbackState{finishSpan: finish, started: started, ctx: next, catalog: catalog, labels: labels}
		return &trpcmodel.BeforeModelResult{Context: context.WithValue(next, callbackStateKey{}, state)}, nil
	}).RegisterAfterModel(func(ctx context.Context, args *trpcmodel.AfterModelArgs) (*trpcmodel.AfterModelResult, error) {
		state := stateFromContext(ctx)
		if state == nil {
			return nil, nil
		}
		if args == nil {
			state.finish(errModelResponseIncomplete)
			return nil, nil
		}
		state.observe(args.Response)
		if args.Response == nil || isTerminalModelResponse(args.Response) || args.Error != nil {
			err := args.Error
			if err == nil && args.Response != nil {
				err = modelResponseError(args.Response)
			}
			if err == nil && args.Response == nil {
				err = errModelResponseIncomplete
			}
			state.finish(err)
		}
		return nil, nil
	})
	toolCallbacks := trpctool.NewCallbacks().RegisterBeforeTool(func(ctx context.Context, _ *trpctool.BeforeToolArgs) (*trpctool.BeforeToolResult, error) {
		started := time.Now()
		next, _, finish := observability.StartOperation(ctx, provider, observability.OperationToolCall, "tool")
		labels := map[string]string{"component": "tool", "operation": observability.OperationToolCall}
		startedLabels := map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "started"}
		_ = catalog.Request(next, startedLabels)
		state := &callbackState{finishSpan: finish, started: started, ctx: next, catalog: catalog, labels: labels}
		return &trpctool.BeforeToolResult{Context: context.WithValue(next, callbackStateKey{}, state)}, nil
	}).RegisterAfterTool(func(ctx context.Context, args *trpctool.AfterToolArgs) (*trpctool.AfterToolResult, error) {
		state := stateFromContext(ctx)
		if state == nil {
			return nil, nil
		}
		if args == nil {
			state.finish(errModelResponseIncomplete)
		} else {
			state.finish(args.Error)
		}
		return nil, nil
	})
	return []llmagent.Option{llmagent.WithModelCallbacks(modelCallbacks), llmagent.WithToolCallbacks(toolCallbacks)}
}

// policyRunner preserves the published Agent runtime policy at the Runner
// boundary. Caller-provided options are applied first; the fixed policy is
// appended last so an execution cannot silently extend its control-plane limits.
type policyRunner struct {
	delegate        trpcrunner.Runner
	capabilities    *storagefactory.CapabilitySet
	toolSets        []trpctool.ToolSet
	runOptions      []trpcagent.RunOption
	tenantID        string
	appID           string
	revision        int64
	toolInvocations runtimestorage.ToolInvocationStore
	closeOnce       sync.Once
	closeErr        error
}

func (runner *policyRunner) Run(ctx context.Context, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	if runner == nil || isNilAgentValue(runner.delegate) || isNilAgentValue(ctx) {
		return nil, ErrInvalid
	}
	runCtx := ctx
	metadata, metadataPresent := executionMetadataValue(ctx)
	if metadataPresent {
		if !validExecutionMetadata(metadata) {
			return nil, ErrInvalid
		}
		if metadata.TenantID != runner.tenantID || metadata.AppID != runner.appID || metadata.Revision != runner.revision || metadata.UserID != userID || metadata.SessionID != sessionID {
			return nil, ErrKnowledgeScope
		}
		if metadata.RequestID == "" {
			metadata.RequestID = "runner-" + uuid.NewString()
			runCtx = WithExecutionMetadata(ctx, metadata)
		}
	} else {
		// Direct Runner callers (for example a trusted queue worker) do not
		// pass through Gateway. Install the same immutable scope here rather
		// than allowing a context-aware capability to run without a request
		// boundary. A partially populated caller context is only a correlation
		// source; its tenant, app, user, and session values cannot retarget the
		// fixed Runner.
		requestID := "runner-" + uuid.NewString()
		traceID := ""
		eventID := requestID
		if execution, executionErr := servicetool.ExecutionContextFromContext(ctx); executionErr == nil {
			if execution.TenantID != runner.tenantID || (execution.AppID != "" && execution.AppID != runner.appID) || (execution.UserID != "" && execution.UserID != userID) || (execution.SessionID != "" && execution.SessionID != sessionID) {
				return nil, ErrKnowledgeScope
			}
			traceID = execution.TraceID
			if execution.EventID != "" {
				eventID = execution.EventID
			}
		}
		if userID == "" {
			userID = "runner-user"
		}
		if sessionID == "" {
			sessionID = "runner-session"
		}
		metadata = ExecutionMetadata{
			TenantID: runner.tenantID, AppID: runner.appID, Revision: runner.revision,
			PrincipalKind: "runner", PrincipalID: "runner", UserID: userID,
			SessionID: sessionID, EventID: eventID, RequestID: requestID, TraceID: traceID,
		}
		runCtx = WithExecutionMetadata(ctx, metadata)
	}
	// Gateway already installs a complete tool context. Direct Runner callers
	// receive one here as well, and the cached Runner's fixed ledger replaces
	// any caller-supplied store so app scope cannot be retargeted through a
	// context value. Inspect the raw value so malformed cross-scope fields are
	// rejected rather than mistaken for an absent context.
	execution, executionPresent := servicetool.RawExecutionContextFromContext(runCtx)
	if executionPresent {
		if !servicetool.ValidExecutionContextIdentity(execution) {
			return nil, ErrInvalid
		}
		if (execution.TenantID != "" && execution.TenantID != runner.tenantID) || (execution.AppID != "" && execution.AppID != runner.appID) || (execution.UserID != "" && execution.UserID != metadata.UserID) || (execution.SessionID != "" && execution.SessionID != metadata.SessionID) || (metadataPresent && execution.RequestID != "" && execution.RequestID != metadata.RequestID) {
			return nil, ErrKnowledgeScope
		}
		if metadataPresent && metadata.EventID != "" {
			if execution.EventID != "" && execution.EventID != metadata.EventID {
				return nil, ErrKnowledgeScope
			}
			execution.EventID = metadata.EventID
		} else if !metadataPresent {
			if execution.EventID == "" {
				execution.EventID = metadata.RequestID
			}
			metadata.EventID = execution.EventID
			runCtx = WithExecutionMetadata(runCtx, metadata)
		} else if execution.EventID == "" {
			execution.EventID = metadata.RequestID
		}
		execution.TenantID = metadata.TenantID
		execution.AppID = metadata.AppID
		execution.UserID = metadata.UserID
		execution.SessionID = metadata.SessionID
		execution.RequestID = metadata.RequestID
		execution.TraceID = metadata.TraceID
		execution.ToolInvocations = runner.toolInvocations
		runCtx = servicetool.WithExecutionContext(runCtx, execution)
	} else if !isNilAgentValue(runner.toolInvocations) {
		eventID := metadata.EventID
		if eventID == "" {
			eventID = metadata.RequestID
		}
		runCtx = servicetool.WithExecutionContext(runCtx, servicetool.ExecutionContext{
			TenantID: metadata.TenantID, AppID: metadata.AppID, UserID: metadata.UserID,
			SessionID: metadata.SessionID, EventID: eventID, RequestID: metadata.RequestID,
			TraceID: metadata.TraceID, ToolInvocations: runner.toolInvocations,
		})
	}
	allOptions := make([]trpcagent.RunOption, 0, len(options)+len(runner.runOptions)+2)
	allOptions = append(allOptions, options...)
	allOptions = append(allOptions, runner.runOptions...)
	// Both request correlation and AppName are immutable platform fields.
	// Append them after caller options so native RunOptions cannot retarget
	// audit/ledger identity or Memory/Artifact namespaces.
	allOptions = append(allOptions, trpcagent.WithRequestID(metadata.RequestID), trpcagent.WithAppName(runner.appID))
	return callDelegateRun(runCtx, runner.delegate, userID, sessionID, message, allOptions...)
}

func callDelegateRun(ctx context.Context, delegate trpcrunner.Runner, userID, sessionID string, message trpcmodel.Message, options ...trpcagent.RunOption) (events <-chan *trpcevent.Event, err error) {
	if isNilAgentValue(ctx) || isNilAgentValue(delegate) {
		return nil, ErrInvalid
	}
	defer func() {
		if recover() != nil {
			events = nil
			err = ErrInvalid
		}
	}()
	return delegate.Run(ctx, userID, sessionID, message, options...)
}

func (runner *policyRunner) Close() error {
	if runner == nil {
		return nil
	}
	runner.closeOnce.Do(func() {
		if !isNilAgentValue(runner.delegate) {
			runner.closeErr = errors.Join(runner.closeErr, closeAgentRunner(runner.delegate))
		}
		if runner.capabilities != nil {
			runner.closeErr = errors.Join(runner.closeErr, closeCapabilitySet(runner.capabilities))
		}
		runner.closeErr = errors.Join(runner.closeErr, closeToolSets(runner.toolSets))
	})
	return runner.closeErr
}

func closeCapabilitySet(set *storagefactory.CapabilitySet) (err error) {
	if set == nil {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errors.New("agent capability close failed")
		}
	}()
	return set.Close()
}

func closeAgentRunner(runner trpcrunner.Runner) (err error) {
	if isNilAgentValue(runner) {
		return nil
	}
	defer func() {
		if recover() != nil {
			err = errors.New("agent runner close failed")
		}
	}()
	return runner.Close()
}

func toTRPCGenerationConfig(configuration appmodel.GenerationConfig) trpcmodel.GenerationConfig {
	return trpcmodel.GenerationConfig{
		Temperature: configuration.Temperature,
		TopP:        configuration.TopP,
		MaxTokens:   configuration.MaxOutputTokens,
	}
}
