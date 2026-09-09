package tool

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	approvalreview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
	trpcagenttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/jsonstrict"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

var errToolInvocationState = errors.New("tool invocation state unavailable")

// RecordToolInvocationAudit emits the stable audit fact for the persisted
// invocation state. CreatedAt is used as the event timestamp so a retry after
// a committed state transition has the same digest and event ID; no arguments,
// results, or credentials are included.
func RecordToolInvocationAudit(ctx context.Context, recorder audit.Recorder, value runtimestorage.ToolInvocation) error {
	if isNilMCPValue(ctx) || !recorder.Configured() || runtimestorage.ValidateToolInvocation(value) != nil {
		return errToolInvocationState
	}
	eventType := audit.EventType("")
	decision := audit.Decision("")
	errorType := ""
	reason := ""
	actorType, actorID := "", ""
	switch value.Status {
	case runtimestorage.ToolInvocationAccepted:
		eventType, decision = audit.EventToolAllowed, audit.DecisionAllow
	case runtimestorage.ToolInvocationSucceeded:
		eventType, decision = audit.EventToolExecuted, audit.DecisionAccepted
	case runtimestorage.ToolInvocationManual:
		eventType, decision = audit.EventToolApprovalRequired, audit.DecisionApprovalRequired
	case runtimestorage.ToolInvocationDenied, runtimestorage.ToolInvocationFailed:
		eventType, decision, errorType = audit.EventToolDenied, audit.DecisionDeny, string(audit.ErrorTool)
	case runtimestorage.ToolInvocationUnknown:
		eventType, decision, errorType = audit.EventToolReconciliationRequired, audit.DecisionApprovalRequired, string(audit.ErrorTool)
		reason = "provider result uncertain; manual reconciliation required"
		actorType, actorID = "system", "tool-invocation-recovery"
	default:
		return nil
	}
	occurredAt := value.CreatedAt.UTC()
	if occurredAt.IsZero() {
		occurredAt = time.Now().UTC()
	}
	return recorder.Record(ctx, audit.Event{
		EventType: eventType,
		EventID:   audit.NewEventID(string(eventType), value.TenantID, value.AppID, value.RequestID, value.TraceID, value.InvocationID),
		TenantID:  value.TenantID, AgentAppID: value.AppID, RequestID: value.RequestID, TraceID: value.TraceID,
		ToolName: value.ToolName, Decision: decision, ErrorType: errorType,
		ActorType: actorType, ActorID: actorID, Reason: reason, OccurredAt: occurredAt,
	})
}

type toolInvocationState struct {
	store        runtimestorage.ToolInvocationStore
	tenantID     string
	appID        string
	invocationID string
	eventID      string
	requestID    string
	traceID      string
	toolCallID   string
	toolName     string
	status       runtimestorage.ToolInvocationStatus
	fence        int64
	argsSHA256   string
}

type toolInvocationStateKey struct{}

func callPrepareToolInvocation(ctx context.Context, store runtimestorage.ToolInvocationStore, input runtimestorage.ToolInvocationInput) (value runtimestorage.ToolInvocation, err error) {
	if isNilMCPValue(ctx) || isNilMCPValue(store) {
		return runtimestorage.ToolInvocation{}, errToolInvocationState
	}
	defer func() {
		if recover() != nil {
			value = runtimestorage.ToolInvocation{}
			err = errToolInvocationState
		}
	}()
	return store.PrepareToolInvocation(ctx, input)
}

func callGetToolInvocation(ctx context.Context, store runtimestorage.ToolInvocationStore, tenantID, appID, invocationID string) (value runtimestorage.ToolInvocation, err error) {
	if isNilMCPValue(ctx) || isNilMCPValue(store) {
		return runtimestorage.ToolInvocation{}, errToolInvocationState
	}
	defer func() {
		if recover() != nil {
			value = runtimestorage.ToolInvocation{}
			err = errToolInvocationState
		}
	}()
	return store.GetToolInvocation(ctx, tenantID, appID, invocationID)
}

func callTransitionToolInvocation(ctx context.Context, store runtimestorage.ToolInvocationStore, transition runtimestorage.ToolInvocationTransition) (value runtimestorage.ToolInvocation, err error) {
	if isNilMCPValue(ctx) || isNilMCPValue(store) {
		return runtimestorage.ToolInvocation{}, errToolInvocationState
	}
	defer func() {
		if recover() != nil {
			value = runtimestorage.ToolInvocation{}
			err = errToolInvocationState
		}
	}()
	return store.TransitionToolInvocation(ctx, transition)
}

// NewToolInvocationPlugins returns two runner-scoped plugins. The first
// durably prepares a call; the second is registered after guardrails and marks
// it dispatching only when all earlier BeforeTool checks have passed. Splitting
// the phases prevents an approval denial from being mistaken for a dispatched
// side effect.
func NewToolInvocationPlugins() []plugin.Plugin {
	return []plugin.Plugin{NewToolInvocationPreparePlugin(), NewToolInvocationDispatchPlugin()}
}

// NewToolInvocationPreparePlugin creates the prepare phase plugin. It is
// exported so the process composition layer can place it before guardrails.
func NewToolInvocationPreparePlugin() plugin.Plugin { return toolInvocationPreparePlugin{} }

// NewToolInvocationDispatchPlugin creates the dispatch/finalize phase plugin.
// It must be placed after guardrail plugins in the Runner plugin order.
func NewToolInvocationDispatchPlugin() plugin.Plugin { return toolInvocationDispatchPlugin{} }

type toolInvocationPreparePlugin struct{}

func (toolInvocationPreparePlugin) Name() string { return "tool_invocation_prepare" }

func (toolInvocationPreparePlugin) Register(registry *plugin.Registry) {
	registry.BeforeTool(func(ctx context.Context, args *trpcagenttool.BeforeToolArgs) (*trpcagenttool.BeforeToolResult, error) {
		execution, err := ExecutionContextFromContext(ctx)
		if err != nil || !validToolInvocationExecution(execution) {
			return nil, errToolInvocationState
		}
		input, stateErr := newToolInvocationInput(execution, args)
		if stateErr != nil {
			return nil, stateErr
		}
		value, prepareErr := callPrepareToolInvocation(ctx, execution.ToolInvocations, input)
		if prepareErr != nil {
			return nil, redactedToolError(prepareErr)
		}
		if !validToolInvocationValue(value, input) {
			return nil, errToolInvocationState
		}
		state := newToolInvocationState(execution.ToolInvocations, input, value.Status, value.FencingToken)
		return &trpcagenttool.BeforeToolResult{Context: context.WithValue(ctx, toolInvocationStateKey{}, state)}, nil
	})
}

type toolInvocationDispatchPlugin struct{}

func (toolInvocationDispatchPlugin) Name() string { return "tool_invocation_dispatch" }

func (toolInvocationDispatchPlugin) Register(registry *plugin.Registry) {
	registry.BeforeTool(func(ctx context.Context, args *trpcagenttool.BeforeToolArgs) (*trpcagenttool.BeforeToolResult, error) {
		execution, executionErr := ExecutionContextFromContext(ctx)
		if executionErr != nil || !validToolInvocationExecution(execution) {
			return nil, errToolInvocationState
		}
		state, ok := toolInvocationStateFromContext(ctx)
		if !ok {
			var stateErr error
			state, stateErr = loadToolInvocationState(ctx, args)
			if stateErr != nil {
				return nil, redactedToolError(stateErr)
			}
		}
		if state.tenantID != execution.TenantID || state.appID != execution.AppID {
			return nil, errToolInvocationState
		}
		if !toolInvocationStateMatchesArgs(state, args) {
			return nil, errToolInvocationState
		}
		store := execution.ToolInvocations
		input, inputErr := toolInvocationInputFromState(execution, state)
		if inputErr != nil || !toolInvocationStateMatchesInput(state, input) {
			return nil, errToolInvocationState
		}
		value, getErr := callGetToolInvocation(ctx, store, input.TenantID, input.AppID, input.InvocationID)
		if getErr != nil {
			return nil, redactedToolError(getErr)
		}
		if !validToolInvocationStateRefresh(value, input, state) {
			return nil, errToolInvocationState
		}
		state.status, state.fence, state.store = value.Status, value.FencingToken, store
		if state.status == runtimestorage.ToolInvocationManual {
			return &trpcagenttool.BeforeToolResult{CustomResult: "manual approval required"}, nil
		}
		if state.status != runtimestorage.ToolInvocationPrepared {
			return nil, errToolInvocationState
		}
		value, err := callTransitionToolInvocation(ctx, store, runtimestorage.ToolInvocationTransition{
			TenantID: state.tenantID, AppID: state.appID, InvocationID: state.invocationID,
			From: runtimestorage.ToolInvocationPrepared, To: runtimestorage.ToolInvocationDispatching,
			Owner: ownerFromExecutionContext(ctx), FencingToken: state.fence,
		})
		if err != nil {
			return nil, redactedToolError(err)
		}
		if !validToolInvocationValue(value, input) || value.Status != runtimestorage.ToolInvocationDispatching || value.FencingToken != state.fence+1 {
			return nil, errToolInvocationState
		}
		// accepted is the durable hand-off point. A crash after this transition
		// and before the provider returns is reconciled as unknown rather than
		// being replayed blindly.
		value, err = callTransitionToolInvocation(ctx, store, runtimestorage.ToolInvocationTransition{
			TenantID: state.tenantID, AppID: state.appID, InvocationID: state.invocationID,
			From: runtimestorage.ToolInvocationDispatching, To: runtimestorage.ToolInvocationAccepted,
			Owner: ownerFromExecutionContext(ctx), FencingToken: value.FencingToken,
		})
		if err != nil {
			return nil, redactedToolError(err)
		}
		if !validToolInvocationValue(value, input) || value.Status != runtimestorage.ToolInvocationAccepted || value.FencingToken != state.fence+2 {
			return nil, errToolInvocationState
		}
		state.status, state.fence, state.store = value.Status, value.FencingToken, store
		if auditErr := RecordToolInvocationAudit(ctx, execution.Audit, value); auditErr != nil {
			return nil, redactedToolError(auditErr)
		}
		return &trpcagenttool.BeforeToolResult{Context: context.WithValue(ctx, toolInvocationStateKey{}, state)}, nil
	})
	registry.OnEvent(func(ctx context.Context, _ *trpcagent.Invocation, current *event.Event) (*event.Event, error) {
		return reconcileShortCircuitedToolEvent(ctx, current)
	})
	registry.AfterTool(func(ctx context.Context, args *trpcagenttool.AfterToolArgs) (*trpcagenttool.AfterToolResult, error) {
		execution, executionErr := ExecutionContextFromContext(ctx)
		if executionErr != nil || !validToolInvocationExecution(execution) {
			return nil, errToolInvocationState
		}
		state, ok := toolInvocationStateFromContext(ctx)
		if !ok {
			var stateErr error
			state, stateErr = loadToolInvocationStateAfter(ctx, args)
			if stateErr != nil {
				return nil, redactedToolError(stateErr)
			}
		}
		if state.tenantID != execution.TenantID || state.appID != execution.AppID || !toolInvocationAfterStateMatchesArgs(state, args) || args != nil && (state.toolCallID != args.ToolCallID || state.toolName != args.ToolName) {
			return nil, errToolInvocationState
		}
		store := execution.ToolInvocations
		input, inputErr := toolInvocationInputFromState(execution, state)
		if inputErr != nil || !toolInvocationStateMatchesInput(state, input) {
			return nil, errToolInvocationState
		}
		value, getErr := callGetToolInvocation(ctx, store, input.TenantID, input.AppID, input.InvocationID)
		if getErr != nil {
			return nil, redactedToolError(getErr)
		}
		if !validToolInvocationStateRefresh(value, input, state) {
			return nil, errToolInvocationState
		}
		state.status, state.fence, state.store = value.Status, value.FencingToken, store
		// Approval reviewers may persist Manual after the prepare callback has
		// already returned its prepared state. Re-read before finalizing so a
		// pending approval is not accidentally converted to denied merely
		// because the upstream approval plugin short-circuited the call.
		if state.status == runtimestorage.ToolInvocationManual || state.status == runtimestorage.ToolInvocationPrepared {
			switch {
			case value.Status == runtimestorage.ToolInvocationManual:
				if auditErr := RecordToolInvocationAudit(ctx, execution.Audit, value); auditErr != nil {
					return nil, redactedToolError(auditErr)
				}
				return &trpcagenttool.AfterToolResult{Context: context.WithValue(ctx, toolInvocationStateKey{}, state)}, nil
			case state.status == runtimestorage.ToolInvocationManual:
				// A manual row may only become prepared through an authenticated
				// approval marker. Do not let an arbitrary prepared row execute.
				if value.Status != runtimestorage.ToolInvocationPrepared || !validToolInvocationApprovalMarker(value.ReviewerID) {
					return nil, errToolInvocationState
				}
				return &trpcagenttool.AfterToolResult{Context: context.WithValue(ctx, toolInvocationStateKey{}, state)}, nil
			case value.Status == runtimestorage.ToolInvocationPrepared && validToolInvocationApprovalMarker(value.ReviewerID):
				// An approval won a race with the short-circuit. Leave the
				// invocation prepared for the next explicit retry.
				return &trpcagenttool.AfterToolResult{Context: context.WithValue(ctx, toolInvocationStateKey{}, state)}, nil
			}
		}
		if args == nil {
			return nil, errToolInvocationState
		}
		to, errorClass := toolInvocationResult(args)
		if state.status == runtimestorage.ToolInvocationPrepared && (args == nil || args.Error == nil) {
			// A previous BeforeTool guardrail short-circuited the actual call.
			to, errorClass = runtimestorage.ToolInvocationDenied, "denied"
		}
		previousFence := state.fence
		value, err := callTransitionToolInvocation(ctx, store, runtimestorage.ToolInvocationTransition{
			TenantID: state.tenantID, AppID: state.appID, InvocationID: state.invocationID,
			From: state.status, To: to, Owner: ownerFromExecutionContext(ctx),
			FencingToken: state.fence, ErrorClass: errorClass,
		})
		if err != nil {
			return nil, redactedToolError(err)
		}
		if !validToolInvocationValue(value, input) || value.Status != to || value.FencingToken != previousFence+1 {
			return nil, errToolInvocationState
		}
		// Audit is a value type with an intentionally private writer; its
		// zero value is a no-op and configured recorders deduplicate retries.
		if auditErr := RecordToolInvocationAudit(ctx, execution.Audit, value); auditErr != nil {
			return nil, redactedToolError(auditErr)
		}
		state.status, state.fence, state.store = value.Status, value.FencingToken, store
		return &trpcagenttool.AfterToolResult{Context: context.WithValue(ctx, toolInvocationStateKey{}, state)}, nil
	})
}

func reconcileShortCircuitedToolEvent(ctx context.Context, current *event.Event) (*event.Event, error) {
	if current == nil || current.Response == nil || current.Response.Object != model.ObjectTypeToolResponse || current.Response.IsPartial {
		return current, nil
	}
	execution, err := ExecutionContextFromContext(ctx)
	if err != nil || !validToolInvocationExecution(execution) {
		return current, errToolInvocationState
	}
	arguments, present, err := event.GetExtension[map[string]string](current, event.ToolCallArgsExtensionKey)
	if err != nil || !present {
		return current, nil
	}
	for _, choice := range current.Response.Choices {
		message := choice.Message
		if message.Role != model.RoleTool || strings.TrimSpace(message.ToolID) == "" || strings.TrimSpace(message.ToolName) == "" {
			continue
		}
		raw, ok := arguments[message.ToolID]
		if !ok {
			continue
		}
		input, inputErr := newToolInvocationInputValues(execution, message.ToolID, message.ToolName, []byte(raw))
		if inputErr != nil {
			continue
		}
		value, getErr := callGetToolInvocation(ctx, execution.ToolInvocations, input.TenantID, input.AppID, input.InvocationID)
		if getErr != nil {
			if errors.Is(getErr, runtimestorage.ErrNotFound) {
				continue
			}
			return current, redactedToolError(getErr)
		}
		if !validToolInvocationValue(value, input) {
			return current, errToolInvocationState
		}
		to, errorClass := runtimestorage.ToolInvocationStatus(""), ""
		switch value.Status {
		case runtimestorage.ToolInvocationPrepared:
			to, errorClass = runtimestorage.ToolInvocationDenied, "denied"
		case runtimestorage.ToolInvocationDispatching, runtimestorage.ToolInvocationAccepted:
			// A result event without the AfterTool finalizer means that the
			// call was short-circuited after admission, or that the result
			// crossed a crash boundary. Never replay it automatically.
			to, errorClass = runtimestorage.ToolInvocationUnknown, "provider_uncertain"
		default:
			continue
		}
		updated, transitionErr := callTransitionToolInvocation(ctx, execution.ToolInvocations, runtimestorage.ToolInvocationTransition{
			TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID,
			From: value.Status, To: to, Owner: execution.RequestID,
			FencingToken: value.FencingToken, ErrorClass: errorClass,
		})
		if transitionErr != nil {
			if errors.Is(transitionErr, runtimestorage.ErrConflict) {
				continue
			}
			return current, redactedToolError(transitionErr)
		}
		if !validToolInvocationValue(updated, input) || updated.Status != to || updated.FencingToken != value.FencingToken+1 {
			return current, errToolInvocationState
		}
		if auditErr := RecordToolInvocationAudit(ctx, execution.Audit, updated); auditErr != nil {
			return current, redactedToolError(auditErr)
		}
	}
	return current, nil
}

func newToolInvocationInput(execution ExecutionContext, args *trpcagenttool.BeforeToolArgs) (runtimestorage.ToolInvocationInput, error) {
	if args == nil {
		return runtimestorage.ToolInvocationInput{}, errToolInvocationState
	}
	return newToolInvocationInputValues(execution, args.ToolCallID, args.ToolName, args.Arguments)
}

func newToolInvocationInputValues(execution ExecutionContext, toolCallID, toolName string, arguments []byte) (runtimestorage.ToolInvocationInput, error) {
	if strings.TrimSpace(toolName) == "" || strings.TrimSpace(toolCallID) == "" || !validMCPArguments(arguments) {
		return runtimestorage.ToolInvocationInput{}, errToolInvocationState
	}
	canonical := canonicalToolArguments(arguments)
	digest := sha256.Sum256(canonical)
	argsSHA := hex.EncodeToString(digest[:])
	input := runtimestorage.ToolInvocationInput{
		TenantID: execution.TenantID, AppID: execution.AppID, InvocationID: runtimestorage.DeriveToolInvocationIDForApp(execution.TenantID, execution.AppID, execution.EventID, toolCallID, toolName, argsSHA),
		EventID: execution.EventID, RequestID: execution.RequestID, TraceID: execution.TraceID,
		ToolCallID: toolCallID, ToolName: toolName, ArgsSHA256: argsSHA, Owner: execution.RequestID,
	}
	if err := runtimestorage.ValidateToolInvocationInput(input); err != nil {
		return runtimestorage.ToolInvocationInput{}, errToolInvocationState
	}
	return input, nil
}

func loadToolInvocationState(ctx context.Context, args *trpcagenttool.BeforeToolArgs) (toolInvocationState, error) {
	execution, err := ExecutionContextFromContext(ctx)
	if err != nil || !validToolInvocationExecution(execution) {
		return toolInvocationState{}, errToolInvocationState
	}
	input, err := newToolInvocationInput(execution, args)
	if err != nil {
		return toolInvocationState{}, err
	}
	value, err := callGetToolInvocation(ctx, execution.ToolInvocations, input.TenantID, input.AppID, input.InvocationID)
	if err != nil {
		return toolInvocationState{}, err
	}
	if !validToolInvocationValue(value, input) {
		return toolInvocationState{}, errToolInvocationState
	}
	return newToolInvocationState(execution.ToolInvocations, input, value.Status, value.FencingToken), nil
}

func loadToolInvocationStateAfter(ctx context.Context, args *trpcagenttool.AfterToolArgs) (toolInvocationState, error) {
	if args == nil {
		return toolInvocationState{}, errToolInvocationState
	}
	execution, err := ExecutionContextFromContext(ctx)
	if err != nil || !validToolInvocationExecution(execution) {
		return toolInvocationState{}, errToolInvocationState
	}
	input, err := newToolInvocationInputValues(execution, args.ToolCallID, args.ToolName, args.Arguments)
	if err != nil {
		return toolInvocationState{}, err
	}
	value, err := callGetToolInvocation(ctx, execution.ToolInvocations, input.TenantID, input.AppID, input.InvocationID)
	if err != nil {
		return toolInvocationState{}, err
	}
	if !validToolInvocationValue(value, input) {
		return toolInvocationState{}, errToolInvocationState
	}
	return newToolInvocationState(execution.ToolInvocations, input, value.Status, value.FencingToken), nil
}

func newToolInvocationState(store runtimestorage.ToolInvocationStore, input runtimestorage.ToolInvocationInput, status runtimestorage.ToolInvocationStatus, fence int64) toolInvocationState {
	return toolInvocationState{store: store, tenantID: input.TenantID, appID: input.AppID, invocationID: input.InvocationID, eventID: input.EventID, requestID: input.RequestID, traceID: input.TraceID, toolCallID: input.ToolCallID, toolName: input.ToolName, status: status, fence: fence, argsSHA256: input.ArgsSHA256}
}

func toolInvocationInputFromState(execution ExecutionContext, state toolInvocationState) (runtimestorage.ToolInvocationInput, error) {
	input := runtimestorage.ToolInvocationInput{TenantID: state.tenantID, AppID: state.appID, InvocationID: state.invocationID, EventID: state.eventID, RequestID: state.requestID, TraceID: state.traceID, ToolCallID: state.toolCallID, ToolName: state.toolName, ArgsSHA256: state.argsSHA256, Owner: execution.RequestID}
	if err := runtimestorage.ValidateToolInvocationInput(input); err != nil || input.TenantID != execution.TenantID || input.AppID != execution.AppID || input.EventID != execution.EventID || input.RequestID != execution.RequestID || input.TraceID != execution.TraceID || input.Owner != execution.RequestID {
		return runtimestorage.ToolInvocationInput{}, errToolInvocationState
	}
	return input, nil
}

func toolInvocationStateMatchesInput(state toolInvocationState, input runtimestorage.ToolInvocationInput) bool {
	return state.tenantID == input.TenantID && state.appID == input.AppID && state.invocationID == input.InvocationID && state.eventID == input.EventID && state.requestID == input.RequestID && state.traceID == input.TraceID && state.toolCallID == input.ToolCallID && state.toolName == input.ToolName && state.argsSHA256 == input.ArgsSHA256
}

func validToolInvocationExecution(execution ExecutionContext) bool {
	if isNilMCPValue(execution.ToolInvocations) || !execution.Audit.Configured() || runtimestorage.ValidateToolInvocationTenantApp(execution.TenantID, execution.AppID) != nil || !ValidExecutionContextIdentity(execution) {
		return false
	}
	for _, value := range []string{execution.TenantID, execution.AppID, execution.UserID, execution.SessionID, execution.EventID, execution.RequestID} {
		if !validMCPExecutionID(value, true) {
			return false
		}
	}
	return validMCPExecutionID(execution.TraceID, false)
}

func validToolInvocationValue(value runtimestorage.ToolInvocation, input runtimestorage.ToolInvocationInput) bool {
	if runtimestorage.ValidateToolInvocationInput(input) != nil || runtimestorage.ValidateToolInvocation(value) != nil || value.TenantID != input.TenantID || value.AppID != input.AppID || value.InvocationID != input.InvocationID || value.EventID != input.EventID || value.RequestID != input.RequestID || value.TraceID != input.TraceID || value.ToolCallID != input.ToolCallID || value.ToolName != input.ToolName || value.ArgsSHA256 != input.ArgsSHA256 || value.Owner != input.Owner {
		return false
	}
	return validMCPExecutionID(value.ErrorClass, false) && validMCPExecutionID(value.ReviewerID, false)
}

func validToolInvocationApprovalMarker(value string) bool {
	return strings.HasPrefix(value, "approved:") && validMCPExecutionID(strings.TrimPrefix(value, "approved:"), true)
}

func validToolInvocationStateRefresh(value runtimestorage.ToolInvocation, input runtimestorage.ToolInvocationInput, state toolInvocationState) bool {
	if !validToolInvocationValue(value, input) {
		return false
	}
	if value.Status == state.status && value.FencingToken == state.fence {
		return true
	}
	if state.status == runtimestorage.ToolInvocationPrepared && value.Status == runtimestorage.ToolInvocationManual {
		return true
	}
	if state.status == runtimestorage.ToolInvocationManual && value.Status == runtimestorage.ToolInvocationPrepared && validToolInvocationApprovalMarker(value.ReviewerID) {
		return true
	}
	return state.status == runtimestorage.ToolInvocationPrepared && value.Status == runtimestorage.ToolInvocationPrepared && validToolInvocationApprovalMarker(value.ReviewerID)
}

func toolInvocationStateMatchesArgs(state toolInvocationState, args *trpcagenttool.BeforeToolArgs) bool {
	if args == nil || !validMCPArguments(args.Arguments) || state.argsSHA256 == "" || strings.TrimSpace(args.ToolCallID) == "" || strings.TrimSpace(args.ToolName) == "" || args.ToolCallID != state.toolCallID || args.ToolName != state.toolName {
		return false
	}
	digest := sha256.Sum256(canonicalToolArguments(args.Arguments))
	return hex.EncodeToString(digest[:]) == state.argsSHA256
}

func toolInvocationAfterStateMatchesArgs(state toolInvocationState, args *trpcagenttool.AfterToolArgs) bool {
	if args == nil {
		return true
	}
	if !validMCPArguments(args.Arguments) || state.argsSHA256 == "" || strings.TrimSpace(args.ToolCallID) == "" || strings.TrimSpace(args.ToolName) == "" || args.ToolCallID != state.toolCallID || args.ToolName != state.toolName {
		return false
	}
	digest := sha256.Sum256(canonicalToolArguments(args.Arguments))
	return hex.EncodeToString(digest[:]) == state.argsSHA256
}

func canonicalToolArguments(raw []byte) []byte {
	var value any
	if len(raw) > 0 && jsonstrict.Validate(raw, true) == nil && json.Unmarshal(raw, &value) == nil {
		if encoded, err := json.Marshal(value); err == nil {
			return encoded
		}
	}
	return append([]byte(nil), raw...)
}

func toolInvocationStateFromContext(ctx context.Context) (toolInvocationState, bool) {
	if isNilMCPValue(ctx) {
		return toolInvocationState{}, false
	}
	raw, valueErr := nilvalue.ContextValue(ctx, toolInvocationStateKey{})
	if valueErr != nil {
		return toolInvocationState{}, false
	}
	state, ok := raw.(toolInvocationState)
	return state, ok && !isNilMCPValue(state.store) && validMCPExecutionID(state.tenantID, true) && validMCPExecutionID(state.appID, true) && validMCPExecutionID(state.invocationID, true) && validMCPExecutionID(state.eventID, true) && validMCPExecutionID(state.requestID, true) && validMCPExecutionID(state.traceID, false) && validMCPExecutionID(state.toolCallID, true) && validMCPExecutionID(state.toolName, true) && state.fence > 0 && len(state.argsSHA256) == sha256.Size*2 && strings.ToLower(state.argsSHA256) == state.argsSHA256
}

func ownerFromExecutionContext(ctx context.Context) string {
	execution, err := ExecutionContextFromContext(ctx)
	if err != nil {
		return ""
	}
	return execution.RequestID
}

// DurableApprovalReviewer is a fail-closed reviewer that records a pending
// approval as manual state in the invocation ledger. Operators approve a
// pending row through the Admin transition endpoint, which moves it back to
// prepared with an authenticated reviewer marker; the next retry is then admitted.
type DurableApprovalReviewer struct {
	store      runtimestorage.ToolInvocationStore
	reviewerID string
}

// NewDurableApprovalReviewer returns a reviewer backed by the supplied
// invocation ledger. It never persists the request arguments or transcript.
func NewDurableApprovalReviewer(store runtimestorage.ToolInvocationStore) approvalreview.Reviewer {
	if isNilMCPValue(store) {
		store = nil
	}
	return &DurableApprovalReviewer{store: store, reviewerID: "durable-manual"}
}

func (reviewer *DurableApprovalReviewer) Review(ctx context.Context, request *approvalreview.Request) (*approvalreview.Decision, error) {
	if reviewer == nil || isNilMCPValue(reviewer.store) || request == nil || strings.TrimSpace(request.Action.ToolName) == "" {
		return nil, errToolInvocationState
	}
	execution, err := ExecutionContextFromContext(ctx)
	if err != nil || !validToolInvocationExecution(execution) {
		return nil, errToolInvocationState
	}
	// The Runner seals the execution store per App. The constructor argument
	// only enables the reviewer in process configuration; never use it to
	// bypass the current execution boundary.
	store := execution.ToolInvocations
	toolCallID, ok := trpcagenttool.ToolCallIDFromContext(ctx)
	if !ok || strings.TrimSpace(toolCallID) == "" {
		return nil, errToolInvocationState
	}
	input, err := newToolInvocationInputValues(execution, toolCallID, request.Action.ToolName, request.Action.Arguments)
	if err != nil {
		return nil, err
	}
	value, err := callGetToolInvocation(ctx, store, input.TenantID, input.AppID, input.InvocationID)
	if err != nil {
		return nil, redactedToolError(err)
	}
	if !validToolInvocationValue(value, input) {
		return nil, errToolInvocationState
	}
	if validToolInvocationApprovalMarker(value.ReviewerID) && value.Status == runtimestorage.ToolInvocationPrepared {
		return &approvalreview.Decision{Approved: true, RiskLevel: "approved", Reason: "durable approval recorded"}, nil
	}
	if value.Status == runtimestorage.ToolInvocationPrepared {
		updated, transitionErr := callTransitionToolInvocation(ctx, store, runtimestorage.ToolInvocationTransition{
			TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: runtimestorage.ToolInvocationPrepared,
			To: runtimestorage.ToolInvocationManual, Owner: execution.RequestID, FencingToken: value.FencingToken,
			ErrorClass: "approval_required", ReviewerID: reviewer.reviewerID,
		})
		if transitionErr == nil && (!validToolInvocationValue(updated, input) || updated.Status != runtimestorage.ToolInvocationManual || updated.FencingToken != value.FencingToken+1) {
			return nil, errToolInvocationState
		}
		if transitionErr != nil && !errors.Is(transitionErr, runtimestorage.ErrConflict) {
			return nil, redactedToolError(transitionErr)
		}
		if transitionErr != nil {
			// Another concurrent reviewer may have won the fence. Re-read
			// before failing the request so a duplicate approval prompt is
			// represented by the same durable manual state.
			value, err = callGetToolInvocation(ctx, store, input.TenantID, input.AppID, input.InvocationID)
			if err != nil {
				return nil, redactedToolError(err)
			}
			if !validToolInvocationValue(value, input) {
				return nil, errToolInvocationState
			}
			if validToolInvocationApprovalMarker(value.ReviewerID) && value.Status == runtimestorage.ToolInvocationPrepared {
				return &approvalreview.Decision{Approved: true, RiskLevel: "approved", Reason: "durable approval recorded"}, nil
			}
		}
	}
	return &approvalreview.Decision{Approved: false, RiskLevel: "manual", Reason: "manual approval required"}, nil
}

func toolInvocationResult(args *trpcagenttool.AfterToolArgs) (runtimestorage.ToolInvocationStatus, string) {
	if args == nil || args.Error == nil {
		return runtimestorage.ToolInvocationSucceeded, ""
	}
	if errors.Is(args.Error, ErrApprovalRequired) || errors.Is(args.Error, ErrApprovalDenied) || errors.Is(args.Error, ErrDenied) {
		return runtimestorage.ToolInvocationDenied, "denied"
	}
	if strings.HasPrefix(args.ToolName, "mcp_") || errors.Is(args.Error, context.Canceled) || errors.Is(args.Error, context.DeadlineExceeded) {
		return runtimestorage.ToolInvocationUnknown, "provider_uncertain"
	}
	return runtimestorage.ToolInvocationFailed, "tool"
}
