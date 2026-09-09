package tool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	approvalreview "trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestRecordToolInvocationAuditIsStableAcrossRetries(t *testing.T) {
	backend := &audit.Backend{}
	writer, err := audit.NewInMemoryWithBackend("tenant-tool", backend)
	if err != nil {
		t.Fatal(err)
	}
	value := runtimestorage.ToolInvocation{
		TenantID: "tenant-tool", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", EventID: "event-audit", RequestID: "request-audit", TraceID: "trace-audit",
		ToolCallID: "call-audit", ToolName: "mcp_demo__write", ArgsSHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Owner: "request-audit", Status: runtimestorage.ToolInvocationUnknown,
		CreatedAt: time.Unix(10, 0).UTC(), UpdatedAt: time.Unix(11, 0).UTC(), FencingToken: 1,
	}
	value.InvocationID = runtimestorage.DeriveToolInvocationIDForApp(value.TenantID, value.AppID, value.EventID, value.ToolCallID, value.ToolName, value.ArgsSHA256)
	recorder := audit.NewRecorder(writer, value.TenantID)
	if err := RecordToolInvocationAudit(context.Background(), recorder, value); err != nil {
		t.Fatal(err)
	}
	if err := RecordToolInvocationAudit(context.Background(), recorder, value); err != nil {
		t.Fatalf("retry audit = %v", err)
	}
	events, err := writer.List(context.Background(), audit.Query{EventTypes: []audit.EventType{audit.EventToolReconciliationRequired}})
	if err != nil || len(events) != 1 || events[0].OccurredAt != value.CreatedAt {
		t.Fatalf("stable audit events = %#v, err=%v", events, err)
	}
}

func TestToolInvocationPluginsPersistSuccessAndShortCircuitDenial(t *testing.T) {
	store := runtimestorageinmemory.NewToolInvocationStore()
	ctx := toolInvocationTestContext(store)
	manager, err := plugin.NewManager(NewToolInvocationPreparePlugin(), NewToolInvocationDispatchPlugin())
	if err != nil {
		t.Fatal(err)
	}
	args := &agenttool.BeforeToolArgs{ToolCallID: "call-1", ToolName: "mcp_demo__read", Arguments: []byte(`{"path":"/tmp/a"}`)}
	before, err := manager.ToolCallbacks().RunBeforeTool(ctx, args)
	if err != nil || before == nil || before.Context == nil {
		t.Fatalf("before = %#v, err = %v", before, err)
	}
	if _, err := manager.ToolCallbacks().RunAfterTool(before.Context, &agenttool.AfterToolArgs{ToolCallID: args.ToolCallID, ToolName: args.ToolName, Arguments: args.Arguments, Result: "ok"}); err != nil {
		t.Fatal(err)
	}
	input, err := newToolInvocationInputValues(mustExecutionContext(ctx), args.ToolCallID, args.ToolName, args.Arguments)
	if err != nil {
		t.Fatal(err)
	}
	value, err := store.GetToolInvocation(ctx, input.TenantID, input.AppID, input.InvocationID)
	if err != nil || value.Status != runtimestorage.ToolInvocationSucceeded {
		t.Fatalf("successful invocation = %+v, err = %v", value, err)
	}

	deniedStore := runtimestorageinmemory.NewToolInvocationStore()
	deniedContext := toolInvocationTestContext(deniedStore)
	denyingPlugin := denialTestPlugin{}
	manager, err = plugin.NewManager(NewToolInvocationPreparePlugin(), denyingPlugin, NewToolInvocationDispatchPlugin())
	if err != nil {
		t.Fatal(err)
	}
	deniedArgs := &agenttool.BeforeToolArgs{ToolCallID: "call-2", ToolName: "mcp_demo__write", Arguments: []byte(`{"value":1}`)}
	before, err = manager.ToolCallbacks().RunBeforeTool(deniedContext, deniedArgs)
	if err != nil || before == nil || before.CustomResult == nil {
		t.Fatalf("denied before = %#v, err = %v", before, err)
	}
	if _, err := manager.ToolCallbacks().RunAfterTool(deniedContext, &agenttool.AfterToolArgs{ToolCallID: deniedArgs.ToolCallID, ToolName: deniedArgs.ToolName, Arguments: deniedArgs.Arguments, Result: before.CustomResult}); err != nil {
		t.Fatal(err)
	}
	input, err = newToolInvocationInputValues(mustExecutionContext(deniedContext), deniedArgs.ToolCallID, deniedArgs.ToolName, deniedArgs.Arguments)
	if err != nil {
		t.Fatal(err)
	}
	value, err = deniedStore.GetToolInvocation(deniedContext, input.TenantID, input.AppID, input.InvocationID)
	if err != nil || value.Status != runtimestorage.ToolInvocationDenied {
		t.Fatalf("denied invocation = %+v, err = %v", value, err)
	}
}

func TestToolInvocationEventFinalizesShortCircuitWithoutAfterHook(t *testing.T) {
	store := runtimestorageinmemory.NewToolInvocationStore()
	ctx := toolInvocationTestContext(store)
	manager, err := plugin.NewManager(NewToolInvocationPreparePlugin(), denialTestPlugin{}, NewToolInvocationDispatchPlugin())
	if err != nil {
		t.Fatal(err)
	}
	args := &agenttool.BeforeToolArgs{ToolCallID: "call-event", ToolName: "mcp_demo__write", Arguments: []byte(`{"value":1}`)}
	before, err := manager.ToolCallbacks().RunBeforeTool(ctx, args)
	if err != nil || before == nil || before.CustomResult == nil {
		t.Fatalf("short-circuit = %#v, err=%v", before, err)
	}
	input, err := newToolInvocationInputValues(mustExecutionContext(ctx), args.ToolCallID, args.ToolName, args.Arguments)
	if err != nil {
		t.Fatal(err)
	}
	responseEvent := event.New("invocation", "agent", event.WithResponse(&model.Response{
		Object:  model.ObjectTypeToolResponse,
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleTool, ToolID: args.ToolCallID, ToolName: args.ToolName}}},
	}), event.WithExtension(event.ToolCallArgsExtensionKey, map[string]string{args.ToolCallID: string(args.Arguments)}))
	if _, err := manager.OnEvent(ctx, nil, responseEvent); err != nil {
		t.Fatal(err)
	}
	value, err := store.GetToolInvocation(ctx, input.TenantID, input.AppID, input.InvocationID)
	if err != nil || value.Status != runtimestorage.ToolInvocationDenied {
		t.Fatalf("event-finalized invocation = %+v, err=%v", value, err)
	}
}

func TestDurableApprovalReviewerPersistsManualAndConsumesApproval(t *testing.T) {
	store := runtimestorageinmemory.NewToolInvocationStore()
	ctx := toolInvocationTestContext(store)
	ctx = context.WithValue(ctx, agenttool.ContextKeyToolCallID{}, "call-approval")
	execution := mustExecutionContext(ctx)
	request := &approvalreview.Request{Action: approvalreview.Action{ToolName: "mcp_demo__write", Arguments: []byte(`{"value":1}`)}}
	input, err := newToolInvocationInputValues(execution, "call-approval", request.Action.ToolName, request.Action.Arguments)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := store.PrepareToolInvocation(ctx, input)
	if err != nil || prepared.Status != runtimestorage.ToolInvocationPrepared {
		t.Fatalf("prepare = %+v, err = %v", prepared, err)
	}
	// The configured reviewer store must not override the execution's sealed
	// app-scoped ledger.
	reviewer := NewDurableApprovalReviewer(runtimestorageinmemory.NewToolInvocationStore())
	decision, err := reviewer.Review(ctx, request)
	if err != nil || decision == nil || decision.Approved {
		t.Fatalf("pending decision = %+v, err = %v", decision, err)
	}
	manual, err := store.GetToolInvocation(ctx, input.TenantID, input.AppID, input.InvocationID)
	if err != nil || manual.Status != runtimestorage.ToolInvocationManual {
		t.Fatalf("manual invocation = %+v, err = %v", manual, err)
	}
	approved, err := store.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: runtimestorage.ToolInvocationManual,
		To: runtimestorage.ToolInvocationPrepared, FencingToken: manual.FencingToken,
		Owner: "operator", ReviewerID: "approved:operator",
	})
	if err != nil || approved.Status != runtimestorage.ToolInvocationPrepared {
		t.Fatalf("approved retry = %+v, err = %v", approved, err)
	}
	decision, err = reviewer.Review(ctx, request)
	if err != nil || decision == nil || !decision.Approved {
		t.Fatalf("approved decision = %+v, err = %v", decision, err)
	}
}

func TestToolInvocationPluginsFailClosedWithoutSealedExecutionContext(t *testing.T) {
	manager, err := plugin.NewManager(NewToolInvocationPreparePlugin(), NewToolInvocationDispatchPlugin())
	if err != nil {
		t.Fatal(err)
	}
	_, err = manager.ToolCallbacks().RunBeforeTool(context.Background(), &agenttool.BeforeToolArgs{
		ToolCallID: "call", ToolName: "mcp_demo__write", Arguments: []byte(`{"value":1}`),
	})
	if !errors.Is(err, errToolInvocationState) {
		t.Fatalf("missing execution context error = %v", err)
	}
}

func toolInvocationTestContext(store runtimestorage.ToolInvocationStore) context.Context {
	return WithExecutionContext(context.Background(), ExecutionContext{
		TenantID: "tenant-tool", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", UserID: "user-tool", SessionID: "session-tool",
		EventID: "event-tool", RequestID: "request-tool", TraceID: "trace-tool", ToolInvocations: store,
		Audit: audit.NewRecorder(&mcpGovernanceWriter{}, "tenant-tool"),
	})
}

func mustExecutionContext(ctx context.Context) ExecutionContext {
	execution, err := ExecutionContextFromContext(ctx)
	if err != nil {
		panic(err)
	}
	return execution
}

type denialTestPlugin struct{}

func (denialTestPlugin) Name() string { return "test_denial" }

func (denialTestPlugin) Register(registry *plugin.Registry) {
	registry.BeforeTool(func(context.Context, *agenttool.BeforeToolArgs) (*agenttool.BeforeToolResult, error) {
		return &agenttool.BeforeToolResult{CustomResult: "denied"}, nil
	})
}

var _ approvalreview.Reviewer = (*DurableApprovalReviewer)(nil)
