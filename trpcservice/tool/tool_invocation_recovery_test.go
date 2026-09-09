package tool

import (
	"errors"
	"testing"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestToolInvocationRecoveryIsRestartSafeAndRequiresReconciliation(t *testing.T) {
	backend := &runtimestorageinmemory.ToolInvocationBackend{}
	firstProcess := runtimestorageinmemory.NewToolInvocationStoreWithBackend(backend)
	secondProcess := runtimestorageinmemory.NewToolInvocationStoreWithBackend(backend)
	ctx := toolInvocationTestContext(firstProcess)
	input, err := newToolInvocationInputValues(mustExecutionContext(ctx), "restart-call", "mcp_demo__write", []byte(`{"value":1}`))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := firstProcess.PrepareToolInvocation(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	dispatching, err := firstProcess.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: runtimestorage.ToolInvocationPrepared,
		To: runtimestorage.ToolInvocationDispatching, Owner: input.Owner, FencingToken: prepared.FencingToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := firstProcess.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: runtimestorage.ToolInvocationDispatching,
		To: runtimestorage.ToolInvocationAccepted, Owner: input.Owner, FencingToken: dispatching.FencingToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	stale, err := secondProcess.RecoverStaleToolInvocations(ctx, time.Now().UTC().Add(time.Second))
	if err != nil || len(stale) != 1 || stale[0].Status != runtimestorage.ToolInvocationUnknown || stale[0].FencingToken != accepted.FencingToken+1 || stale[0].ErrorClass != "provider_uncertain" {
		t.Fatalf("recovery = %#v err=%v", stale, err)
	}
	if staleAgain, err := firstProcess.RecoverStaleToolInvocations(ctx, time.Now().UTC().Add(time.Second)); err != nil || len(staleAgain) != 0 {
		t.Fatalf("recovery is not idempotent: values=%#v err=%v", staleAgain, err)
	}
	if _, err := firstProcess.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: runtimestorage.ToolInvocationAccepted,
		To: runtimestorage.ToolInvocationSucceeded, Owner: input.Owner, FencingToken: accepted.FencingToken,
	}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale provider completion error = %v", err)
	}
	value, err := secondProcess.GetToolInvocation(ctx, input.TenantID, input.AppID, input.InvocationID)
	if err != nil || value.Status != runtimestorage.ToolInvocationUnknown {
		t.Fatalf("post-restart invocation = %#v err=%v", value, err)
	}
}

func TestToolInvocationRecoveryRequiresExecutionContext(t *testing.T) {
	store := runtimestorageinmemory.NewToolInvocationStore()
	firstContext := toolInvocationTestContext(store)
	firstInput, err := newToolInvocationInputValues(mustExecutionContext(firstContext), "tenant-call", "mcp_demo__write", []byte(`{"tenant":1}`))
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.PrepareToolInvocation(firstContext, firstInput)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionToolInvocation(firstContext, runtimestorage.ToolInvocationTransition{TenantID: firstInput.TenantID, AppID: firstInput.AppID, InvocationID: firstInput.InvocationID, From: first.Status, To: runtimestorage.ToolInvocationDispatching, Owner: firstInput.Owner, FencingToken: first.FencingToken}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecoverStaleToolInvocations(nil, time.Now().UTC().Add(time.Second)); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil execution context error = %v", err)
	}
}
