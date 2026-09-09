package inmemory

import (
	"context"
	"testing"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestRecoverStaleToolInvocationsFencesProviderBoundary(t *testing.T) {
	store := NewToolInvocationStore()
	ctx := context.Background()
	input := runtimestorage.ToolInvocationInput{
		TenantID: "tenant-a", AppID: "app_01J1K9ZQTVE4PAWF1TSB2WMHNP", EventID: "event-a", RequestID: "request-a", TraceID: "trace-a",
		ToolCallID: "call-a", ToolName: "mcp_demo__write", ArgsSHA256: "", Owner: "request-a",
	}
	input.ArgsSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	input.InvocationID = runtimestorage.DeriveToolInvocationIDForApp(input.TenantID, input.AppID, input.EventID, input.ToolCallID, input.ToolName, input.ArgsSHA256)
	prepared, err := store.PrepareToolInvocation(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := store.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: prepared.Status,
		To: runtimestorage.ToolInvocationDispatching, Owner: input.Owner, FencingToken: prepared.FencingToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	accepted, err = store.TransitionToolInvocation(ctx, runtimestorage.ToolInvocationTransition{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, From: accepted.Status,
		To: runtimestorage.ToolInvocationAccepted, Owner: input.Owner, FencingToken: accepted.FencingToken,
	})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.Status != runtimestorage.ToolInvocationAccepted {
		t.Fatalf("accepted state = %+v", accepted)
	}
	values, err := store.RecoverStaleToolInvocations(ctx, time.Now().UTC().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 1 || values[0].Status != runtimestorage.ToolInvocationUnknown || values[0].FencingToken != accepted.FencingToken+1 {
		t.Fatalf("recovered values = %+v", values)
	}
	if values, err := store.RecoverStaleToolInvocations(ctx, time.Now().UTC().Add(time.Second)); err != nil || len(values) != 0 {
		t.Fatalf("recovery was not idempotent: values=%+v err=%v", values, err)
	}
}
