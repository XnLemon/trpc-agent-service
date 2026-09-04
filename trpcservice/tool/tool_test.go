package tool

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/resilience"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type writer struct{ events []audit.Event }

func (w *writer) Append(_ context.Context, e audit.Event) (audit.AppendResult, error) {
	w.events = append(w.events, e)
	return audit.AppendResult{Event: e}, nil
}

func TestPolicyDecisionAuditsWithoutPayload(t *testing.T) {
	w := &writer{}
	p := Policy{Recorder: audit.Recorder{Writer: w, TenantID: "tenant"}, Allowed: map[string]Decision{"search": Allow, "admin": ApprovalRequired}}
	if _, err := p.Decide(context.Background(), "req", "trace", "search"); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Decide(context.Background(), "req2", "trace2", "admin"); err != ErrApprovalRequired {
		t.Fatalf("approval err = %v", err)
	}
	if len(w.events) != 2 || w.events[0].EventType != audit.EventToolAllowed || w.events[1].EventType != audit.EventToolApprovalRequired {
		t.Fatalf("events = %+v", w.events)
	}
}

func TestPolicyDeniesByDefault(t *testing.T) {
	p := Policy{}
	if _, err := p.Decide(context.Background(), "req", "trace", "unknown"); err != ErrDenied {
		t.Fatalf("err = %v", err)
	}
}

func TestPolicyRejectsInvalidNamesAndAuditFailures(t *testing.T) {
	p := Policy{}
	for _, name := range []string{"", strings.Repeat("x", 257)} {
		if _, err := p.Decide(context.Background(), "req", "trace", name); !errors.Is(err, audit.ErrInvalid) {
			t.Fatalf("name %q err=%v", name, err)
		}
	}
	w := &failingWriter{}
	p = Policy{Recorder: audit.Recorder{Writer: w, TenantID: "t"}, Allowed: map[string]Decision{"search": Allow}}
	if _, err := p.Decide(context.Background(), "req", "trace", "search"); !errors.Is(err, audit.ErrWriteFailed) {
		t.Fatalf("err=%v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Append(context.Context, audit.Event) (audit.AppendResult, error) {
	return audit.AppendResult{}, errors.New("down")
}

func TestResilientToolRetriesCallableOperation(t *testing.T) {
	delegate := &callableTool{remainingFailures: 1}
	policy, err := resilience.New(resilience.Config{Timeout: time.Second, MaxAttempts: 2, FailureThreshold: 2, OpenTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	tool, err := NewResilientTool(delegate, policy)
	if err != nil {
		t.Fatal(err)
	}
	result, err := tool.Call(context.Background(), []byte(`{}`))
	if err != nil || result != "ok" || delegate.calls != 2 {
		t.Fatalf("Call() result=%v err=%v calls=%d", result, err, delegate.calls)
	}
}

func TestResolveWithPolicyRequiresExplicitRetrySafeToolID(t *testing.T) {
	delegate := &callableTool{remainingFailures: 1}
	registry, err := NewRegistry(callableFactory{delegate: delegate})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := resilience.New(resilience.Config{Timeout: time.Second, MaxAttempts: 2, FailureThreshold: 2, OpenTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	authorizations := []agent.ToolAuthorization{{ToolID: "callable", Required: true}}
	tools, err := registry.ResolveWithPolicy(authorizations, policy)
	if err != nil || len(tools) != 1 {
		t.Fatalf("ResolveWithPolicy() = %v, %v", tools, err)
	}
	if _, err := tools[0].(trpctool.CallableTool).Call(context.Background(), []byte(`{}`)); err == nil {
		t.Fatal("unlisted callable tool unexpectedly retried to success")
	}
	if delegate.calls != 1 {
		t.Fatalf("unlisted tool calls = %d, want one", delegate.calls)
	}

	delegate = &callableTool{remainingFailures: 1}
	registry, err = NewRegistry(callableFactory{delegate: delegate})
	if err != nil {
		t.Fatal(err)
	}
	tools, err = registry.ResolveWithPolicy(authorizations, policy, " callable ")
	if err != nil || len(tools) != 1 {
		t.Fatalf("ResolveWithPolicy() opt-in = %v, %v", tools, err)
	}
	if _, err := tools[0].(trpctool.CallableTool).Call(context.Background(), []byte(`{}`)); err != nil || delegate.calls != 2 {
		t.Fatalf("opt-in call err=%v calls=%d, want two attempts", err, delegate.calls)
	}
}

func TestNewResilientToolRejectsZeroPolicy(t *testing.T) {
	var policy resilience.Policy
	if _, err := NewResilientTool(&callableTool{}, &policy); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("NewResilientTool() = %v, want ErrUnavailable", err)
	}
}

func TestResilientToolRejectsSuccessfulFallbackWithoutToolResult(t *testing.T) {
	policy, err := resilience.New(resilience.Config{
		Timeout: time.Second, MaxAttempts: 1, FailureThreshold: 1, OpenTimeout: time.Second,
		Fallback: func(context.Context, error) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	tool, err := NewResilientTool(&callableTool{remainingFailures: 1}, policy)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := tool.Call(context.Background(), []byte(`{}`)); result != nil || !errors.Is(err, ErrUnavailable) {
		t.Fatalf("Call() result=%v err=%v, want unavailable without a tool result", result, err)
	}
}

type callableFactory struct {
	delegate trpctool.CallableTool
}

func (factory callableFactory) ID() string { return "callable" }

func (factory callableFactory) New() trpctool.Tool { return factory.delegate }

type callableTool struct {
	remainingFailures int
	calls             int
}

func (tool *callableTool) Declaration() *trpctool.Declaration {
	return &trpctool.Declaration{Name: "callable"}
}

func (tool *callableTool) Call(context.Context, []byte) (any, error) {
	tool.calls++
	if tool.remainingFailures > 0 {
		tool.remainingFailures--
		return nil, errors.New("temporary")
	}
	return "ok", nil
}
