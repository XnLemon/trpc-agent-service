package observability

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestNoopProviderLifecycleAndCorrelation(t *testing.T) {
	provider := NewNoopProvider()
	ctx := WithCorrelation(context.Background(), "req-1", "trace-1")
	if RequestID(ctx) != "req-1" || TraceID(ctx) != "trace-1" {
		t.Fatalf("correlation values were not preserved")
	}
	next, span := provider.Tracer("test").Start(ctx, OperationGatewayDispatch, Attribute{Key: "component", Value: "gateway"})
	span.SetStatus(StatusOK, "")
	span.RecordError(errors.New("provider detail"))
	span.End()
	if next == nil {
		t.Fatal("tracer returned nil context")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRedactionRemovesCredentialsAndRawFields(t *testing.T) {
	input := "Authorization: Bearer abc123 dsn=postgres://user:pass@example/db token=secret"
	redacted := RedactString(input)
	for _, secret := range []string{"abc123", "user:pass", "secret"} {
		if strings.Contains(redacted, secret) {
			t.Fatalf("redacted value contains %q: %s", secret, redacted)
		}
	}
	fields := RedactFields(map[string]string{"message": "user text", "api_key": "key", "operation": "runner.execution"})
	if fields["message"] != "<redacted>" || fields["api_key"] != "<redacted>" || fields["operation"] != "runner.execution" {
		t.Fatalf("unexpected fields: %#v", fields)
	}
}

func TestErrorClassAndDuration(t *testing.T) {
	if ErrorClass(context.Canceled) != "canceled" || ErrorClass(context.DeadlineExceeded) != "timeout" || ErrorClass(errors.New("x")) != "error" {
		t.Fatal("unexpected error classes")
	}
	if DurationMilliseconds(time.Now().Add(-time.Millisecond)) <= 0 {
		t.Fatal("duration should be positive")
	}
}

func TestOTLPProviderFallsBackToNoopWithoutEndpoint(t *testing.T) {
	provider, err := NewOTLPProvider(context.Background(), OTLPConfig{Headers: map[string]string{"authorization": "Bearer secret"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartOperationProvidesGenericHook(t *testing.T) {
	ctx, _, finish := StartOperation(context.Background(), NewNoopProvider(), OperationToolCall, "tool")
	if ctx == nil {
		t.Fatal("operation context is nil")
	}
	finish(context.Canceled)
}
