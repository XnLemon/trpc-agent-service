package observability

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
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

func TestTelemetryAdaptersEnforceSafeFieldsAndWrapSDKPrimitives(t *testing.T) {
	var logs bytes.Buffer
	provider := NewProvider(Config{
		TracerProvider: tracenoop.NewTracerProvider(),
		MeterProvider:  metricnoop.NewMeterProvider(),
		Logger:         slog.New(slog.NewJSONHandler(&logs, nil)),
	})
	ctx, span := provider.Tracer("test").Start(context.Background(), OperationToolCall,
		Attribute{Key: "operation", Value: OperationToolCall},
		Attribute{Key: "message", Value: "user text"},
		Attribute{Key: "token", Value: "secret"})
	span.SetAttributes(Attribute{Key: "status", Value: "ok"}, Attribute{Key: "raw", Value: "payload"})
	span.RecordError(errors.New("token=secret"))
	span.End()
	meter := provider.Meter("test")
	meter.Counter("requests").Add(ctx, 1, Attribute{Key: "component", Value: "tool"})
	meter.Histogram("duration").Record(ctx, 1, Attribute{Key: "operation", Value: OperationToolCall})
	meter.UpDownCounter("active").Add(ctx, 1, Attribute{Key: "status", Value: "ok"})
	provider.Logger().Log(ctx, LevelInfo, "Authorization: Bearer secret", Attribute{Key: "message", Value: "user text"}, Attribute{Key: "operation", Value: OperationToolCall})
	if strings.Contains(logs.String(), "user text") || strings.Contains(logs.String(), "secret") {
		t.Fatalf("unsafe log content: %s", logs.String())
	}
}

func TestProviderBranchesAndOTLPConstruction(t *testing.T) {
	otlp, err := NewOTLPProvider(context.Background(), OTLPConfig{Endpoint: "127.0.0.1:4318", Insecure: true, Headers: map[string]string{"authorization": "Bearer secret"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := otlp.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	var shutdowns int
	provider := NewProvider(Config{Shutdown: func(context.Context) error { shutdowns++; return nil }})
	if provider.Tracer("test") == nil || provider.Meter("test") == nil || provider.Logger() == nil {
		t.Fatal("provider components must be available")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if shutdowns != 1 {
		t.Fatalf("shutdown lifecycle = %d", shutdowns)
	}
	var discard discardWriter
	if n, err := discard.Write([]byte("x")); n != 1 || err != nil {
		t.Fatalf("discard writer = %d/%v", n, err)
	}
	ctx := WithCorrelation(context.TODO(), "req", "trace")
	if RequestID(ctx) != "req" || TraceID(ctx) != "trace" {
		t.Fatal("nil/correlation context handling failed")
	}
}
