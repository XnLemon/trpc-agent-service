package runtime

import (
	"context"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestTelemetryOptionsRecordsModelAndToolOutcomes(t *testing.T) { //nolint:gocyclo -- table-like callback contract coverage
	t.Run("model success records usage", func(t *testing.T) {
		provider := &runtimeTelemetryProvider{}
		options := applyTelemetryOptions(t, provider)
		before, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
		if err != nil || before == nil || before.Context == nil {
			t.Fatalf("before model = %#v, %v", before, err)
		}
		if _, err := options.ModelCallbacks.RunAfterModel(before.Context, &trpcmodel.AfterModelArgs{Response: &trpcmodel.Response{Usage: &trpcmodel.Usage{PromptTokens: 7, CompletionTokens: 11}}}); err != nil {
			t.Fatal(err)
		}

		assertTelemetrySpan(t, provider, observability.OperationModelCall, observability.StatusOK, false)
		assertTelemetryMetric(t, provider, metrics.RequestsTotal, 1, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": "openai", "model_family": "gpt-family", "status": "started"})
		assertTelemetryMetric(t, provider, metrics.TokensTotal, 7, map[string]string{"component": "model", "provider": "openai", "model_family": "gpt-family"})
		assertTelemetryMetric(t, provider, metrics.TokensTotal, 11, map[string]string{"component": "model", "provider": "openai", "model_family": "gpt-family"})
		assertTelemetryMetric(t, provider, metrics.OperationDuration, -1, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": "openai", "model_family": "gpt-family", "status": "success", "error_class": ""})
	})

	t.Run("model error records status", func(t *testing.T) {
		provider := &runtimeTelemetryProvider{}
		options := applyTelemetryOptions(t, provider)
		before, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := options.ModelCallbacks.RunAfterModel(before.Context, &trpcmodel.AfterModelArgs{Error: context.Canceled}); err != nil {
			t.Fatal(err)
		}
		assertTelemetrySpan(t, provider, observability.OperationModelCall, observability.StatusError, true)
		assertTelemetryMetric(t, provider, metrics.OperationDuration, -1, map[string]string{"component": "model", "operation": observability.OperationModelCall, "provider": "openai", "model_family": "gpt-family", "status": "error", "error_class": "canceled"})
	})

	t.Run("tool success and error record outcomes", func(t *testing.T) {
		provider := &runtimeTelemetryProvider{}
		options := applyTelemetryOptions(t, provider)
		before, err := options.ToolCallbacks.RunBeforeTool(context.Background(), &trpctool.BeforeToolArgs{ToolName: "lookup"})
		if err != nil || before == nil || before.Context == nil {
			t.Fatalf("before tool = %#v, %v", before, err)
		}
		if _, err := options.ToolCallbacks.RunAfterTool(before.Context, &trpctool.AfterToolArgs{}); err != nil {
			t.Fatal(err)
		}
		before, err = options.ToolCallbacks.RunBeforeTool(context.Background(), &trpctool.BeforeToolArgs{ToolName: "lookup"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := options.ToolCallbacks.RunAfterTool(before.Context, &trpctool.AfterToolArgs{Error: context.DeadlineExceeded}); err != nil {
			t.Fatal(err)
		}

		if len(provider.spans) != 2 {
			t.Fatalf("tool spans = %d, want 2", len(provider.spans))
		}
		if provider.spans[0].status != observability.StatusOK || !provider.spans[0].ended || provider.spans[0].recordedError != nil {
			t.Fatalf("success tool span = %+v", provider.spans[0])
		}
		if provider.spans[1].status != observability.StatusError || !provider.spans[1].ended || provider.spans[1].recordedError == nil {
			t.Fatalf("error tool span = %+v", provider.spans[1])
		}
		assertTelemetryMetric(t, provider, metrics.RequestsTotal, 1, map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "started"})
		assertTelemetryMetric(t, provider, metrics.OperationDuration, -1, map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "success", "error_class": ""})
		assertTelemetryMetric(t, provider, metrics.OperationDuration, -1, map[string]string{"component": "tool", "operation": observability.OperationToolCall, "status": "error", "error_class": "timeout"})
	})

	t.Run("nil callback args are ignored safely", func(t *testing.T) {
		provider := &runtimeTelemetryProvider{}
		options := applyTelemetryOptions(t, provider)
		modelBefore, err := options.ModelCallbacks.RunBeforeModel(context.Background(), &trpcmodel.BeforeModelArgs{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := options.ModelCallbacks.RunAfterModel(modelBefore.Context, nil); err != nil {
			t.Fatal(err)
		}
		if len(provider.spans) != 1 {
			t.Fatalf("spans = %d, want 1", len(provider.spans))
		}
		if span := provider.spans[0]; span.status != observability.StatusOK || !span.ended || span.recordedError != nil {
			t.Fatalf("nil args span = %+v", span)
		}
	})
}

func applyTelemetryOptions(t *testing.T, provider observability.Provider) llmagent.Options {
	t.Helper()
	options := llmagent.Options{}
	for _, option := range telemetryOptions(provider, "openai", "gpt-family") {
		option(&options)
	}
	if options.ModelCallbacks == nil || options.ToolCallbacks == nil {
		t.Fatalf("telemetry callbacks = model %v, tool %v", options.ModelCallbacks, options.ToolCallbacks)
	}
	return options
}

func assertTelemetrySpan(t *testing.T, provider *runtimeTelemetryProvider, operation string, status observability.Status, wantError bool) {
	t.Helper()
	if len(provider.spans) == 0 {
		t.Fatal("no telemetry span recorded")
	}
	span := provider.spans[len(provider.spans)-1]
	if span.name != operation || span.status != status || !span.ended || (span.recordedError != nil) != wantError {
		t.Fatalf("span = %+v, want operation=%q status=%q error=%v", span, operation, status, wantError)
	}
}

func assertTelemetryMetric(t *testing.T, provider *runtimeTelemetryProvider, name string, wantValue float64, wantLabels map[string]string) {
	t.Helper()
	for _, metric := range provider.metrics {
		if metric.name == name && (wantValue < 0 || metric.value == wantValue) && telemetryLabelsEqual(metric.attrs, wantLabels) {
			return
		}
	}
	t.Fatalf("metric %q with value %v and labels %#v not found in %#v", name, wantValue, wantLabels, provider.metrics)
}

func telemetryLabelsEqual(attrs []observability.Attribute, want map[string]string) bool {
	if len(attrs) != len(want) {
		return false
	}
	for _, attr := range attrs {
		if want[attr.Key] != attr.Value {
			return false
		}
	}
	return true
}

type runtimeTelemetryProvider struct {
	spans   []*runtimeTelemetrySpan
	metrics []runtimeTelemetryMetric
}

func (p *runtimeTelemetryProvider) Tracer(string) observability.Tracer {
	return runtimeTelemetryTracer{provider: p}
}
func (p *runtimeTelemetryProvider) Meter(string) observability.Meter {
	return runtimeTelemetryMeter{provider: p}
}
func (p *runtimeTelemetryProvider) Logger() observability.Logger   { return runtimeTelemetryLogger{} }
func (p *runtimeTelemetryProvider) Shutdown(context.Context) error { return nil }

type runtimeTelemetryTracer struct{ provider *runtimeTelemetryProvider }

func (t runtimeTelemetryTracer) Start(ctx context.Context, name string, attrs ...observability.Attribute) (context.Context, observability.Span) {
	span := &runtimeTelemetrySpan{name: name, attrs: append([]observability.Attribute(nil), attrs...)}
	t.provider.spans = append(t.provider.spans, span)
	return ctx, span
}

type runtimeTelemetrySpan struct {
	name          string
	attrs         []observability.Attribute
	status        observability.Status
	recordedError error
	ended         bool
}

func (s *runtimeTelemetrySpan) End() { s.ended = true }
func (s *runtimeTelemetrySpan) SetAttributes(attrs ...observability.Attribute) {
	s.attrs = append(s.attrs, attrs...)
}
func (s *runtimeTelemetrySpan) SetStatus(status observability.Status, _ string) { s.status = status }
func (s *runtimeTelemetrySpan) RecordError(err error)                           { s.recordedError = err }

type runtimeTelemetryMeter struct{ provider *runtimeTelemetryProvider }

func (m runtimeTelemetryMeter) Counter(name string) observability.Counter {
	return runtimeTelemetryCounter{provider: m.provider, name: name}
}
func (m runtimeTelemetryMeter) Histogram(name string) observability.Histogram {
	return runtimeTelemetryHistogram{provider: m.provider, name: name}
}
func (m runtimeTelemetryMeter) UpDownCounter(string) observability.UpDownCounter {
	return runtimeTelemetryUpDownCounter{}
}

type runtimeTelemetryMetric struct {
	name  string
	value float64
	attrs []observability.Attribute
}

type runtimeTelemetryCounter struct {
	provider *runtimeTelemetryProvider
	name     string
}

func (c runtimeTelemetryCounter) Add(_ context.Context, value int64, attrs ...observability.Attribute) {
	c.provider.metrics = append(c.provider.metrics, runtimeTelemetryMetric{name: c.name, value: float64(value), attrs: append([]observability.Attribute(nil), attrs...)})
}

type runtimeTelemetryHistogram struct {
	provider *runtimeTelemetryProvider
	name     string
}

func (h runtimeTelemetryHistogram) Record(_ context.Context, value float64, attrs ...observability.Attribute) {
	h.provider.metrics = append(h.provider.metrics, runtimeTelemetryMetric{name: h.name, value: value, attrs: append([]observability.Attribute(nil), attrs...)})
}

type runtimeTelemetryUpDownCounter struct{}

func (runtimeTelemetryUpDownCounter) Add(context.Context, int64, ...observability.Attribute) {}

type runtimeTelemetryLogger struct{}

func (runtimeTelemetryLogger) Log(context.Context, observability.Level, string, ...observability.Attribute) {
}
