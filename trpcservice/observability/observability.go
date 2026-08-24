// Package observability defines the provider-neutral runtime telemetry contract.
// Concrete SDKs are intentionally hidden behind these small interfaces.
package observability

import (
	"context"
	"errors"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

type Attribute struct{ Key, Value string }

type Span interface {
	End()
	SetAttributes(...Attribute)
	SetStatus(Status, string)
	RecordError(error)
}
type Tracer interface {
	Start(context.Context, string, ...Attribute) (context.Context, Span)
}
type Counter interface {
	Add(context.Context, int64, ...Attribute)
}
type Histogram interface {
	Record(context.Context, float64, ...Attribute)
}
type UpDownCounter interface {
	Add(context.Context, int64, ...Attribute)
}
type Meter interface {
	Counter(string) Counter
	Histogram(string) Histogram
	UpDownCounter(string) UpDownCounter
}
type Logger interface {
	Log(context.Context, Level, string, ...Attribute)
}
type Provider interface {
	Tracer(string) Tracer
	Meter(string) Meter
	Logger() Logger
	Shutdown(context.Context) error
}

type Status string

const (
	StatusUnset Status = "unset"
	StatusOK    Status = "ok"
	StatusError Status = "error"
)

type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

const (
	OperationHTTPRequest      = "http.request"
	OperationGatewayDispatch  = "gateway.dispatch"
	OperationRunnerExecution  = "runner.execution"
	OperationModelCall        = "model.call"
	OperationToolCall         = "tool.call"
	OperationStorageOperation = "storage.operation"
	OperationChannelReceive   = "channel.receive"
	OperationChannelSend      = "channel.send"
)

type Config struct {
	ServiceName    string
	TracerProvider trace.TracerProvider
	MeterProvider  metric.MeterProvider
	Logger         *slog.Logger
	Shutdown       func(context.Context) error
}

// OTLPConfig contains only exporter configuration; no secret is retained in the provider.
type OTLPConfig struct {
	ServiceName string
	Endpoint    string
	Headers     map[string]string
	Insecure    bool
}

// NewOTLPProvider creates a standard OTLP/HTTP provider. An empty endpoint deliberately
// selects the no-op provider so local development never requires an exporter.
func NewOTLPProvider(ctx context.Context, config OTLPConfig) (Provider, error) {
	if config.Endpoint == "" {
		return NewNoopProvider(), nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	options := []otlptracehttp.Option{otlptracehttp.WithEndpoint(config.Endpoint)}
	if config.Insecure {
		options = append(options, otlptracehttp.WithInsecure())
	}
	if len(config.Headers) > 0 {
		options = append(options, otlptracehttp.WithHeaders(RedactFields(config.Headers)))
	}
	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return NewNoopProvider(), err
	}
	if config.ServiceName == "" {
		config.ServiceName = "trpc-agent-service"
	}
	resourceValue, err := resource.New(ctx, resource.WithAttributes(semconv.ServiceName(config.ServiceName)))
	if err != nil {
		_ = exporter.Shutdown(ctx)
		return NewNoopProvider(), err
	}
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(resourceValue))
	return NewProvider(Config{ServiceName: config.ServiceName, TracerProvider: tracerProvider, Shutdown: tracerProvider.Shutdown}), nil
}

type provider struct {
	tracer   Tracer
	meter    Meter
	logger   Logger
	shutdown func(context.Context) error
	once     sync.Once
	closeErr error
}

func NewProvider(config Config) Provider {
	if config.ServiceName == "" {
		config.ServiceName = "trpc-agent-service"
	}
	tp := config.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := config.MeterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	p := &provider{tracer: otelTracer{tracer: tp.Tracer(config.ServiceName)}, meter: otelMeter{meter: mp.Meter(config.ServiceName)}, logger: slogLogger{logger: logger}}
	if config.Shutdown != nil {
		p.shutdown = config.Shutdown
	} else {
		p.shutdown = func(context.Context) error { return nil }
	}
	return p
}

func NewNoopProvider() Provider {
	return NewProvider(Config{TracerProvider: trace.NewNoopTracerProvider(), MeterProvider: metricnoop.NewMeterProvider(), Logger: slog.New(slog.NewTextHandler(discardWriter{}, nil))})
}

func (p *provider) Tracer(string) Tracer { return p.tracer }
func (p *provider) Meter(string) Meter   { return p.meter }
func (p *provider) Logger() Logger       { return p.logger }
func (p *provider) Shutdown(ctx context.Context) error {
	p.once.Do(func() { p.closeErr = p.shutdown(ctx) })
	return p.closeErr
}

type discardWriter struct{}

func (discardWriter) Write(b []byte) (int, error) { return len(b), nil }

type otelTracer struct{ tracer trace.Tracer }

func (t otelTracer) Start(ctx context.Context, name string, attrs ...Attribute) (context.Context, Span) {
	values := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		values = append(values, attribute.String(a.Key, a.Value))
	}
	next, span := t.tracer.Start(ctx, name, trace.WithAttributes(values...))
	return next, otelSpan{span: span}
}

type otelSpan struct{ span trace.Span }

func (s otelSpan) End() { s.span.End() }
func (s otelSpan) SetAttributes(attrs ...Attribute) {
	values := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		values = append(values, attribute.String(a.Key, a.Value))
	}
	s.span.SetAttributes(values...)
}
func (s otelSpan) SetStatus(status Status, description string) {
	code := codes.Unset
	if status == StatusOK {
		code = codes.Ok
	}
	if status == StatusError {
		code = codes.Error
	}
	s.span.SetStatus(code, description)
}
func (s otelSpan) RecordError(err error) {
	if err != nil {
		s.span.RecordError(err)
	}
}

type otelMeter struct{ meter metric.Meter }

func (m otelMeter) Counter(name string) Counter {
	c, _ := m.meter.Int64Counter(name)
	return otelCounter{c}
}
func (m otelMeter) Histogram(name string) Histogram {
	h, _ := m.meter.Float64Histogram(name)
	return otelHistogram{h}
}
func (m otelMeter) UpDownCounter(name string) UpDownCounter {
	c, _ := m.meter.Int64UpDownCounter(name)
	return otelUpDownCounter{c}
}
func kv(attrs []Attribute) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, attribute.String(a.Key, a.Value))
	}
	return out
}

type otelCounter struct{ c metric.Int64Counter }

func (c otelCounter) Add(ctx context.Context, v int64, attrs ...Attribute) {
	c.c.Add(ctx, v, metric.WithAttributes(kv(attrs)...))
}

type otelHistogram struct{ h metric.Float64Histogram }

func (h otelHistogram) Record(ctx context.Context, v float64, attrs ...Attribute) {
	h.h.Record(ctx, v, metric.WithAttributes(kv(attrs)...))
}

type otelUpDownCounter struct{ c metric.Int64UpDownCounter }

func (c otelUpDownCounter) Add(ctx context.Context, v int64, attrs ...Attribute) {
	c.c.Add(ctx, v, metric.WithAttributes(kv(attrs)...))
}

type slogLogger struct{ logger *slog.Logger }

func (l slogLogger) Log(ctx context.Context, level Level, msg string, attrs ...Attribute) {
	args := make([]any, 0, len(attrs)*2)
	for _, a := range attrs {
		args = append(args, a.Key, RedactString(a.Value))
	}
	slogLevel := slog.LevelInfo
	switch level {
	case LevelDebug:
		slogLevel = slog.LevelDebug
	case LevelWarn:
		slogLevel = slog.LevelWarn
	case LevelError:
		slogLevel = slog.LevelError
	}
	l.logger.Log(ctx, slogLevel, msg, args...)
}

var sensitivePattern = regexp.MustCompile(`(?i)(bearer\s+|api[_-]?key\s*[=:]\s*|token\s*[=:]\s*|authorization\s*[=:]\s*|secret(?:[_-]?ref)?\s*[=:]\s*|password\s*[=:]\s*)[^\s,;]+`)
var dsnPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://)([^/@\s]+):([^/@\s]+)@`)
var bearerPattern = regexp.MustCompile(`(?i)Bearer\s+[^\s,;]+`)

func RedactString(value string) string {
	value = bearerPattern.ReplaceAllString(value, "Bearer <redacted>")
	value = dsnPattern.ReplaceAllString(value, `${1}<redacted>@`)
	value = sensitivePattern.ReplaceAllString(value, `${1}<redacted>`)
	return value
}
func RedactFields(fields map[string]string) map[string]string {
	out := make(map[string]string, len(fields))
	for key, value := range fields {
		lower := strings.ToLower(key)
		if strings.Contains(lower, "secret") || strings.Contains(lower, "token") || strings.Contains(lower, "password") || (strings.Contains(lower, "api") && strings.Contains(lower, "key")) || lower == "authorization" || strings.Contains(lower, "message") || strings.Contains(lower, "raw") {
			out[key] = "<redacted>"
		} else {
			out[key] = RedactString(value)
		}
	}
	return out
}

type contextKey string

const (
	requestIDKey contextKey = "trpcservice.observability.request_id"
	traceIDKey   contextKey = "trpcservice.observability.trace_id"
)

func WithCorrelation(ctx context.Context, requestID, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, requestIDKey, requestID)
	return context.WithValue(ctx, traceIDKey, traceID)
}
func RequestID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(requestIDKey).(string)
	return value
}
func TraceID(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	value, _ := ctx.Value(traceIDKey).(string)
	return value
}
func ErrorClass(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	return "error"
}
func DurationMilliseconds(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000
}

// StartOperation is the common hook used by Model, Tool, Storage and Channel
// adapters. The returned finish function records a stable status/error class
// and always ends the span; it never exposes provider error text.
func StartOperation(ctx context.Context, provider Provider, operation, component string) (context.Context, Span, func(error)) {
	if provider == nil {
		provider = NewNoopProvider()
	}
	started, span := provider.Tracer("trpcservice.operations").Start(ctx, operation, Attribute{Key: "component", Value: component}, Attribute{Key: "operation", Value: operation})
	finish := func(err error) {
		if err != nil {
			class := ErrorClass(err)
			span.SetAttributes(Attribute{Key: "error_class", Value: class})
			span.SetStatus(StatusError, class)
			span.RecordError(err)
		} else {
			span.SetStatus(StatusOK, "")
		}
		span.End()
	}
	return started, span, finish
}
