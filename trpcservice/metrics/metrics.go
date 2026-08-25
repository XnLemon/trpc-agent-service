// Package metrics defines the low-cardinality runtime metric catalog.
package metrics

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
)

const (
	// RequestsTotal counts handled requests.
	RequestsTotal = "trpcservice_requests_total"
	// OperationDuration records operation latency.
	OperationDuration = "trpcservice_operation_duration_ms"
	// ActiveExecutions tracks currently running executions.
	ActiveExecutions  = "trpcservice_active_executions"
	// RunnerLeases tracks active runner leases.
	RunnerLeases      = "trpcservice_runner_leases"
	// OperationRetries counts retried operations.
	OperationRetries  = "trpcservice_operation_retries_total"
	// UsageCostTotal counts aggregated usage cost.
	UsageCostTotal    = "trpcservice_usage_cost_minor_total"
	// Readiness tracks service readiness.
	Readiness         = "trpcservice_readiness"
	// Shutdown tracks service shutdown.
	Shutdown          = "trpcservice_shutdown"
)

var allowedLabels = map[string]struct{}{
	"component": {}, "operation": {}, "provider": {}, "channel": {},
	"status": {}, "error_class": {}, "model_family": {},
}
var allowedValues = map[string]map[string]struct{}{
	"component":   {"http": {}, "gateway": {}, "runner": {}, "model": {}, "tool": {}, "storage": {}, "channel": {}},
	"operation":   {observability.OperationHTTPRequest: {}, observability.OperationGatewayDispatch: {}, observability.OperationRunnerExecution: {}, observability.OperationModelCall: {}, observability.OperationToolCall: {}, observability.OperationStorageOperation: {}, observability.OperationChannelReceive: {}, observability.OperationChannelSend: {}},
	"status":      {"started": {}, "complete": {}, "ok": {}, "error": {}, "success": {}, "failure": {}, "canceled": {}, "timeout": {}, "retry": {}},
	"error_class": {"": {}, "error": {}, "canceled": {}, "timeout": {}, "invalid": {}, "unauthenticated": {}, "not_ready": {}, "rate_limited": {}, "duplicate": {}, "unavailable": {}, "storage": {}, "model": {}, "tool": {}},
}
var highCardinalityPattern = regexp.MustCompile(`(?i)(session|user|message|request|trace|[0-9a-f]{16,}|https?://)`)

// ValidateLabels rejects high-cardinality or sensitive dimensions.
func ValidateLabels(labels map[string]string) error {
	for key, value := range labels {
		if _, ok := allowedLabels[key]; !ok {
			return fmt.Errorf("metric label %q is not allowed", key)
		}
		if values, ok := allowedValues[key]; ok {
			if _, allowed := values[value]; !allowed {
				return fmt.Errorf("metric label %q has unsupported value", key)
			}
		} else if len(value) > 64 || strings.ContainsAny(value, "\r\n") || highCardinalityPattern.MatchString(value) {
			return fmt.Errorf("metric label %q has high-cardinality value", key)
		}
	}
	return nil
}

// Attributes validates labels and converts them to telemetry attributes.
func Attributes(labels map[string]string) ([]observability.Attribute, error) {
	if err := ValidateLabels(labels); err != nil {
		return nil, err
	}
	out := make([]observability.Attribute, 0, len(labels))
	for key, value := range labels {
		out = append(out, observability.Attribute{Key: key, Value: value})
	}
	return out, nil
}

// Catalog records bounded-cardinality runtime metrics.
type Catalog struct {
	requests  observability.Counter
	duration  observability.Histogram
	active    observability.UpDownCounter
	leases    observability.UpDownCounter
	retries   observability.Counter
	usage     observability.Counter
	readiness observability.UpDownCounter
	shutdown  observability.UpDownCounter
}

// New creates a metric catalog backed by provider.
func New(provider observability.Provider) Catalog {
	meter := provider.Meter("trpcservice.metrics")
	return Catalog{requests: meter.Counter(RequestsTotal), duration: meter.Histogram(OperationDuration), active: meter.UpDownCounter(ActiveExecutions), leases: meter.UpDownCounter(RunnerLeases), retries: meter.Counter(OperationRetries), usage: meter.Counter(UsageCostTotal), readiness: meter.UpDownCounter(Readiness), shutdown: meter.UpDownCounter(Shutdown)}
}

// Usage records aggregated cost with only bounded dimensions. Tenant and app
// are intentionally represented by coarse configured labels supplied by the
// caller; session/user/request identifiers are never accepted here.
func (c Catalog) Usage(ctx context.Context, total audit.UsageTotal, labels map[string]string) error {
	if labels == nil {
		labels = map[string]string{}
	}
	if total.Channel != "" {
		labels["channel"] = total.Channel
	}
	if total.Provider != "" {
		labels["provider"] = total.Provider
	}
	if err := ValidateLabels(labels); err != nil {
		return err
	}
	if total.ModelCostMinor != 0 {
		c.usage.Add(ctx, total.ModelCostMinor, mustAttributes(labels)...)
	}
	if total.ToolCostMinor != 0 {
		c.usage.Add(ctx, total.ToolCostMinor, mustAttributes(labels)...)
	}
	return nil
}

func mustAttributes(labels map[string]string) []observability.Attribute {
	attrs, _ := Attributes(labels)
	return attrs
}

// Request increments the request counter.
func (c Catalog) Request(ctx context.Context, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.requests.Add(ctx, 1, attrs...)
	return nil
}

// Duration records operation latency.
func (c Catalog) Duration(ctx context.Context, milliseconds float64, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.duration.Record(ctx, milliseconds, attrs...)
	return nil
}

// Active adjusts active executions.
func (c Catalog) Active(ctx context.Context, delta int64, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.active.Add(ctx, delta, attrs...)
	return nil
}

// Lease adjusts runner leases.
func (c Catalog) Lease(ctx context.Context, delta int64, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.leases.Add(ctx, delta, attrs...)
	return nil
}

// Retry increments operation retries.
func (c Catalog) Retry(ctx context.Context, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.retries.Add(ctx, 1, attrs...)
	return nil
}

// State adjusts readiness and shutdown gauges.
func (c Catalog) State(ctx context.Context, readiness, shutdown int64, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.readiness.Add(ctx, readiness, attrs...)
	c.shutdown.Add(ctx, shutdown, attrs...)
	return nil
}
