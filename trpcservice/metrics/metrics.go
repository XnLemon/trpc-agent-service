// Package metrics defines the low-cardinality runtime metric catalog.
package metrics

import (
	"context"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
)

const (
	RequestsTotal     = "trpcservice_requests_total"
	OperationDuration = "trpcservice_operation_duration_ms"
	ActiveExecutions  = "trpcservice_active_executions"
	RunnerLeases      = "trpcservice_runner_leases"
	OperationRetries  = "trpcservice_operation_retries_total"
	Readiness         = "trpcservice_readiness"
	Shutdown          = "trpcservice_shutdown"
)

var allowedLabels = map[string]struct{}{
	"component": {}, "operation": {}, "provider": {}, "channel": {},
	"status": {}, "error_class": {}, "model_family": {},
}

// ValidateLabels rejects high-cardinality or sensitive dimensions.
func ValidateLabels(labels map[string]string) error {
	for key := range labels {
		if _, ok := allowedLabels[key]; !ok {
			return fmt.Errorf("metric label %q is not allowed", key)
		}
	}
	return nil
}
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

type Catalog struct {
	requests  observability.Counter
	duration  observability.Histogram
	active    observability.UpDownCounter
	leases    observability.UpDownCounter
	retries   observability.Counter
	readiness observability.UpDownCounter
	shutdown  observability.UpDownCounter
}

func New(provider observability.Provider) Catalog {
	meter := provider.Meter("trpcservice.metrics")
	return Catalog{requests: meter.Counter(RequestsTotal), duration: meter.Histogram(OperationDuration), active: meter.UpDownCounter(ActiveExecutions), leases: meter.UpDownCounter(RunnerLeases), retries: meter.Counter(OperationRetries), readiness: meter.UpDownCounter(Readiness), shutdown: meter.UpDownCounter(Shutdown)}
}
func (c Catalog) Request(ctx context.Context, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.requests.Add(ctx, 1, attrs...)
	return nil
}
func (c Catalog) Duration(ctx context.Context, milliseconds float64, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.duration.Record(ctx, milliseconds, attrs...)
	return nil
}
func (c Catalog) Active(ctx context.Context, delta int64, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.active.Add(ctx, delta, attrs...)
	return nil
}
func (c Catalog) Lease(ctx context.Context, delta int64, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.leases.Add(ctx, delta, attrs...)
	return nil
}
func (c Catalog) Retry(ctx context.Context, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.retries.Add(ctx, 1, attrs...)
	return nil
}
func (c Catalog) State(ctx context.Context, readiness, shutdown int64, labels map[string]string) error {
	attrs, err := Attributes(labels)
	if err != nil {
		return err
	}
	c.readiness.Add(ctx, readiness, attrs...)
	c.shutdown.Add(ctx, shutdown, attrs...)
	return nil
}
