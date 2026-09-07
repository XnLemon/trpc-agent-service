package gateway

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
)

func TestUsageAccumulatorAndSaturatingAdd(t *testing.T) {
	var nilAccumulator *usageAccumulator
	if got := nilAccumulator.Snapshot(); got != (budget.Usage{}) {
		t.Fatalf("nil snapshot = %+v", got)
	}
	nilAccumulator.Observe(context.Background(), budget.Usage{InputTokens: 1})
	accumulator := &usageAccumulator{}
	accumulator.Observe(context.Background(), budget.Usage{InputTokens: 2, OutputTokens: 3})
	accumulator.Observe(context.Background(), budget.Usage{InputTokens: math.MaxInt64, OutputTokens: math.MaxInt64})
	if got := accumulator.Snapshot(); got.InputTokens != math.MaxInt64 || got.OutputTokens != math.MaxInt64 {
		t.Fatalf("saturated usage = %+v", got)
	}
	if got := saturatingAdd(-2, 1); got != -1 {
		t.Fatalf("negative add = %d", got)
	}
}

func TestBudgetAuditUsageResults(t *testing.T) {
	pricing := budget.Pricing{Configured: true, Currency: "USD"}
	for _, event := range []audit.EventType{audit.EventExecutionCompleted, audit.EventExecutionCanceled, audit.EventExecutionFailed, audit.EventExecutionFallback} {
		usage := budgetAuditUsage(budget.Usage{InputTokens: 2, OutputTokens: 3, SpendMinor: 4}, pricing, event, "provider", "model")
		if usage == nil || usage.Currency != "USD" {
			t.Fatalf("audit usage for %v = %+v", event, usage)
		}
	}
	if usage := budgetAuditUsage(budget.Usage{}, budget.Pricing{}, audit.EventExecutionCompleted, "", ""); usage.ModelCostMinor != nil {
		t.Fatalf("unconfigured pricing emitted cost: %+v", usage)
	}
}

func TestBudgetHelpersHandleNilAndClassification(t *testing.T) {
	ctx := context.Background()
	var dispatcher *Dispatcher
	if err := dispatcher.releaseBudget(ctx, budget.Reservation{}); err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.settleBudget(ctx, nil, audit.EventExecutionCompleted); err != nil {
		t.Fatal(err)
	}
	for _, err := range []error{budget.ErrExceeded, budget.ErrCostUnavailable, budget.ErrUnavailable} {
		if !isBudgetRejection(err) {
			t.Fatalf("%v not classified as budget rejection", err)
		}
	}
	if isBudgetRejection(errors.New("other")) {
		t.Fatal("unrelated error classified as budget rejection")
	}
	if err := budgetAdmissionAudit(ctx, nil, "tenant", "request", "trace"); err != nil {
		t.Fatal(err)
	}
}
