package budget_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget"
	budgetmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/budget/inmemory"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestPricingAndEstimateUseProviderNeutralRates(t *testing.T) {
	pricing, err := budget.ParsePricing(map[string]string{
		budget.InputCostOption: "2", budget.OutputCostOption: "4",
	}, "USD")
	if err != nil {
		t.Fatal(err)
	}
	if err := pricing.RequirePricing(); err != nil {
		t.Fatal(err)
	}
	cost, err := pricing.Cost(budget.Usage{InputTokens: 1_000_001, OutputTokens: 2_000_000})
	if err != nil || cost != 11 {
		t.Fatalf("cost = %d, err = %v, want 11", cost, err)
	}
	estimate, err := budget.EstimateExecution(2, 100, pricing)
	if err != nil {
		t.Fatal(err)
	}
	if estimate.InputTokens != 2*budget.DefaultInputTokensEstimate || estimate.OutputTokens != 200 || estimate.SpendMinor != 2 {
		t.Fatalf("execution estimate = %+v, want spend 2 minor units", estimate)
	}
	if _, err := budget.ParsePricing(map[string]string{budget.InputCostOption: "2"}, "USD"); !errors.Is(err, budget.ErrCostUnavailable) {
		t.Fatalf("partial pricing error = %v", err)
	}
}

func TestControllerReserveSettleAndReleaseIsIdempotent(t *testing.T) {
	store := budgetmemory.New()
	controller := budget.NewController(store)
	root := budgetTenant(t, 100, 100)
	ctx := context.Background()

	reservation, err := controller.Reserve(ctx, root, "request-1", budget.Estimate{InputTokens: 10, OutputTokens: 20, SpendMinor: 30})
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := controller.Reserve(ctx, root, "request-1", budget.Estimate{InputTokens: 10, OutputTokens: 20, SpendMinor: 30})
	if err != nil || duplicate.State != budget.ReservationStateReserved {
		t.Fatalf("idempotent reserve = %+v, err = %v", duplicate, err)
	}
	if _, err := controller.Reserve(ctx, root, "request-1", budget.Estimate{InputTokens: 11, OutputTokens: 20, SpendMinor: 30}); !errors.Is(err, budget.ErrConflict) {
		t.Fatalf("reserve idempotency conflict = %v", err)
	}

	settled, err := controller.Settle(ctx, reservation, budget.Usage{InputTokens: 4, OutputTokens: 6, SpendMinor: 7})
	if err != nil || settled.State != budget.ReservationStateSettled || settled.ActualTokens != 10 || settled.ActualSpendMinor != 7 {
		t.Fatalf("settlement = %+v, err = %v", settled, err)
	}
	if again, err := controller.Settle(ctx, reservation, budget.Usage{InputTokens: 4, OutputTokens: 6, SpendMinor: 7}); err != nil || again.State != budget.ReservationStateSettled {
		t.Fatalf("idempotent settlement = %+v, err = %v", again, err)
	}
	ledger, err := store.Snapshot(ctx, root.TenantID, time.Now().UTC())
	if err != nil || ledger.UsedTokens != 10 || ledger.UsedMinor != 7 || ledger.ReservedTokens != 0 || ledger.ReservedMinor != 0 {
		t.Fatalf("settled ledger = %+v, err = %v", ledger, err)
	}

	released, err := controller.Reserve(ctx, root, "request-2", budget.Estimate{InputTokens: 20, OutputTokens: 10, SpendMinor: 20})
	if err != nil {
		t.Fatal(err)
	}
	if released, err = controller.Release(ctx, released); err != nil || released.State != budget.ReservationStateReleased {
		t.Fatalf("release = %+v, err = %v", released, err)
	}
	if _, err := controller.Release(ctx, released); err != nil {
		t.Fatalf("idempotent release error = %v", err)
	}
}

func TestControllerFailsClosedAndRecordsActualOverage(t *testing.T) {
	root := budgetTenant(t, 10, 10)
	if _, err := budget.NewController(nil).Reserve(context.Background(), root, "request-1", budget.Estimate{InputTokens: 1}); !errors.Is(err, budget.ErrUnavailable) {
		t.Fatalf("nil store error = %v", err)
	}

	store := budgetmemory.New()
	controller := budget.NewController(store)
	reservation, err := controller.Reserve(context.Background(), root, "request-2", budget.Estimate{InputTokens: 2, SpendMinor: 2})
	if err != nil {
		t.Fatal(err)
	}
	settled, err := controller.Settle(context.Background(), reservation, budget.Usage{InputTokens: 20, SpendMinor: 20})
	if !errors.Is(err, budget.ErrExceeded) || settled.State != budget.ReservationStateSettled {
		t.Fatalf("overage settlement = %+v, err = %v", settled, err)
	}
	ledger, err := store.Snapshot(context.Background(), root.TenantID, time.Now().UTC())
	if err != nil || ledger.UsedTokens != 20 || ledger.UsedMinor != 20 {
		t.Fatalf("overage ledger = %+v, err = %v", ledger, err)
	}
}

func TestControllerDisablesTenantsWithoutLimits(t *testing.T) {
	root := budgetTenant(t, 0, 0)
	root.MonthlyTokenBudget = nil
	root.MonthlySpendLimitMinor = nil
	reservation, err := budget.NewController(nil).Reserve(context.Background(), root, "request-1", budget.Estimate{InputTokens: 1})
	if err != nil || reservation.State != budget.ReservationStateDisabled {
		t.Fatalf("disabled budget = %+v, err = %v", reservation, err)
	}
}

func budgetTenant(t *testing.T, tokenBudget, spendLimit int64) tenant.Tenant {
	t.Helper()
	root, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "budget-test", DisplayName: "Budget Test", MonthlyTokenBudget: &tokenBudget,
		MonthlySpendLimitMinor: &spendLimit, BillingCurrency: "USD", AuditRetentionDays: 30,
		LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return *root
}
