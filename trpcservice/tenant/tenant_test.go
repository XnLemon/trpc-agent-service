package tenant

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func validCreate(key string) CreateInput {
	return CreateInput{TenantKey: key, DisplayName: "Example", AuditRetentionDays: 30, LogMaskingLevel: MaskingBasic, TraceSamplingRate: 1}
}

func TestNewTenantDefaultsAndID(t *testing.T) {
	tenant, err := NewTenant(validCreate(" Acme-1 "))
	if err != nil {
		t.Fatal(err)
	}
	if tenant.TenantKey != "acme-1" || tenant.Status != StatusActive || tenant.Version != 1 {
		t.Fatalf("unexpected tenant: %+v", tenant)
	}
	if err := validateTenantID(tenant.TenantID); err != nil {
		t.Fatal(err)
	}
	if tenant.AuditRetentionDays != 30 {
		t.Fatalf("retention changed: %d", tenant.AuditRetentionDays)
	}
}

func TestNewTenantValidationAndQuotaSemantics(t *testing.T) {
	zero := int64(0)
	input := validCreate("zero")
	input.RateLimitRPM = &zero
	input.MonthlyTokenBudget = &zero
	tenant, err := NewTenant(input)
	if err != nil {
		t.Fatal(err)
	}
	if tenant.RateLimitRPM == nil || *tenant.RateLimitRPM != 0 || tenant.MonthlyTokenBudget == nil || *tenant.MonthlyTokenBudget != 0 {
		t.Fatal("zero quota must remain distinct from nil")
	}
	bad := validCreate("bad")
	bad.MonthlySpendLimitMinor = &zero
	if _, err := NewTenant(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected currency validation, got %v", err)
	}
	bad = validCreate("bad-sampling")
	bad.TraceSamplingRate = 1.1
	if _, err := NewTenant(bad); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected sampling validation, got %v", err)
	}
}

func TestInMemoryIsolationAndOptimisticLock(t *testing.T) {
	r := NewInMemoryRepository()
	ctx := context.Background()
	a, err := r.Create(ctx, validCreate("aa"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Create(ctx, validCreate("AA")); !errors.Is(err, ErrDuplicateKey) {
		t.Fatalf("expected duplicate key, got %v", err)
	}
	copyA, err := r.Get(ctx, a.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	copyA.DisplayName = "mutated"
	check, _ := r.Get(ctx, a.TenantID)
	if check.DisplayName == "mutated" {
		t.Fatal("repository leaked internal pointer")
	}
	if _, err := r.UpdateConfiguration(ctx, UpdateConfigurationInput{TenantID: a.TenantID, ExpectedVersion: 99, DisplayName: "New", AuditRetentionDays: 30, LogMaskingLevel: MaskingBasic, TraceSamplingRate: 1}); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestStatusTransitionsAndEvent(t *testing.T) {
	r := NewInMemoryRepository()
	a, err := r.Create(context.Background(), validCreate("status"))
	if err != nil {
		t.Fatal(err)
	}
	meta := TransitionMetadata{ActorType: "admin", ActorID: "u1", Reason: "maintenance", CorrelationID: "c1"}
	suspended, event, err := r.TransitionStatus(context.Background(), TransitionStatusInput{TenantID: a.TenantID, ExpectedVersion: 1, NextStatus: StatusSuspended, Metadata: meta})
	if err != nil || suspended.CanAcceptExecution() || event.NextVersion != 2 {
		t.Fatalf("unexpected transition: %+v %+v %v", suspended, event, err)
	}
	if _, _, err := r.TransitionStatus(context.Background(), TransitionStatusInput{TenantID: a.TenantID, ExpectedVersion: 2, NextStatus: StatusSuspended, Metadata: meta}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("expected invalid transition, got %v", err)
	}
	disabled, _, err := r.TransitionStatus(context.Background(), TransitionStatusInput{TenantID: a.TenantID, ExpectedVersion: 2, NextStatus: StatusDisabled, Metadata: meta})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.UpdateConfiguration(context.Background(), UpdateConfigurationInput{TenantID: a.TenantID, ExpectedVersion: disabled.Version, DisplayName: "blocked", AuditRetentionDays: 30, LogMaskingLevel: MaskingBasic, TraceSamplingRate: 1}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("expected disabled guard, got %v", err)
	}
}

func TestConcurrentUpdatesHaveSingleWinner(t *testing.T) {
	r := NewInMemoryRepository()
	a, err := r.Create(context.Background(), validCreate("race"))
	if err != nil {
		t.Fatal(err)
	}
	input := func(name string) UpdateConfigurationInput {
		return UpdateConfigurationInput{TenantID: a.TenantID, ExpectedVersion: 1, DisplayName: name, AuditRetentionDays: 30, LogMaskingLevel: MaskingBasic, TraceSamplingRate: 1}
	}
	var wg sync.WaitGroup
	wg.Add(2)
	results := make(chan error, 2)
	for _, name := range []string{"one", "two"} {
		go func(name string) {
			defer wg.Done()
			_, err := r.UpdateConfiguration(context.Background(), input(name))
			results <- err
		}(name)
	}
	wg.Wait()
	close(results)
	var successes, conflicts int
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrConflict) {
			conflicts++
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}
