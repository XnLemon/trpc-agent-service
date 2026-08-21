package tenant

import (
	"errors"
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
