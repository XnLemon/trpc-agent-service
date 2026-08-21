package tenant

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestConfigurationSnapshotIsolatedFromContextAndSource(t *testing.T) {
	tenant, err := NewTenant(validCreate("snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := NewConfigurationSnapshot(tenant)
	if err != nil {
		t.Fatal(err)
	}
	tenant.DisplayName = "changed after snapshot"
	ctx := WithConfigurationSnapshot(context.Background(), snapshot)
	fromContext, ok := ConfigurationSnapshotFromContext(ctx)
	if !ok || fromContext.Tenant.DisplayName != "Example" {
		t.Fatalf("unexpected context snapshot: %+v", fromContext)
	}
	fromContext.Tenant.DisplayName = "caller mutation"
	again, _ := ConfigurationSnapshotFromContext(ctx)
	if again.Tenant.DisplayName != "Example" {
		t.Fatal("context exposed mutable snapshot")
	}
}

func TestRunnerIdentityUsesUnambiguousNamespace(t *testing.T) {
	tenantID := "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	first, err := NewRunnerIdentity(tenantID, "12", "3:45")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRunnerIdentity(tenantID, "1", "23:45")
	if err != nil {
		t.Fatal(err)
	}
	if first.UserID == second.UserID || first.SessionID == second.SessionID {
		t.Fatalf("ambiguous namespace: %+v %+v", first, second)
	}
}

func TestConfigurationSnapshotRejectsInvalidExternalTenant(t *testing.T) {
	tenant, err := NewTenant(validCreate("snapshot-invalid"))
	if err != nil {
		t.Fatal(err)
	}
	tenant.TenantKey = "Not-Normalized"
	if _, err := NewConfigurationSnapshot(tenant); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected invalid snapshot, got %v", err)
	}
}

func TestConfigurationSnapshotRequiresCompleteRootState(t *testing.T) {
	tenant, err := NewTenant(validCreate("snapshot-state"))
	if err != nil {
		t.Fatal(err)
	}
	tenant.CreatedAt = time.Time{}
	if err := tenant.Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("expected incomplete tenant rejection, got %v", err)
	}
}
