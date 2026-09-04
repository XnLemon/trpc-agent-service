package runtimeprofile

import (
	"context"
	"testing"
)

func profile() RuntimeProfile {
	return RuntimeProfile{TenantID: "t_00000000000000000000000000", ProfileID: "profile", RuntimeKey: "custom", RuntimeKind: "custom-agent", ExecutionMode: "in_process", ImplementationRef: "custom", Version: 1, RuntimeVersion: "v1", SchemaVersion: 1, ImplementationDigest: "sha256:x", ConfigDigest: "sha256:y", Config: map[string]any{"x": 1}, Capabilities: []string{"text"}, GovernanceMode: "full", Status: "draft"}
}
func TestInMemoryRepositoryLifecycle(t *testing.T) {
	r := NewInMemoryRepository()
	p := profile()
	got, err := r.Create(context.Background(), p)
	if err != nil || got.Version != 1 {
		t.Fatalf("create: %+v %v", got, err)
	}
	got.Status = "active"
	updated, err := r.Update(context.Background(), got, 1)
	if err != nil || updated.Version != 2 {
		t.Fatalf("update: %+v %v", updated, err)
	}
	if _, err := r.Update(context.Background(), updated, 1); err != ErrConflict {
		t.Fatalf("conflict=%v", err)
	}
	items, err := r.List(context.Background(), p.TenantID)
	if err != nil || len(items) != 1 {
		t.Fatalf("list=%v %v", items, err)
	}
}

func TestProfileRejectsSecretBearingConfig(t *testing.T) {
	p := profile(); p.Config = map[string]any{"api_token": "value"}
	if err := p.Validate(); err == nil { t.Fatal("expected secret config rejection") }
}
