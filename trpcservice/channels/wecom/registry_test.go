package wecom

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestRegistryScopesAccountsAndWorkerGroupCloses(t *testing.T) {
	r := NewRegistry()
	p1 := &Provider{}
	p2 := &Provider{}
	if err := r.Register(Account{TenantID: "tenant-a", AccountID: "one", Provider: p1}); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(Account{TenantID: "tenant-b", AccountID: "one", Provider: p2}); err != nil {
		t.Fatal(err)
	}
	got, _ := r.Resolve("tenant-a", "one")
	if got != p1 {
		t.Fatal("tenant account crossed provider boundary")
	}
	group, err := NewWorkerGroup(r, 1)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	if err := group.Dispatch(context.Background(), "tenant-a", "one", func(context.Context, *Provider) error { calls.Add(1); return nil }); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d", calls.Load())
	}
	if err := group.Close(); err != nil {
		t.Fatal(err)
	}
	if err := group.Dispatch(context.Background(), "tenant-a", "one", func(context.Context, *Provider) error { return nil }); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("dispatch after close = %v", err)
	}
}

func TestRegistryRemoveAndCloseLifecycle(t *testing.T) {
	r := NewRegistry()
	provider := &Provider{}
	account := Account{TenantID: "tenant-a", AccountID: "account-1", Provider: provider}
	if err := r.Register(account); err != nil {
		t.Fatal(err)
	}
	if err := r.Remove(account.TenantID, account.AccountID); err != nil {
		t.Fatalf("remove registered account: %v", err)
	}
	if _, err := r.Resolve(account.TenantID, account.AccountID); !errors.Is(err, ErrAccountMissing) {
		t.Fatalf("resolve after remove = %v, want ErrAccountMissing", err)
	}
	if err := r.Remove(account.TenantID, account.AccountID); !errors.Is(err, ErrAccountMissing) {
		t.Fatalf("remove missing account = %v, want ErrAccountMissing", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close registry: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("close registry twice: %v", err)
	}
	if _, err := r.Resolve(account.TenantID, account.AccountID); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("resolve after close = %v, want ErrRegistryClosed", err)
	}
	if err := r.Remove(account.TenantID, account.AccountID); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("remove after close = %v, want ErrRegistryClosed", err)
	}
	if err := r.Register(account); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("register after close = %v, want ErrRegistryClosed", err)
	}
}

func TestRegistryNilReceiverLifecycle(t *testing.T) {
	var registry *Registry
	if err := registry.Remove("tenant", "account"); !errors.Is(err, ErrAccountMissing) {
		t.Fatalf("nil remove = %v, want ErrAccountMissing", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("nil close = %v", err)
	}
}
