// Package inmemory provides the single-process tenant repository.
package inmemory

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// InMemoryRepository is a single-process repository for development and
// tests. It does not provide cross-node sharing or durability.
type InMemoryRepository struct {
	mu    contextRWMutex
	byID  map[string]*tenant.Tenant
	byKey map[string]string
}

// NewInMemoryRepository creates an empty repository.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{byID: make(map[string]*tenant.Tenant), byKey: make(map[string]string)}
}

// NewRepository is the concise constructor for the InMemory implementation.
func NewRepository() *InMemoryRepository { return NewInMemoryRepository() }

var _ tenant.Repository = (*InMemoryRepository)(nil)

func (r *InMemoryRepository) Create(ctx context.Context, input tenant.CreateInput) (*tenant.Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, err := tenant.NewTenant(input)
	if err != nil {
		return nil, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if _, exists := r.byID[t.TenantID]; exists {
		return nil, fmt.Errorf("%w: %s", tenant.ErrDuplicateKey, t.TenantID)
	}
	if _, exists := r.byKey[t.TenantKey]; exists {
		return nil, fmt.Errorf("%w: %s", tenant.ErrDuplicateKey, t.TenantKey)
	}
	copy := t.Clone()
	r.byID[t.TenantID] = &copy
	r.byKey[t.TenantKey] = t.TenantID
	return cloneTenant(t), nil
}

func (r *InMemoryRepository) Get(ctx context.Context, tenantID string) (*tenant.Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.rLock(ctx); err != nil {
		return nil, err
	}
	defer r.rUnlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, ok := r.byID[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", tenant.ErrNotFound, tenantID)
	}
	return cloneTenant(t), nil
}

func (r *InMemoryRepository) UpdateConfiguration(ctx context.Context, input tenant.UpdateConfigurationInput) (*tenant.Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if input.AuditRetentionDays == 0 {
		input.AuditRetentionDays = 90
	}
	if input.LogMaskingLevel == "" {
		input.LogMaskingLevel = tenant.MaskingBasic
	}
	if err := tenant.ValidateConfiguration(input.DisplayName, input.RateLimitRPM, input.MaxConcurrentExecutions, input.MonthlyTokenBudget, input.MonthlySpendLimitMinor, input.BillingCurrency, input.AuditRetentionDays, input.LogMaskingLevel, input.TraceSamplingRate); err != nil {
		return nil, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, ok := r.byID[input.TenantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", tenant.ErrNotFound, input.TenantID)
	}
	if t.Status == tenant.StatusDisabled {
		return nil, tenant.ErrDisabled
	}
	if input.ExpectedVersion != t.Version {
		return nil, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, input.ExpectedVersion, t.Version)
	}
	updated := *t
	updated.DisplayName = strings.TrimSpace(input.DisplayName)
	updated.RateLimitRPM = cloneInt64(input.RateLimitRPM)
	updated.MaxConcurrentExecutions = cloneInt64(input.MaxConcurrentExecutions)
	updated.MonthlyTokenBudget = cloneInt64(input.MonthlyTokenBudget)
	updated.MonthlySpendLimitMinor = cloneInt64(input.MonthlySpendLimitMinor)
	updated.BillingCurrency = input.BillingCurrency
	updated.AuditRetentionDays = input.AuditRetentionDays
	updated.LogMaskingLevel = input.LogMaskingLevel
	updated.TraceSamplingRate = input.TraceSamplingRate
	updated.DefaultAgentAppID = cloneString(input.DefaultAgentAppID)
	updated.DefaultBackendProfileID = cloneString(input.DefaultBackendProfileID)
	updated.Version++
	updated.UpdatedAt = time.Now().UTC()
	r.byID[input.TenantID] = &updated
	return cloneTenant(&updated), nil
}

func (r *InMemoryRepository) TransitionStatus(ctx context.Context, input tenant.TransitionStatusInput) (*tenant.Tenant, tenant.StatusChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	if err := validateMetadata(input.Metadata); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	if err := r.lock(ctx); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	defer r.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, tenant.StatusChangeEvent{}, err
	}
	t, ok := r.byID[input.TenantID]
	if !ok {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: %s", tenant.ErrNotFound, input.TenantID)
	}
	if t.Status == tenant.StatusDisabled {
		return nil, tenant.StatusChangeEvent{}, tenant.ErrDisabled
	}
	if input.ExpectedVersion != t.Version {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", tenant.ErrConflict, input.ExpectedVersion, t.Version)
	}
	if !validTransition(t.Status, input.NextStatus) {
		return nil, tenant.StatusChangeEvent{}, fmt.Errorf("%w: %s -> %s", tenant.ErrInvalidTransition, t.Status, input.NextStatus)
	}
	now := time.Now().UTC()
	event := tenant.StatusChangeEvent{TenantID: t.TenantID, PreviousStatus: t.Status, NextStatus: input.NextStatus, ActorType: strings.TrimSpace(input.Metadata.ActorType), ActorID: strings.TrimSpace(input.Metadata.ActorID), Reason: strings.TrimSpace(input.Metadata.Reason), CorrelationID: strings.TrimSpace(input.Metadata.CorrelationID), PreviousVersion: t.Version, NextVersion: t.Version + 1, OccurredAt: now}
	updated := *t
	updated.Status = input.NextStatus
	updated.Version++
	updated.UpdatedAt = now
	r.byID[input.TenantID] = &updated
	return cloneTenant(&updated), event, nil
}

func validateMetadata(m tenant.TransitionMetadata) error {
	reason := strings.TrimSpace(m.Reason)
	if strings.TrimSpace(m.ActorType) == "" || strings.TrimSpace(m.ActorID) == "" || reason == "" || strings.TrimSpace(m.CorrelationID) == "" {
		return fmt.Errorf("%w: transition metadata requires actor, reason, and correlation ID", tenant.ErrInvalid)
	}
	if len([]rune(reason)) > 1000 {
		return fmt.Errorf("%w: transition reason must contain at most 1000 characters", tenant.ErrInvalid)
	}
	return nil
}

func validTransition(from, to tenant.Status) bool {
	switch {
	case from == tenant.StatusActive && (to == tenant.StatusSuspended || to == tenant.StatusDisabled):
		return true
	case from == tenant.StatusSuspended && (to == tenant.StatusActive || to == tenant.StatusDisabled):
		return true
	default:
		return false
	}
}

func cloneTenant(t *tenant.Tenant) *tenant.Tenant {
	if t == nil {
		return nil
	}
	c := t.Clone()
	return &c
}

func cloneInt64(v *int64) *int64 {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func cloneString(v *string) *string {
	if v == nil {
		return nil
	}
	c := *v
	return &c
}

func checkContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (r *InMemoryRepository) lock(ctx context.Context) error {
	return r.mu.lock(ctx)
}

func (r *InMemoryRepository) unlock() { r.mu.unlock() }

func (r *InMemoryRepository) rLock(ctx context.Context) error {
	return r.mu.rlock(ctx)
}

func (r *InMemoryRepository) rUnlock() { r.mu.runlock() }
