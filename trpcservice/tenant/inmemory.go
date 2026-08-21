package tenant

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

// InMemoryRepository is a single-process repository for development and
// tests. It does not provide cross-node sharing or durability.
type InMemoryRepository struct {
	mu    sync.RWMutex
	byID  map[string]*Tenant
	byKey map[string]string
}

// NewInMemoryRepository creates an empty repository.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{byID: make(map[string]*Tenant), byKey: make(map[string]string)}
}

func (r *InMemoryRepository) Create(ctx context.Context, input CreateInput) (*Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, err := NewTenant(input)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if _, exists := r.byID[t.TenantID]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateKey, t.TenantID)
	}
	if _, exists := r.byKey[t.TenantKey]; exists {
		return nil, fmt.Errorf("%w: %s", ErrDuplicateKey, t.TenantKey)
	}
	r.byID[t.TenantID] = cloneTenant(t)
	r.byKey[t.TenantKey] = t.TenantID
	return cloneTenant(t), nil
}

func (r *InMemoryRepository) Get(ctx context.Context, tenantID string) (*Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, ok := r.byID[tenantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, tenantID)
	}
	return cloneTenant(t), nil
}

func (r *InMemoryRepository) UpdateConfiguration(ctx context.Context, input UpdateConfigurationInput) (*Tenant, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if input.AuditRetentionDays == 0 {
		input.AuditRetentionDays = 90
	}
	if input.LogMaskingLevel == "" {
		input.LogMaskingLevel = MaskingBasic
	}
	if err := validateConfiguration(input.DisplayName, input.RateLimitRPM, input.MaxConcurrentExecutions, input.MonthlyTokenBudget, input.MonthlySpendLimitMinor, input.BillingCurrency, input.AuditRetentionDays, input.LogMaskingLevel, input.TraceSamplingRate); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	t, ok := r.byID[input.TenantID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, input.TenantID)
	}
	if t.Status == StatusDisabled {
		return nil, ErrDisabled
	}
	if input.ExpectedVersion != t.Version {
		return nil, fmt.Errorf("%w: expected %d, got %d", ErrConflict, input.ExpectedVersion, t.Version)
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
	r.byID[input.TenantID] = cloneTenant(&updated)
	return cloneTenant(&updated), nil
}

func (r *InMemoryRepository) TransitionStatus(ctx context.Context, input TransitionStatusInput) (*Tenant, StatusChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, StatusChangeEvent{}, err
	}
	if err := validateMetadata(input.Metadata); err != nil {
		return nil, StatusChangeEvent{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := checkContext(ctx); err != nil {
		return nil, StatusChangeEvent{}, err
	}
	t, ok := r.byID[input.TenantID]
	if !ok {
		return nil, StatusChangeEvent{}, fmt.Errorf("%w: %s", ErrNotFound, input.TenantID)
	}
	if t.Status == StatusDisabled {
		return nil, StatusChangeEvent{}, ErrDisabled
	}
	if input.ExpectedVersion != t.Version {
		return nil, StatusChangeEvent{}, fmt.Errorf("%w: expected %d, got %d", ErrConflict, input.ExpectedVersion, t.Version)
	}
	if !validTransition(t.Status, input.NextStatus) {
		return nil, StatusChangeEvent{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.Status, input.NextStatus)
	}
	now := time.Now().UTC()
	event := StatusChangeEvent{TenantID: t.TenantID, PreviousStatus: t.Status, NextStatus: input.NextStatus, ActorType: strings.TrimSpace(input.Metadata.ActorType), ActorID: strings.TrimSpace(input.Metadata.ActorID), Reason: strings.TrimSpace(input.Metadata.Reason), CorrelationID: strings.TrimSpace(input.Metadata.CorrelationID), PreviousVersion: t.Version, NextVersion: t.Version + 1, OccurredAt: now}
	updated := *t
	updated.Status = input.NextStatus
	updated.Version++
	updated.UpdatedAt = now
	r.byID[input.TenantID] = cloneTenant(&updated)
	return cloneTenant(&updated), event, nil
}

func validateMetadata(m TransitionMetadata) error {
	if strings.TrimSpace(m.ActorType) == "" || strings.TrimSpace(m.ActorID) == "" || strings.TrimSpace(m.Reason) == "" || strings.TrimSpace(m.CorrelationID) == "" {
		return fmt.Errorf("%w: transition metadata requires actor, reason, and correlation ID", ErrInvalid)
	}
	return nil
}

func validTransition(from, to Status) bool {
	switch {
	case from == StatusActive && (to == StatusSuspended || to == StatusDisabled):
		return true
	case from == StatusSuspended && (to == StatusActive || to == StatusDisabled):
		return true
	default:
		return false
	}
}

func cloneTenant(t *Tenant) *Tenant {
	if t == nil {
		return nil
	}
	c := *t
	c.RateLimitRPM = cloneInt64(t.RateLimitRPM)
	c.MaxConcurrentExecutions = cloneInt64(t.MaxConcurrentExecutions)
	c.MonthlyTokenBudget = cloneInt64(t.MonthlyTokenBudget)
	c.MonthlySpendLimitMinor = cloneInt64(t.MonthlySpendLimitMinor)
	c.DefaultAgentAppID = cloneString(t.DefaultAgentAppID)
	c.DefaultBackendProfileID = cloneString(t.DefaultBackendProfileID)
	return &c
}
