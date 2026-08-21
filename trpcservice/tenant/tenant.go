// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// Status is the lifecycle state of a tenant.
type Status string

const (
	StatusActive    Status = "active"
	StatusSuspended Status = "suspended"
	StatusDisabled  Status = "disabled"
)

// LogMaskingLevel controls the default masking policy for tenant telemetry.
type LogMaskingLevel string

const (
	MaskingNone   LogMaskingLevel = "none"
	MaskingBasic  LogMaskingLevel = "basic"
	MaskingStrict LogMaskingLevel = "strict"
)

var (
	ErrInvalid           = errors.New("invalid tenant")
	ErrNotFound          = errors.New("tenant not found")
	ErrConflict          = errors.New("tenant version conflict")
	ErrDuplicateKey      = errors.New("tenant key already exists")
	ErrDisabled          = errors.New("tenant is disabled")
	ErrInvalidTransition = errors.New("invalid tenant status transition")
)

// Tenant is the narrow root entity. Related application, channel and backend
// configuration stays outside this package and is referenced by ID only.
type Tenant struct {
	TenantID    string
	TenantKey   string
	DisplayName string
	Status      Status

	RateLimitRPM            *int64
	MaxConcurrentExecutions *int64
	MonthlyTokenBudget      *int64
	MonthlySpendLimitMinor  *int64
	BillingCurrency         string

	AuditRetentionDays int
	LogMaskingLevel    LogMaskingLevel
	TraceSamplingRate  float64

	DefaultAgentAppID       *string
	DefaultBackendProfileID *string

	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Clone returns a defensive copy suitable for repository boundaries.
func (t Tenant) Clone() Tenant {
	c := t
	c.RateLimitRPM = cloneInt64(t.RateLimitRPM)
	c.MaxConcurrentExecutions = cloneInt64(t.MaxConcurrentExecutions)
	c.MonthlyTokenBudget = cloneInt64(t.MonthlyTokenBudget)
	c.MonthlySpendLimitMinor = cloneInt64(t.MonthlySpendLimitMinor)
	c.DefaultAgentAppID = cloneString(t.DefaultAgentAppID)
	c.DefaultBackendProfileID = cloneString(t.DefaultBackendProfileID)
	return c
}

// Validate checks all tenant root invariants. It is useful at runtime when a
// snapshot came from an external adapter rather than NewTenant.
func (t Tenant) Validate() error {
	if err := validateTenantID(t.TenantID); err != nil {
		return err
	}
	key, err := normalizeTenantKey(t.TenantKey)
	if err != nil {
		return err
	}
	if key != t.TenantKey {
		return fmt.Errorf("%w: tenant key must be normalized", ErrInvalid)
	}
	if t.Status != StatusActive && t.Status != StatusSuspended && t.Status != StatusDisabled {
		return fmt.Errorf("%w: unknown status %q", ErrInvalid, t.Status)
	}
	if err := validateConfiguration(t.DisplayName, t.RateLimitRPM, t.MaxConcurrentExecutions, t.MonthlyTokenBudget, t.MonthlySpendLimitMinor, t.BillingCurrency, t.AuditRetentionDays, t.LogMaskingLevel, t.TraceSamplingRate); err != nil {
		return err
	}
	if t.Version < 1 || t.CreatedAt.IsZero() || t.UpdatedAt.IsZero() {
		return fmt.Errorf("%w: version and timestamps must be initialized", ErrInvalid)
	}
	return nil
}

// CreateInput contains the full initial tenant snapshot. An empty TenantID
// generates a time-ordered, non-enumerable t_ + Crockford ULID-like ID.
type CreateInput struct {
	TenantID    string
	TenantKey   string
	DisplayName string
	Status      Status

	RateLimitRPM            *int64
	MaxConcurrentExecutions *int64
	MonthlyTokenBudget      *int64
	MonthlySpendLimitMinor  *int64
	BillingCurrency         string

	AuditRetentionDays int
	LogMaskingLevel    LogMaskingLevel
	TraceSamplingRate  float64

	DefaultAgentAppID       *string
	DefaultBackendProfileID *string
}

// UpdateConfigurationInput is a complete configuration snapshot update.
type UpdateConfigurationInput struct {
	TenantID        string
	ExpectedVersion int64
	DisplayName     string

	RateLimitRPM            *int64
	MaxConcurrentExecutions *int64
	MonthlyTokenBudget      *int64
	MonthlySpendLimitMinor  *int64
	BillingCurrency         string

	AuditRetentionDays int
	LogMaskingLevel    LogMaskingLevel
	TraceSamplingRate  float64

	DefaultAgentAppID       *string
	DefaultBackendProfileID *string
}

// TransitionMetadata is the audit context required for a lifecycle change.
type TransitionMetadata struct {
	ActorType     string
	ActorID       string
	Reason        string
	CorrelationID string
}

// StatusChangeEvent is returned with every successful lifecycle transition.
// A later audit/outbox adapter can persist this event without changing domain rules.
type StatusChangeEvent struct {
	TenantID        string
	PreviousStatus  Status
	NextStatus      Status
	ActorType       string
	ActorID         string
	Reason          string
	CorrelationID   string
	PreviousVersion int64
	NextVersion     int64
	OccurredAt      time.Time
}

// TransitionStatusInput requests an optimistic-lock protected state change.
type TransitionStatusInput struct {
	TenantID        string
	ExpectedVersion int64
	NextStatus      Status
	Metadata        TransitionMetadata
}

// NewTenant validates and constructs a tenant snapshot.
func NewTenant(input CreateInput) (*Tenant, error) {
	key, err := normalizeTenantKey(input.TenantKey)
	if err != nil {
		return nil, err
	}
	id := input.TenantID
	if id == "" {
		id, err = newTenantID()
		if err != nil {
			return nil, fmt.Errorf("generate tenant id: %w", err)
		}
	} else if err := validateTenantID(id); err != nil {
		return nil, err
	}
	status := input.Status
	if status == "" {
		status = StatusActive
	}
	if status != StatusActive && status != StatusSuspended && status != StatusDisabled {
		return nil, fmt.Errorf("%w: unknown status %q", ErrInvalid, status)
	}
	if status == StatusDisabled {
		return nil, fmt.Errorf("%w: new tenant cannot be disabled", ErrInvalid)
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
	now := time.Now().UTC()
	tenant := &Tenant{
		TenantID: id, TenantKey: key, DisplayName: strings.TrimSpace(input.DisplayName), Status: status,
		RateLimitRPM: cloneInt64(input.RateLimitRPM), MaxConcurrentExecutions: cloneInt64(input.MaxConcurrentExecutions), MonthlyTokenBudget: cloneInt64(input.MonthlyTokenBudget), MonthlySpendLimitMinor: cloneInt64(input.MonthlySpendLimitMinor), BillingCurrency: input.BillingCurrency,
		AuditRetentionDays: input.AuditRetentionDays, LogMaskingLevel: input.LogMaskingLevel, TraceSamplingRate: input.TraceSamplingRate,
		DefaultAgentAppID: cloneString(input.DefaultAgentAppID), DefaultBackendProfileID: cloneString(input.DefaultBackendProfileID), Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	return tenant, nil
}

// CanAcceptExecution applies the runtime gate described in issue #4.
func (t Tenant) CanAcceptExecution() bool { return t.Status == StatusActive }

func validateConfiguration(displayName string, rate, concurrent, tokens, spend *int64, currency string, retention int, masking LogMaskingLevel, sampling float64) error {
	if n := len([]rune(strings.TrimSpace(displayName))); n < 1 || n > 200 {
		return fmt.Errorf("%w: display name must contain 1-200 characters", ErrInvalid)
	}
	if rate != nil && *rate < 0 {
		return fmt.Errorf("%w: rate limit cannot be negative", ErrInvalid)
	}
	if concurrent != nil && *concurrent <= 0 {
		return fmt.Errorf("%w: max concurrency must be positive", ErrInvalid)
	}
	if tokens != nil && *tokens < 0 {
		return fmt.Errorf("%w: token budget cannot be negative", ErrInvalid)
	}
	if spend != nil && *spend < 0 {
		return fmt.Errorf("%w: spend limit cannot be negative", ErrInvalid)
	}
	if spend != nil && !validCurrency(currency) {
		return fmt.Errorf("%w: spend limit requires an uppercase ISO-4217 currency", ErrInvalid)
	}
	if currency != "" && !validCurrency(currency) {
		return fmt.Errorf("%w: billing currency must be three uppercase letters", ErrInvalid)
	}
	if retention <= 0 {
		return fmt.Errorf("%w: audit retention must be positive", ErrInvalid)
	}
	if masking != MaskingNone && masking != MaskingBasic && masking != MaskingStrict {
		return fmt.Errorf("%w: unknown masking level", ErrInvalid)
	}
	if sampling < 0 || sampling > 1 {
		return fmt.Errorf("%w: trace sampling must be between 0 and 1", ErrInvalid)
	}
	return nil
}

// ValidateConfiguration validates a complete configuration snapshot.
func ValidateConfiguration(displayName string, rate, concurrent, tokens, spend *int64, currency string, retention int, masking LogMaskingLevel, sampling float64) error {
	return validateConfiguration(displayName, rate, concurrent, tokens, spend, currency, retention, masking, sampling)
}

func normalizeTenantKey(key string) (string, error) {
	key = strings.ToLower(strings.TrimSpace(key))
	if len(key) < 2 || len(key) > 64 || key[0] < 'a' || key[0] > 'z' {
		return "", fmt.Errorf("%w: tenant key must match [a-z][a-z0-9-]{1,63}", ErrInvalid)
	}
	for i := 1; i < len(key); i++ {
		c := key[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			return "", fmt.Errorf("%w: tenant key must match [a-z][a-z0-9-]{1,63}", ErrInvalid)
		}
	}
	return key, nil
}

func validateTenantID(id string) error {
	if len(id) != 28 || !strings.HasPrefix(id, "t_") {
		return fmt.Errorf("%w: tenant id must be t_ followed by 26 Crockford characters", ErrInvalid)
	}
	if id[2] > '7' {
		return fmt.Errorf("%w: tenant id has invalid ULID padding bits", ErrInvalid)
	}
	for _, c := range id[2:] {
		if !strings.ContainsRune("0123456789ABCDEFGHJKMNPQRSTVWXYZ", c) {
			return fmt.Errorf("%w: invalid tenant id", ErrInvalid)
		}
	}
	return nil
}

func validCurrency(currency string) bool {
	if len(currency) != 3 {
		return false
	}
	for _, c := range currency {
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return true
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

const crockford = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

func newTenantID() (string, error) {
	var data [16]byte
	ms := time.Now().UnixMilli()
	for i := 5; i >= 0; i-- {
		data[i] = byte(ms)
		ms >>= 8
	}
	if _, err := rand.Read(data[6:]); err != nil {
		return "", err
	}
	value := new(big.Int).SetBytes(data[:])
	var encoded [26]byte
	for i := len(encoded) - 1; i >= 0; i-- {
		encoded[i] = crockford[value.Uint64()&31]
		value.Rsh(value, 5)
	}
	return "t_" + string(encoded[:]), nil
}
