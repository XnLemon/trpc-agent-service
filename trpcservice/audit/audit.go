// Package audit defines tenant-scoped business audit and usage contracts.
// It deliberately has no dependency on a telemetry or database provider.
package audit

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

const SchemaVersion = 1

var (
	ErrInvalid     = errors.New("invalid audit event")
	ErrTenantScope = errors.New("audit tenant scope violation")
	ErrNotFound    = errors.New("audit event not found")
	ErrConflict    = errors.New("audit event conflict")
)

type EventType string

const (
	EventControlPlaneChanged      EventType = "control_plane.changed"
	EventExecutionStarted         EventType = "execution.started"
	EventExecutionCompleted       EventType = "execution.completed"
	EventExecutionFailed          EventType = "execution.failed"
	EventExecutionCanceled        EventType = "execution.canceled"
	EventExecutionTimedOut        EventType = "execution.timed_out"
	EventExecutionFallback        EventType = "execution.fallback"
	EventToolAllowed              EventType = "tool.allowed"
	EventToolDenied               EventType = "tool.denied"
	EventToolApprovalRequired     EventType = "tool.approval_required"
	EventIMAuthorizationAllowed   EventType = "im.authorization_allowed"
	EventIMAuthorizationDenied    EventType = "im.authorization_denied"
	EventIMIngressAccepted        EventType = "im.ingress_accepted"
	EventIMIngressDuplicate       EventType = "im.ingress_duplicate"
	EventIMDeliverySent           EventType = "im.delivery_sent"
	EventIMDeliveryRetryScheduled EventType = "im.delivery_retry_scheduled"
	EventIMDeliveryDeadLettered   EventType = "im.delivery_dead_lettered"
	EventIMDeliveryReconciled     EventType = "im.delivery_reconciled"
	EventBudgetRejected           EventType = "budget.rejected"
	EventContentRedacted          EventType = "content.redacted"
	EventAuditIncomplete          EventType = "audit_incomplete"
)

type Decision string

const (
	DecisionAllow            Decision = "allow"
	DecisionDeny             Decision = "deny"
	DecisionApprovalRequired Decision = "approval_required"
	DecisionAccepted         Decision = "accepted"
	DecisionDuplicate        Decision = "duplicate"
	DecisionRejected         Decision = "rejected"
)

type ExecutionResult string

const (
	ResultSuccess  ExecutionResult = "success"
	ResultFailure  ExecutionResult = "failure"
	ResultCanceled ExecutionResult = "canceled"
	ResultTimeout  ExecutionResult = "timeout"
	ResultRejected ExecutionResult = "rejected"
)

type Usage struct {
	InputTokens      *int64
	OutputTokens     *int64
	ModelCostMinor   *int64
	ToolCostMinor    *int64
	Currency         string
	BudgetUsedTokens *int64
	BudgetUsedMinor  *int64
	ExecutionResult  ExecutionResult
	Provider         string
	Model            string
}

func (u *Usage) Clone() *Usage {
	if u == nil {
		return nil
	}
	copy := *u
	copy.InputTokens = cloneInt64(u.InputTokens)
	copy.OutputTokens = cloneInt64(u.OutputTokens)
	copy.ModelCostMinor = cloneInt64(u.ModelCostMinor)
	copy.ToolCostMinor = cloneInt64(u.ToolCostMinor)
	copy.BudgetUsedTokens = cloneInt64(u.BudgetUsedTokens)
	copy.BudgetUsedMinor = cloneInt64(u.BudgetUsedMinor)
	return &copy
}

type Event struct {
	SchemaVersion   int
	EventID         string
	EventType       EventType
	TenantID        string
	Channel         string
	UserID          string
	SessionID       string
	AgentAppID      string
	Revision        *int64
	ModelProfileID  string
	ToolName        string
	Decision        Decision
	LatencyMS       *int64
	ErrorType       string
	Cost            *Usage
	RequestID       string
	TraceID         string
	CorrelationID   string
	ActorType       string
	ActorID         string
	Reason          string
	PreviousVersion *int64
	NextVersion     *int64
	OccurredAt      time.Time
}

func (e Event) Clone() Event {
	e.Cost = e.Cost.Clone()
	e.Revision = cloneInt64(e.Revision)
	e.LatencyMS = cloneInt64(e.LatencyMS)
	e.PreviousVersion = cloneInt64(e.PreviousVersion)
	e.NextVersion = cloneInt64(e.NextVersion)
	return e
}

func (e Event) Validate() error {
	if e.SchemaVersion != SchemaVersion || clean(e.EventID) == "" || clean(e.TenantID) == "" || !validEventType(e.EventType) || e.OccurredAt.IsZero() || e.OccurredAt.Location() != time.UTC {
		return ErrInvalid
	}
	for _, value := range []string{e.EventID, e.TenantID, e.Channel, e.UserID, e.SessionID, e.AgentAppID, e.ModelProfileID, e.ToolName, e.ErrorType, e.RequestID, e.TraceID, e.CorrelationID, e.ActorType, e.ActorID, e.Reason} {
		if hasControl(value) {
			return ErrInvalid
		}
	}
	if e.LatencyMS != nil && *e.LatencyMS < 0 || e.Revision != nil && *e.Revision < 0 {
		return ErrInvalid
	}
	if (e.PreviousVersion == nil) != (e.NextVersion == nil) {
		return ErrInvalid
	}
	if e.PreviousVersion != nil && (*e.PreviousVersion < 0 || *e.NextVersion != *e.PreviousVersion+1) {
		return ErrInvalid
	}
	if e.EventType == EventControlPlaneChanged && (clean(e.ActorType) == "" || clean(e.ActorID) == "" || clean(e.Reason) == "" || clean(e.CorrelationID) == "" || e.PreviousVersion == nil) {
		return ErrInvalid
	}
	if len([]rune(strings.TrimSpace(e.Reason))) > 1000 || e.Decision != "" && !validDecision(e.Decision) || e.Cost != nil && e.Cost.Validate() != nil {
		return ErrInvalid
	}
	return nil
}

func (u Usage) Validate() error {
	for _, value := range []*int64{u.InputTokens, u.OutputTokens, u.ModelCostMinor, u.ToolCostMinor, u.BudgetUsedTokens, u.BudgetUsedMinor} {
		if value != nil && *value < 0 {
			return ErrInvalid
		}
	}
	if (u.ModelCostMinor != nil || u.ToolCostMinor != nil) && !validCurrency(u.Currency) || u.ExecutionResult != "" && !validResult(u.ExecutionResult) {
		return ErrInvalid
	}
	for _, value := range []string{u.Currency, u.Provider, u.Model} {
		if hasControl(value) || strings.Contains(value, "://") {
			return ErrInvalid
		}
	}
	return nil
}

func (e Event) Digest() (string, error) {
	if err := e.Validate(); err != nil {
		return "", err
	}
	b, err := json.Marshal(e.Clone())
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

type AppendResult struct {
	Event     Event
	Duplicate bool
	Digest    string
}
type Query struct {
	EventTypes []EventType
	Since      time.Time
	Until      time.Time
}
type GroupBy string

const (
	GroupTenant   GroupBy = "tenant"
	GroupApp      GroupBy = "app"
	GroupChannel  GroupBy = "channel"
	GroupProvider GroupBy = "provider"
	GroupModel    GroupBy = "model"
)

type UsageQuery struct {
	Query   Query
	GroupBy []GroupBy
}
type UsageTotal struct {
	TenantID         string
	AppID            string
	Channel          string
	Provider         string
	Model            string
	InputTokens      int64
	OutputTokens     int64
	ModelCostMinor   int64
	ToolCostMinor    int64
	BudgetUsedTokens int64
	BudgetUsedMinor  int64
	EventCount       int64
	Currency         string
}

type Writer interface {
	Append(context.Context, Event) (AppendResult, error)
}
type Reader interface {
	Get(context.Context, string) (Event, error)
	List(context.Context, Query) ([]Event, error)
}
type Aggregator interface {
	AggregateUsage(context.Context, UsageQuery) ([]UsageTotal, error)
}

type Backend struct {
	mu     sync.RWMutex
	events map[string]record
}
type record struct {
	event  Event
	digest string
}
type Store struct {
	tenantID string
	backend  *Backend
}

func NewInMemory(tenantID string) (*Store, error) {
	return NewInMemoryWithBackend(tenantID, &Backend{})
}
func NewInMemoryWithBackend(tenantID string, backend *Backend) (*Store, error) {
	tenantID = clean(tenantID)
	if tenantID == "" || hasControl(tenantID) || backend == nil {
		return nil, ErrInvalid
	}
	backend.mu.Lock()
	if backend.events == nil {
		backend.events = map[string]record{}
	}
	backend.mu.Unlock()
	return &Store{tenantID: tenantID, backend: backend}, nil
}
func (s *Store) scope(tenantID string) error {
	if s == nil || s.backend == nil || s.tenantID == "" || tenantID != s.tenantID {
		return ErrTenantScope
	}
	return nil
}
func check(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalid
	}
	return ctx.Err()
}

func (s *Store) Append(ctx context.Context, event Event) (AppendResult, error) {
	if err := check(ctx); err != nil {
		return AppendResult{}, err
	}
	if err := s.scope(event.TenantID); err != nil {
		return AppendResult{}, err
	}
	digest, err := event.Digest()
	if err != nil {
		return AppendResult{}, err
	}
	event = event.Clone()
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	key := eventKey(event.TenantID, event.EventID)
	if existing, ok := s.backend.events[key]; ok {
		if existing.digest != digest {
			return AppendResult{}, ErrConflict
		}
		return AppendResult{Event: existing.event.Clone(), Duplicate: true, Digest: digest}, nil
	}
	s.backend.events[key] = record{event: event, digest: digest}
	return AppendResult{Event: event.Clone(), Digest: digest}, nil
}

func (s *Store) Get(ctx context.Context, eventID string) (Event, error) {
	if err := check(ctx); err != nil {
		return Event{}, err
	}
	if err := s.scope(s.tenantID); err != nil || clean(eventID) == "" {
		return Event{}, ErrTenantScope
	}
	s.backend.mu.RLock()
	defer s.backend.mu.RUnlock()
	value, ok := s.backend.events[eventKey(s.tenantID, clean(eventID))]
	if !ok {
		return Event{}, ErrNotFound
	}
	return value.event.Clone(), nil
}

func (s *Store) List(ctx context.Context, query Query) ([]Event, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	if err := s.scope(s.tenantID); err != nil {
		return nil, err
	}
	types := map[EventType]struct{}{}
	for _, typ := range query.EventTypes {
		if !validEventType(typ) {
			return nil, ErrInvalid
		}
		types[typ] = struct{}{}
	}
	s.backend.mu.RLock()
	values := make([]Event, 0)
	for _, item := range s.backend.events {
		event := item.event
		if event.TenantID != s.tenantID {
			continue
		}
		if len(types) > 0 {
			if _, ok := types[event.EventType]; !ok {
				continue
			}
		}
		if !query.Since.IsZero() && event.OccurredAt.Before(query.Since) {
			continue
		}
		if !query.Until.IsZero() && event.OccurredAt.After(query.Until) {
			continue
		}
		values = append(values, event.Clone())
	}
	s.backend.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool {
		if values[i].OccurredAt.Equal(values[j].OccurredAt) {
			return values[i].EventID < values[j].EventID
		}
		return values[i].OccurredAt.Before(values[j].OccurredAt)
	})
	return values, nil
}

func (s *Store) AggregateUsage(ctx context.Context, query UsageQuery) ([]UsageTotal, error) {
	if err := check(ctx); err != nil {
		return nil, err
	}
	for _, group := range query.GroupBy {
		if !validGroup(group) {
			return nil, ErrInvalid
		}
	}
	events, err := s.List(ctx, query.Query)
	if err != nil {
		return nil, err
	}
	groups := map[string]*UsageTotal{}
	for _, event := range events {
		if event.Cost == nil {
			continue
		}
		key := aggregateKey(event, query.GroupBy)
		total := groups[key]
		if total == nil {
			total = &UsageTotal{TenantID: s.tenantID, AppID: event.AgentAppID, Channel: event.Channel, Provider: event.Cost.Provider, Model: event.Cost.Model, Currency: event.Cost.Currency}
			groups[key] = total
		}
		addUsage(total, event.Cost)
	}
	result := make([]UsageTotal, 0, len(groups))
	for _, total := range groups {
		result = append(result, *total)
	}
	sort.Slice(result, func(i, j int) bool {
		return aggregateKeyTotal(result[i], query.GroupBy) < aggregateKeyTotal(result[j], query.GroupBy)
	})
	return result, nil
}
func addUsage(total *UsageTotal, usage *Usage) {
	total.EventCount++
	if usage.InputTokens != nil {
		total.InputTokens += *usage.InputTokens
	}
	if usage.OutputTokens != nil {
		total.OutputTokens += *usage.OutputTokens
	}
	if usage.ModelCostMinor != nil {
		total.ModelCostMinor += *usage.ModelCostMinor
	}
	if usage.ToolCostMinor != nil {
		total.ToolCostMinor += *usage.ToolCostMinor
	}
	if usage.BudgetUsedTokens != nil {
		total.BudgetUsedTokens += *usage.BudgetUsedTokens
	}
	if usage.BudgetUsedMinor != nil {
		total.BudgetUsedMinor += *usage.BudgetUsedMinor
	}
}
func aggregateKey(event Event, groups []GroupBy) string {
	parts := make([]string, 0, len(groups))
	for _, group := range groups {
		switch group {
		case GroupTenant:
			parts = append(parts, event.TenantID)
		case GroupApp:
			parts = append(parts, event.AgentAppID)
		case GroupChannel:
			parts = append(parts, event.Channel)
		case GroupProvider:
			parts = append(parts, event.Cost.Provider)
		case GroupModel:
			parts = append(parts, event.Cost.Model)
		}
	}
	return strings.Join(parts, "\\x00")
}
func aggregateKeyTotal(total UsageTotal, groups []GroupBy) string {
	parts := make([]string, 0, len(groups))
	for _, group := range groups {
		switch group {
		case GroupTenant:
			parts = append(parts, total.TenantID)
		case GroupApp:
			parts = append(parts, total.AppID)
		case GroupChannel:
			parts = append(parts, total.Channel)
		case GroupProvider:
			parts = append(parts, total.Provider)
		case GroupModel:
			parts = append(parts, total.Model)
		}
	}
	return strings.Join(parts, "\\x00")
}
func eventKey(tenantID, eventID string) string { return tenantID + "\\x00" + eventID }
func clean(value string) string                { return strings.TrimSpace(value) }
func hasControl(value string) bool {
	for _, r := range value {
		if unicode.IsControl(r) {
			return true
		}
	}
	return false
}
func validEventType(value EventType) bool {
	switch value {
	case EventControlPlaneChanged, EventExecutionStarted, EventExecutionCompleted, EventExecutionFailed, EventExecutionCanceled, EventExecutionTimedOut, EventExecutionFallback, EventToolAllowed, EventToolDenied, EventToolApprovalRequired, EventIMAuthorizationAllowed, EventIMAuthorizationDenied, EventIMIngressAccepted, EventIMIngressDuplicate, EventIMDeliverySent, EventIMDeliveryRetryScheduled, EventIMDeliveryDeadLettered, EventIMDeliveryReconciled, EventBudgetRejected, EventContentRedacted, EventAuditIncomplete:
		return true
	}
	return false
}
func validDecision(value Decision) bool {
	switch value {
	case DecisionAllow, DecisionDeny, DecisionApprovalRequired, DecisionAccepted, DecisionDuplicate, DecisionRejected:
		return true
	}
	return false
}
func validResult(value ExecutionResult) bool {
	switch value {
	case ResultSuccess, ResultFailure, ResultCanceled, ResultTimeout, ResultRejected:
		return true
	}
	return false
}
func validCurrency(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, r := range value {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
}

func validGroup(value GroupBy) bool {
	switch value {
	case GroupTenant, GroupApp, GroupChannel, GroupProvider, GroupModel:
		return true
	default:
		return false
	}
}
func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

var _ Writer = (*Store)(nil)
var _ Reader = (*Store)(nil)
var _ Aggregator = (*Store)(nil)
