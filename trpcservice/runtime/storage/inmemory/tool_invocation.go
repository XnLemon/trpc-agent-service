package inmemory

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

var (
	ErrToolInvocationInvalid  = errors.New("invalid tool invocation")
	ErrToolInvocationConflict = errors.New("tool invocation conflict")
)

type toolInvocationKey struct {
	tenantID     string
	appID        string
	invocationID string
}

// ToolInvocationBackend is shareable between in-memory store instances so
// restart/conformance tests can exercise the same durable contract without a
// database.
type ToolInvocationBackend struct {
	mu    sync.RWMutex
	items map[toolInvocationKey]runtimestorage.ToolInvocation
}

// ToolInvocationStore is a concurrency-safe in-memory implementation of the
// tenant/app-scoped tool invocation contract.
type ToolInvocationStore struct {
	backend *ToolInvocationBackend
}

func NewToolInvocationStore() *ToolInvocationStore {
	return NewToolInvocationStoreWithBackend(&ToolInvocationBackend{})
}

func NewToolInvocationStoreWithBackend(backend *ToolInvocationBackend) *ToolInvocationStore {
	if backend == nil {
		backend = &ToolInvocationBackend{}
	}
	backend.mu.Lock()
	if backend.items == nil {
		backend.items = make(map[toolInvocationKey]runtimestorage.ToolInvocation)
	}
	backend.mu.Unlock()
	return &ToolInvocationStore{backend: backend}
}

var _ runtimestorage.ToolInvocationStore = (*ToolInvocationStore)(nil)
var _ runtimestorage.ToolInvocationRecoveryStore = (*ToolInvocationStore)(nil)
var _ runtimestorage.ToolInvocationReconciliationStore = (*ToolInvocationStore)(nil)
var _ runtimestorage.ToolInvocationAuditStore = (*ToolInvocationStore)(nil)

func (store *ToolInvocationStore) PrepareToolInvocation(ctx context.Context, input runtimestorage.ToolInvocationInput) (runtimestorage.ToolInvocation, error) {
	if err := toolInvocationContext(ctx); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if err := runtimestorage.ValidateToolInvocationInput(input); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if store == nil || store.backend == nil {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	key := toolInvocationKey{tenantID: input.TenantID, appID: input.AppID, invocationID: input.InvocationID}
	store.backend.mu.Lock()
	defer store.backend.mu.Unlock()
	if old, ok := store.backend.items[key]; ok {
		if old.AppID != input.AppID || old.EventID != input.EventID || old.RequestID != input.RequestID || old.TraceID != input.TraceID || old.ToolCallID != input.ToolCallID || old.ToolName != input.ToolName || old.ArgsSHA256 != input.ArgsSHA256 {
			return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
		}
		if err := runtimestorage.ValidateToolInvocation(old); err != nil {
			return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
		}
		return old, nil
	}
	now := time.Now().UTC()
	value := runtimestorage.ToolInvocation{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID, EventID: input.EventID,
		RequestID: input.RequestID, TraceID: input.TraceID, ToolCallID: input.ToolCallID,
		ToolName: input.ToolName, ArgsSHA256: input.ArgsSHA256, Status: runtimestorage.ToolInvocationPrepared,
		Owner: input.Owner, FencingToken: 1, CreatedAt: now, UpdatedAt: now,
	}
	store.backend.items[key] = value
	if err := runtimestorage.ValidateToolInvocation(value); err != nil {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	return value, nil
}

func (store *ToolInvocationStore) TransitionToolInvocation(ctx context.Context, transition runtimestorage.ToolInvocationTransition) (runtimestorage.ToolInvocation, error) {
	if err := toolInvocationContext(ctx); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if store == nil || store.backend == nil {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	if err := runtimestorage.ValidateToolInvocationTransitionRequest(transition); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	key := toolInvocationKey{tenantID: transition.TenantID, appID: transition.AppID, invocationID: transition.InvocationID}
	store.backend.mu.Lock()
	defer store.backend.mu.Unlock()
	value, ok := store.backend.items[key]
	if !ok {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrNotFound
	}
	if value.Status == transition.To && value.FencingToken == transition.FencingToken+1 {
		if err := runtimestorage.ValidateToolInvocation(value); err != nil {
			return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
		}
		return value, nil
	}
	if value.Status != transition.From || value.FencingToken != transition.FencingToken {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
	}
	if transition.Owner != "" && transition.Owner != value.Owner && transition.To != runtimestorage.ToolInvocationManual && transition.From != runtimestorage.ToolInvocationManual {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrConflict
	}
	if transition.To == runtimestorage.ToolInvocationManual && strings.TrimSpace(transition.ReviewerID) == "" {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrInvalid
	}
	value.Status = transition.To
	value.FencingToken++
	value.ErrorClass = strings.TrimSpace(transition.ErrorClass)
	value.ReviewerID = strings.TrimSpace(transition.ReviewerID)
	value.UpdatedAt = time.Now().UTC()
	store.backend.items[key] = value
	if err := runtimestorage.ValidateToolInvocation(value); err != nil {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	return value, nil
}

// RecoverStaleToolInvocations fences calls that may have crossed the
// provider boundary before a process crash. It deliberately never rewinds a
// terminal state or retries a call automatically.
func (store *ToolInvocationStore) RecoverStaleToolInvocations(ctx context.Context, before time.Time) ([]runtimestorage.ToolInvocation, error) {
	if err := toolInvocationContext(ctx); err != nil {
		return nil, err
	}
	if store == nil || store.backend == nil || before.IsZero() {
		return nil, runtimestorage.ErrInvalid
	}
	before = before.UTC()
	store.backend.mu.Lock()
	defer store.backend.mu.Unlock()
	values := make([]runtimestorage.ToolInvocation, 0)
	for key, value := range store.backend.items {
		if (value.Status != runtimestorage.ToolInvocationDispatching && value.Status != runtimestorage.ToolInvocationAccepted) || value.UpdatedAt.After(before) {
			continue
		}
		value.Status = runtimestorage.ToolInvocationUnknown
		value.ErrorClass = "provider_uncertain"
		value.ReviewerID = ""
		value.FencingToken++
		value.UpdatedAt = time.Now().UTC()
		store.backend.items[key] = value
		if err := runtimestorage.ValidateToolInvocation(value); err != nil {
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	sort.Slice(values, func(i, j int) bool {
		if !values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].CreatedAt.Before(values[j].CreatedAt)
		}
		if values[i].TenantID != values[j].TenantID {
			return values[i].TenantID < values[j].TenantID
		}
		if values[i].AppID != values[j].AppID {
			return values[i].AppID < values[j].AppID
		}
		return values[i].InvocationID < values[j].InvocationID
	})
	return values, nil
}

// ListToolInvocationReconciliation returns unknown provider hand-offs that
// still need an audit/reviewer fact. It is intentionally process-wide because
// the recovery worker is process-wide; every returned row keeps its tenant
// identity for the downstream tenant-scoped audit writer.
func (store *ToolInvocationStore) ListToolInvocationReconciliation(ctx context.Context) ([]runtimestorage.ToolInvocation, error) {
	values, err := store.ListToolInvocationAuditCandidates(ctx)
	if err != nil {
		return nil, err
	}
	filtered := values[:0]
	for _, value := range values {
		if value.Status == runtimestorage.ToolInvocationUnknown && value.ErrorClass == "provider_uncertain" {
			filtered = append(filtered, value)
		}
	}
	return filtered, nil
}

// ListToolInvocationAuditCandidates exposes only states that have a stable
// platform audit fact. It is process-wide for the same reason as recovery.
func (store *ToolInvocationStore) ListToolInvocationAuditCandidates(ctx context.Context) ([]runtimestorage.ToolInvocation, error) {
	if err := toolInvocationContext(ctx); err != nil {
		return nil, err
	}
	if store == nil || store.backend == nil {
		return nil, runtimestorage.ErrStorage
	}
	store.backend.mu.RLock()
	values := make([]runtimestorage.ToolInvocation, 0)
	for _, value := range store.backend.items {
		switch value.Status {
		case runtimestorage.ToolInvocationAccepted, runtimestorage.ToolInvocationSucceeded, runtimestorage.ToolInvocationFailed, runtimestorage.ToolInvocationDenied, runtimestorage.ToolInvocationUnknown, runtimestorage.ToolInvocationManual:
			if err := runtimestorage.ValidateToolInvocation(value); err != nil {
				store.backend.mu.RUnlock()
				return nil, runtimestorage.ErrStorage
			}
			values = append(values, value)
		}
	}
	store.backend.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool {
		if values[i].TenantID != values[j].TenantID {
			return values[i].TenantID < values[j].TenantID
		}
		if values[i].AppID != values[j].AppID {
			return values[i].AppID < values[j].AppID
		}
		return values[i].InvocationID < values[j].InvocationID
	})
	return values, nil
}

func (store *ToolInvocationStore) GetToolInvocation(ctx context.Context, tenantID, appID, invocationID string) (runtimestorage.ToolInvocation, error) {
	if err := toolInvocationContext(ctx); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	if store == nil || store.backend == nil {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	if err := runtimestorage.ValidateToolInvocationLookup(tenantID, appID, invocationID); err != nil {
		return runtimestorage.ToolInvocation{}, err
	}
	store.backend.mu.RLock()
	defer store.backend.mu.RUnlock()
	value, ok := store.backend.items[toolInvocationKey{tenantID: tenantID, appID: appID, invocationID: invocationID}]
	if !ok {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrNotFound
	}
	if err := runtimestorage.ValidateToolInvocation(value); err != nil {
		return runtimestorage.ToolInvocation{}, runtimestorage.ErrStorage
	}
	return value, nil
}

func (store *ToolInvocationStore) ListToolInvocations(ctx context.Context, tenantID, appID string, statuses []runtimestorage.ToolInvocationStatus) ([]runtimestorage.ToolInvocation, error) {
	if err := toolInvocationContext(ctx); err != nil {
		return nil, err
	}
	if store == nil || store.backend == nil {
		return nil, runtimestorage.ErrStorage
	}
	if err := runtimestorage.ValidateToolInvocationTenantApp(tenantID, appID); err != nil {
		return nil, err
	}
	filter := make(map[runtimestorage.ToolInvocationStatus]struct{}, len(statuses))
	for _, status := range statuses {
		if !runtimestorage.ValidToolInvocationStatus(status) {
			return nil, runtimestorage.ErrInvalid
		}
		filter[status] = struct{}{}
	}
	store.backend.mu.RLock()
	values := make([]runtimestorage.ToolInvocation, 0)
	for key, value := range store.backend.items {
		if key.tenantID != tenantID || key.appID != appID {
			continue
		}
		if len(filter) > 0 {
			if _, ok := filter[value.Status]; !ok {
				continue
			}
		}
		if err := runtimestorage.ValidateToolInvocation(value); err != nil {
			store.backend.mu.RUnlock()
			return nil, runtimestorage.ErrStorage
		}
		values = append(values, value)
	}
	store.backend.mu.RUnlock()
	sort.Slice(values, func(i, j int) bool {
		if values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].InvocationID < values[j].InvocationID
		}
		return values[i].CreatedAt.Before(values[j].CreatedAt)
	})
	return values, nil
}

func toolInvocationContext(ctx context.Context) error {
	if nilvalue.Is(ctx) {
		return runtimestorage.ErrInvalid
	}
	return ctx.Err()
}
