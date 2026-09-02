// Package inmemory provides the single-process Agent App repository.
package inmemory

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
)

// List returns a stable page of Apps belonging to one tenant.
func (r *InMemoryRepository) List(ctx context.Context, tenantID, query, status, cursor string, limit int) ([]*agent.App, string, error) {
	if err := checkContext(ctx); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if err := r.mu.rlock(ctx); err != nil {
		return nil, "", err
	}
	defer r.mu.runlock()
	query, status = strings.ToLower(strings.TrimSpace(query)), strings.TrimSpace(status)
	items := make([]*agent.App, 0)
	for scope, value := range r.apps {
		if scope.tenantID != tenantID || (status != "" && string(value.Status) != status) {
			continue
		}
		if query != "" && !strings.Contains(strings.ToLower(value.AppID+" "+value.AppKey+" "+value.DisplayName), query) {
			continue
		}
		items = append(items, cloneApp(value))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].AppID < items[j].AppID })
	if offset >= len(items) {
		return []*agent.App{}, "", nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = encodeCursor(end)
	}
	return items[offset:end], next, nil
}

// ListRevisions returns revisions for one App using stable numeric ordering.
func (r *InMemoryRepository) ListRevisions(ctx context.Context, tenantID, appID, query, status, cursor string, limit int) ([]*agent.Revision, string, error) {
	if err := checkContext(ctx); err != nil {
		return nil, "", err
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	offset, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	if err := r.mu.rlock(ctx); err != nil {
		return nil, "", err
	}
	defer r.mu.runlock()
	values := r.revisions[appScope{tenantID: tenantID, appID: appID}]
	items := make([]*agent.Revision, 0, len(values))
	status = strings.TrimSpace(status)
	for _, value := range values {
		if status != "" && string(value.State) != status {
			continue
		}
		items = append(items, cloneRevision(value))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Revision < items[j].Revision })
	if offset >= len(items) {
		return []*agent.Revision{}, "", nil
	}
	end := offset + limit
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = encodeCursor(end)
	}
	return items[offset:end], next, nil
}

func decodeCursor(cursor string) (int, error) {
	if cursor == "" {
		return 0, nil
	}
	var offset int
	if _, err := fmt.Sscanf(cursor, "%d", &offset); err != nil || offset < 0 {
		return 0, fmt.Errorf("invalid cursor")
	}
	return offset, nil
}
func encodeCursor(offset int) string { return fmt.Sprintf("%d", offset) }

type appScope struct {
	tenantID string
	appID    string
}

type keyScope struct {
	tenantID string
	appKey   string
}

// InMemoryRepository is a thread-safe, tenant-scoped development repository.
// It does not provide durability or cross-process consistency.
type InMemoryRepository struct {
	mu        contextRWMutex
	apps      map[appScope]*agent.App
	byKey     map[keyScope]string
	revisions map[appScope]map[int64]*agent.Revision
	next      map[appScope]int64
}

// NewInMemoryRepository creates an empty repository.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		apps:      make(map[appScope]*agent.App),
		byKey:     make(map[keyScope]string),
		revisions: make(map[appScope]map[int64]*agent.Revision),
		next:      make(map[appScope]int64),
	}
}

// NewRepository is the concise constructor for the InMemory implementation.
func NewRepository() *InMemoryRepository { return NewInMemoryRepository() }

var _ agent.Repository = (*InMemoryRepository)(nil)

// Create stores a new agent application in memory.
func (r *InMemoryRepository) Create(ctx context.Context, input agent.CreateInput) (*agent.App, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	app, err := agent.NewApp(input)
	if err != nil {
		return nil, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	scope := appScope{tenantID: app.TenantID, appID: app.AppID}
	key := keyScope{tenantID: app.TenantID, appKey: app.AppKey}
	if _, exists := r.apps[scope]; exists {
		return nil, fmt.Errorf("%w: %s", agent.ErrDuplicateKey, app.AppID)
	}
	if _, exists := r.byKey[key]; exists {
		return nil, fmt.Errorf("%w: %s", agent.ErrDuplicateKey, app.AppKey)
	}
	copy := app.Clone()
	r.apps[scope] = &copy
	r.byKey[key] = app.AppID
	r.revisions[scope] = make(map[int64]*agent.Revision)
	return cloneApp(app), nil
}

// Get loads an agent application within the requested tenant.
func (r *InMemoryRepository) Get(ctx context.Context, tenantID, appID string) (*agent.App, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.rlock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.runlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	app, err := r.getLocked(tenantID, appID)
	if err != nil {
		return nil, err
	}
	return cloneApp(app), nil
}

// UpdateMetadata applies an expected-version application metadata update.
func (r *InMemoryRepository) UpdateMetadata(ctx context.Context, input agent.UpdateMetadataInput) (*agent.App, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedVersion)
	if err != nil {
		return nil, err
	}
	updated := app.Clone()
	updated.DisplayName = strings.TrimSpace(input.DisplayName)
	updated.Description = strings.TrimSpace(input.Description)
	updated.Version++
	updated.UpdatedAt = nextTime(app.UpdatedAt)
	if err := updated.Validate(); err != nil {
		return nil, err
	}
	r.storeAppLocked(&updated)
	return cloneApp(&updated), nil
}

// CreateDraft stores a draft revision for an agent application.
func (r *InMemoryRepository) CreateDraft(ctx context.Context, input agent.CreateDraftInput) (*agent.Revision, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if _, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion); err != nil {
		return nil, err
	}
	scope := appScope{tenantID: input.TenantID, appID: input.AppID}
	number := r.next[scope] + 1
	draft, err := agent.NewRevision(agent.CreateRevisionInput{
		TenantID: input.TenantID, AppID: input.AppID, Revision: number,
		Kind: input.Kind, SchemaVersion: input.SchemaVersion, Configuration: input.Configuration,
	})
	if err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	copy := draft.Clone()
	r.revisions[scope][number] = &copy
	r.next[scope] = number
	return cloneRevision(draft), nil
}

// UpdateDraft applies an expected-version draft update in memory.
func (r *InMemoryRepository) UpdateDraft(ctx context.Context, input agent.UpdateDraftInput) (*agent.Revision, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if _, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion); err != nil {
		return nil, err
	}
	existing, err := r.revisionLocked(input.TenantID, input.AppID, input.Revision)
	if err != nil {
		return nil, err
	}
	if existing.State != agent.RevisionStateDraft {
		return nil, agent.ErrImmutableRevision
	}
	if input.ExpectedDraftVersion != existing.DraftVersion {
		return nil, conflict(input.ExpectedDraftVersion, existing.DraftVersion)
	}
	candidate, err := agent.NewRevision(agent.CreateRevisionInput{
		TenantID: input.TenantID, AppID: input.AppID, Revision: input.Revision,
		Kind: existing.Kind, SchemaVersion: existing.SchemaVersion, Configuration: input.Configuration,
	})
	if err != nil {
		return nil, err
	}
	candidate.DraftVersion = existing.DraftVersion + 1
	candidate.CreatedAt = existing.CreatedAt
	candidate.UpdatedAt = nextTime(existing.UpdatedAt)
	if err := candidate.Validate(); err != nil {
		return nil, err
	}
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	copy := candidate.Clone()
	r.revisions[appScope{tenantID: input.TenantID, appID: input.AppID}][input.Revision] = &copy
	return cloneRevision(candidate), nil
}

// GetRevision loads a specific in-memory application revision.
func (r *InMemoryRepository) GetRevision(ctx context.Context, tenantID, appID string, revision int64) (*agent.Revision, error) {
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	if err := r.mu.rlock(ctx); err != nil {
		return nil, err
	}
	defer r.mu.runlock()
	if err := checkContext(ctx); err != nil {
		return nil, err
	}
	value, err := r.revisionLocked(tenantID, appID, revision)
	if err != nil {
		return nil, err
	}
	return cloneRevision(value), nil
}

// Publish makes a draft revision active in memory.
func (r *InMemoryRepository) Publish(ctx context.Context, input agent.PublishInput) (*agent.App, *agent.Revision, agent.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	if err := validateChange(input.Metadata); err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	if !input.TenantActive {
		return nil, nil, agent.ChangeEvent{}, fmt.Errorf("%w: tenant must be active", agent.ErrInvalid)
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	draft, err := r.revisionLocked(input.TenantID, input.AppID, input.Revision)
	if err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	if draft.State != agent.RevisionStateDraft {
		return nil, nil, agent.ChangeEvent{}, agent.ErrImmutableRevision
	}
	if input.ExpectedDraftVersion != draft.DraftVersion {
		return nil, nil, agent.ChangeEvent{}, conflict(input.ExpectedDraftVersion, draft.DraftVersion)
	}
	now := nextTime(maxTime(app.UpdatedAt, draft.UpdatedAt))
	published, err := draft.Publish(now)
	if err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	updated := app.Clone()
	previousStatus := updated.Status
	previousRevision := cloneInt64(updated.CurrentRevision)
	updated.CurrentRevision = int64Pointer(input.Revision)
	updated.CanaryRevision = nil
	if updated.Status == agent.StatusDraft {
		updated.Status = agent.StatusActive
	}
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	event := newEvent(agent.ChangePublished, &updated, previousStatus, previousRevision, published.ContentDigest, input.Metadata, app.Version, now)
	if err := checkContext(ctx); err != nil {
		return nil, nil, agent.ChangeEvent{}, err
	}
	r.storeAppLocked(&updated)
	copy := published.Clone()
	r.revisions[appScope{tenantID: input.TenantID, appID: input.AppID}][input.Revision] = &copy
	return cloneApp(&updated), cloneRevision(&published), event.Clone(), nil
}

// Rollback restores an earlier published revision in memory.
func (r *InMemoryRepository) Rollback(ctx context.Context, input agent.RollbackInput) (*agent.App, agent.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if err := validateChange(input.Metadata); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	target, err := r.revisionLocked(input.TenantID, input.AppID, input.TargetRevision)
	if err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if target.State != agent.RevisionStatePublished {
		return nil, agent.ChangeEvent{}, fmt.Errorf("%w: rollback target must be published", agent.ErrInvalid)
	}
	if app.CurrentRevision == nil || *app.CurrentRevision == input.TargetRevision {
		return nil, agent.ChangeEvent{}, fmt.Errorf("%w: rollback must change the current revision", agent.ErrInvalid)
	}
	now := nextTime(app.UpdatedAt)
	updated := app.Clone()
	previous := cloneInt64(updated.CurrentRevision)
	updated.CurrentRevision = int64Pointer(input.TargetRevision)
	updated.CanaryRevision = nil
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	event := newEvent(agent.ChangeRolledBack, &updated, app.Status, previous, target.ContentDigest, input.Metadata, app.Version, now)
	if err := checkContext(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	r.storeAppLocked(&updated)
	return cloneApp(&updated), event.Clone(), nil
}

// SetCanary selects or clears a published candidate revision for all future
// executions of one tenant-scoped App. Existing execution snapshots remain
// unchanged.
func (r *InMemoryRepository) SetCanary(ctx context.Context, input agent.SetCanaryInput) (*agent.App, agent.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if err := validateChange(input.Metadata); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if !input.TenantActive {
		return nil, agent.ChangeEvent{}, fmt.Errorf("%w: tenant must be active", agent.ErrInvalid)
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedAppVersion)
	if err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if app.Status != agent.StatusActive {
		return nil, agent.ChangeEvent{}, fmt.Errorf("%w: canary requires an active app", agent.ErrInvalid)
	}
	var candidate *agent.Revision
	if input.CandidateRevision != nil {
		if *input.CandidateRevision < 1 {
			return nil, agent.ChangeEvent{}, fmt.Errorf("%w: candidate revision must be positive", agent.ErrInvalid)
		}
		var getErr error
		candidate, getErr = r.revisionLocked(input.TenantID, input.AppID, *input.CandidateRevision)
		if getErr != nil {
			return nil, agent.ChangeEvent{}, getErr
		}
		if candidate.State != agent.RevisionStatePublished {
			return nil, agent.ChangeEvent{}, fmt.Errorf("%w: canary revision must be published", agent.ErrInvalid)
		}
		if app.CurrentRevision == nil || *app.CurrentRevision == *input.CandidateRevision {
			return nil, agent.ChangeEvent{}, fmt.Errorf("%w: canary revision must differ from current revision", agent.ErrInvalid)
		}
	}
	previous := cloneInt64(app.CanaryRevision)
	if sameRevision(previous, input.CandidateRevision) {
		return nil, agent.ChangeEvent{}, fmt.Errorf("%w: canary revision is unchanged", agent.ErrInvalid)
	}
	now := nextTime(app.UpdatedAt)
	updated := app.Clone()
	updated.CanaryRevision = cloneInt64(input.CandidateRevision)
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	digest := ""
	if candidate != nil {
		digest = candidate.ContentDigest
	}
	eventType := agent.ChangeCanaryStopped
	if updated.CanaryRevision != nil {
		eventType = agent.ChangeCanaryStarted
	}
	event := newCanaryEvent(eventType, &updated, app.CanaryRevision, digest, input.Metadata, app.Version, now)
	r.storeAppLocked(&updated)
	return cloneApp(&updated), event.Clone(), nil
}

func sameRevision(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// TransitionStatus changes an application status with optimistic concurrency.
func (r *InMemoryRepository) TransitionStatus(ctx context.Context, input agent.TransitionStatusInput) (*agent.App, agent.ChangeEvent, error) {
	if err := checkContext(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if err := validateChange(input.Metadata); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if err := r.mu.lock(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	defer r.mu.unlock()
	if err := checkContext(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	app, err := r.mutableAppLocked(input.TenantID, input.AppID, input.ExpectedVersion)
	if err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	if !app.CanTransitionTo(input.NextStatus) {
		return nil, agent.ChangeEvent{}, fmt.Errorf("%w: %s -> %s", agent.ErrInvalidTransition, app.Status, input.NextStatus)
	}
	now := nextTime(app.UpdatedAt)
	updated := app.Clone()
	previousStatus := updated.Status
	updated.Status = input.NextStatus
	updated.Version++
	updated.UpdatedAt = now
	if err := updated.Validate(); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	digest := ""
	if updated.CurrentRevision != nil {
		current, err := r.revisionLocked(input.TenantID, input.AppID, *updated.CurrentRevision)
		if err != nil {
			return nil, agent.ChangeEvent{}, err
		}
		digest = current.ContentDigest
	}
	event := newEvent(statusEventType(input.NextStatus), &updated, previousStatus, cloneInt64(app.CurrentRevision), digest, input.Metadata, app.Version, now)
	if err := checkContext(ctx); err != nil {
		return nil, agent.ChangeEvent{}, err
	}
	r.storeAppLocked(&updated)
	return cloneApp(&updated), event.Clone(), nil
}

func (r *InMemoryRepository) getLocked(tenantID, appID string) (*agent.App, error) {
	app, ok := r.apps[appScope{tenantID: tenantID, appID: appID}]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s app %s", agent.ErrNotFound, tenantID, appID)
	}
	return app, nil
}

func (r *InMemoryRepository) mutableAppLocked(tenantID, appID string, expected int64) (*agent.App, error) {
	app, err := r.getLocked(tenantID, appID)
	if err != nil {
		return nil, err
	}
	if app.Status == agent.StatusDisabled {
		return nil, agent.ErrDisabled
	}
	if expected != app.Version {
		return nil, conflict(expected, app.Version)
	}
	return app, nil
}

func (r *InMemoryRepository) revisionLocked(tenantID, appID string, revision int64) (*agent.Revision, error) {
	scope := appScope{tenantID: tenantID, appID: appID}
	if _, err := r.getLocked(tenantID, appID); err != nil {
		return nil, err
	}
	value, ok := r.revisions[scope][revision]
	if !ok {
		return nil, fmt.Errorf("%w: tenant %s app %s revision %d", agent.ErrNotFound, tenantID, appID, revision)
	}
	return value, nil
}

func (r *InMemoryRepository) storeAppLocked(app *agent.App) {
	copy := app.Clone()
	r.apps[appScope{tenantID: app.TenantID, appID: app.AppID}] = &copy
}

func validateChange(metadata agent.ChangeMetadata) error {
	reason := strings.TrimSpace(metadata.Reason)
	if strings.TrimSpace(metadata.ActorType) == "" || strings.TrimSpace(metadata.ActorID) == "" || reason == "" || strings.TrimSpace(metadata.CorrelationID) == "" {
		return fmt.Errorf("%w: change metadata requires actor, reason, and correlation ID", agent.ErrInvalid)
	}
	if len([]rune(reason)) > 1000 {
		return fmt.Errorf("%w: change reason must contain at most 1000 characters", agent.ErrInvalid)
	}
	return nil
}

func newEvent(eventType agent.ChangeEventType, app *agent.App, previousStatus agent.Status, previousRevision *int64, digest string, metadata agent.ChangeMetadata, previousVersion int64, at time.Time) agent.ChangeEvent {
	return agent.ChangeEvent{
		EventType: eventType, TenantID: app.TenantID, AppID: app.AppID,
		PreviousRevision: cloneInt64(previousRevision), CurrentRevision: cloneInt64(app.CurrentRevision),
		ContentDigest: digest, PreviousStatus: previousStatus, CurrentStatus: app.Status,
		ActorType: strings.TrimSpace(metadata.ActorType), ActorID: strings.TrimSpace(metadata.ActorID),
		Reason: strings.TrimSpace(metadata.Reason), CorrelationID: strings.TrimSpace(metadata.CorrelationID),
		PreviousVersion: previousVersion, NextVersion: app.Version, OccurredAt: at,
	}
}

func newCanaryEvent(eventType agent.ChangeEventType, app *agent.App, previousRevision *int64, digest string, metadata agent.ChangeMetadata, previousVersion int64, at time.Time) agent.ChangeEvent {
	event := newEvent(eventType, app, app.Status, previousRevision, digest, metadata, previousVersion, at)
	event.CurrentRevision = cloneInt64(app.CanaryRevision)
	return event
}

func statusEventType(next agent.Status) agent.ChangeEventType {
	switch next {
	case agent.StatusSuspended:
		return agent.ChangeSuspended
	case agent.StatusActive:
		return agent.ChangeResumed
	default:
		return agent.ChangeDisabled
	}
}

func conflict(expected, actual int64) error {
	return fmt.Errorf("%w: expected %d, got %d", agent.ErrConflict, expected, actual)
}

func checkContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func cloneApp(app *agent.App) *agent.App {
	if app == nil {
		return nil
	}
	copy := app.Clone()
	return &copy
}

func cloneRevision(revision *agent.Revision) *agent.Revision {
	if revision == nil {
		return nil
	}
	copy := revision.Clone()
	return &copy
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func int64Pointer(value int64) *int64 { return &value }

func maxTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func nextTime(previous time.Time) time.Time {
	now := time.Now().UTC()
	if now.Before(previous) {
		return previous
	}
	return now
}
