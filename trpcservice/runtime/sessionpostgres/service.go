// Package sessionpostgres adapts the tenant-scoped RuntimeStore to the
// upstream session.Service capability used by Runner.
package sessionpostgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"sync"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// Service keeps the upstream session behavior for summaries, tracks, and
// transient state while making Session metadata, state versions, and events
// durable through a tenant-scoped RuntimeStore.
type Service struct {
	tenantID string
	delegate session.Service
	store    runtimestorage.RuntimeStore
	mu       sync.Mutex
	versions map[string]int64
}

// New creates a fixed-tenant session capability. The delegate is borrowed;
// callers remain responsible for closing it.
func New(tenantID string, delegate session.Service, store runtimestorage.RuntimeStore) (*Service, error) {
	if runtimestorage.ValidateTenant(tenantID) != nil || delegate == nil || store == nil {
		return nil, runtimestorage.ErrInvalid
	}
	return &Service{tenantID: tenantID, delegate: delegate, store: store, versions: map[string]int64{}}, nil
}

func (s *Service) CreateSession(ctx context.Context, key session.Key, state session.StateMap, options ...session.Option) (*session.Session, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	created, err := s.delegate.CreateSession(ctx, key, state, options...)
	if err != nil {
		return nil, err
	}
	if _, err := s.store.CreateSession(ctx, s.tenantID, key.SessionID, stateToAny(created.State)); err != nil && !errors.Is(err, runtimestorage.ErrDuplicate) {
		_ = s.delegate.DeleteSession(ctx, key)
		return nil, err
	}
	s.setVersion(key.SessionID, 1)
	return created, nil
}

func (s *Service) GetSession(ctx context.Context, key session.Key, options ...session.Option) (*session.Session, error) {
	if err := validateKey(key); err != nil {
		return nil, err
	}
	persisted, storeErr := s.store.GetSession(ctx, s.tenantID, key.SessionID)
	if storeErr != nil {
		if errors.Is(storeErr, runtimestorage.ErrNotFound) {
			return s.delegate.GetSession(ctx, key, options...)
		}
		return nil, storeErr
	}
	state := anyToState(persisted.State)
	value, err := s.delegate.GetSession(ctx, key, options...)
	if err == nil && value != nil {
		if err := s.delegate.UpdateSessionState(ctx, key, state); err != nil {
			return nil, err
		}
		if refreshed, refreshErr := s.delegate.GetSession(ctx, key, options...); refreshErr == nil && refreshed != nil {
			value = refreshed
		}
		value.State = cloneState(state)
		if err := s.restoreHistory(ctx, value, key.SessionID, options...); err != nil {
			return nil, err
		}
		s.setVersion(key.SessionID, persisted.Version)
		return value, nil
	}
	value, err = s.delegate.CreateSession(ctx, key, state)
	if err != nil {
		return nil, err
	}
	if err := s.restoreHistory(ctx, value, key.SessionID, options...); err != nil {
		return nil, err
	}
	s.setVersion(key.SessionID, persisted.Version)
	return value, err
}

func (s *Service) restoreHistory(ctx context.Context, value *session.Session, sessionID string, options ...session.Option) error {
	history, err := s.store.ListEventPayloads(ctx, s.tenantID, sessionID)
	if err != nil {
		return err
	}
	existing := make(map[string]struct{}, value.GetEventCount())
	for _, item := range value.GetEvents() {
		existing[item.ID] = struct{}{}
	}
	for _, item := range history {
		if _, ok := existing[item.EventID]; ok {
			continue
		}
		var historical trpcevent.Event
		if err := json.Unmarshal(item.Payload, &historical); err != nil {
			return runtimestorage.ErrStorage
		}
		if err := s.delegate.AppendEvent(ctx, value, &historical, options...); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	if err := validateKey(key); err != nil {
		return err
	}
	persisted, err := s.store.GetSession(ctx, s.tenantID, key.SessionID)
	if err != nil {
		return err
	}
	updated, err := s.store.UpdateSessionState(ctx, s.tenantID, key.SessionID, persisted.Version, stateToAny(state))
	if err != nil {
		return err
	}
	s.setVersion(key.SessionID, updated.Version)
	return s.delegate.UpdateSessionState(ctx, key, state)
}

func (s *Service) AppendEvent(ctx context.Context, sess *session.Session, value *trpcevent.Event, options ...session.Option) error {
	if sess == nil || value == nil {
		return session.ErrNilSession
	}
	if value.ID == "" {
		return runtimestorage.ErrInvalid
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return runtimestorage.ErrInvalid
	}
	// Inbound message_event rows are created by the trusted Channel/Gateway
	// boundary, where binding_id and external_message_id are available. Runner
	// event history is a separate session-scoped immutable log.
	if _, err := s.store.AppendEventPayload(ctx, runtimestorage.EventPayload{
		TenantID: s.tenantID, SessionID: sess.ID, EventID: value.ID, Payload: payload,
	}); err != nil {
		return err
	}
	return s.delegate.AppendEvent(ctx, sess, value, options...)
}

// The remaining methods preserve the complete upstream Service contract while
// keeping their existing delegate semantics.
func (s *Service) ListSessions(ctx context.Context, key session.UserKey, options ...session.Option) ([]*session.Session, error) {
	return s.delegate.ListSessions(ctx, key, options...)
}
func (s *Service) DeleteSession(ctx context.Context, key session.Key, options ...session.Option) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if err := s.store.DeleteSession(ctx, s.tenantID, key.SessionID); err != nil && !errors.Is(err, runtimestorage.ErrNotFound) {
		return err
	}
	if err := s.delegate.DeleteSession(ctx, key, options...); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.versions, key.SessionID)
	s.mu.Unlock()
	return nil
}
func (s *Service) UpdateAppState(ctx context.Context, app string, state session.StateMap) error {
	return s.delegate.UpdateAppState(ctx, app, state)
}
func (s *Service) DeleteAppState(ctx context.Context, app, key string) error {
	return s.delegate.DeleteAppState(ctx, app, key)
}
func (s *Service) ListAppStates(ctx context.Context, app string) (session.StateMap, error) {
	return s.delegate.ListAppStates(ctx, app)
}
func (s *Service) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	return s.delegate.UpdateUserState(ctx, key, state)
}
func (s *Service) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	return s.delegate.ListUserStates(ctx, key)
}
func (s *Service) DeleteUserState(ctx context.Context, key session.UserKey, field string) error {
	return s.delegate.DeleteUserState(ctx, key, field)
}
func (s *Service) CreateSessionSummary(ctx context.Context, value *session.Session, filter string, force bool) error {
	return s.delegate.CreateSessionSummary(ctx, value, filter, force)
}
func (s *Service) EnqueueSummaryJob(ctx context.Context, value *session.Session, filter string, force bool) error {
	return s.delegate.EnqueueSummaryJob(ctx, value, filter, force)
}
func (s *Service) GetSessionSummaryText(ctx context.Context, value *session.Session, options ...session.SummaryOption) (string, bool) {
	return s.delegate.GetSessionSummaryText(ctx, value, options...)
}
func (s *Service) Close() error { return s.delegate.Close() }

func validateKey(key session.Key) error {
	if key.AppName == "" {
		return session.ErrAppNameRequired
	}
	if key.UserID == "" {
		return session.ErrUserIDRequired
	}
	if key.SessionID == "" {
		return session.ErrSessionIDRequired
	}
	return nil
}
func (s *Service) setVersion(id string, version int64) {
	s.mu.Lock()
	s.versions[id] = version
	s.mu.Unlock()
}
func cloneState(value session.StateMap) session.StateMap {
	result := make(session.StateMap, len(value))
	for key, data := range value {
		result[key] = append([]byte(nil), data...)
	}
	return result
}
func stateToAny(value session.StateMap) map[string]any {
	result := make(map[string]any, len(value))
	for key, data := range value {
		result[key] = append([]byte(nil), data...)
	}
	return result
}
func anyToState(value map[string]any) session.StateMap {
	result := make(session.StateMap, len(value))
	for key, data := range value {
		if raw, ok := data.([]byte); ok {
			result[key] = append([]byte(nil), raw...)
		} else if encoded, ok := data.(string); ok {
			if decoded, err := base64.StdEncoding.DecodeString(encoded); err == nil {
				result[key] = decoded
			} else {
				result[key] = []byte(encoded)
			}
		} else if encoded, err := json.Marshal(data); err == nil {
			result[key] = encoded
		}
	}
	return result
}

var _ session.Service = (*Service)(nil)
