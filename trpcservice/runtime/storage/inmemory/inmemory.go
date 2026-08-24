// Package inmemory provides a concurrency-safe runtime store for tests and local development.
package inmemory

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

type Store struct {
	mu       sync.RWMutex
	sessions map[string]runtimestorage.Session
	events   map[string]runtimestorage.MessageEvent
	messages map[string]string
	replies  map[string]runtimestorage.ReplyOutbox
}

func New() *Store {
	return &Store{sessions: map[string]runtimestorage.Session{}, events: map[string]runtimestorage.MessageEvent{}, messages: map[string]string{}, replies: map[string]runtimestorage.ReplyOutbox{}}
}

func (s *Store) GetSession(ctx context.Context, tenantID, sessionID string) (runtimestorage.Session, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return runtimestorage.Session{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.sessions[key(tenantID, sessionID)]
	if !ok {
		return runtimestorage.Session{}, runtimestorage.ErrNotFound
	}
	return cloneSession(value), nil
}

func (s *Store) CreateSession(ctx context.Context, tenantID, sessionID string, state map[string]any) (runtimestorage.Session, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return runtimestorage.Session{}, err
	}
	now := time.Now().UTC()
	value := runtimestorage.Session{TenantID: tenantID, SessionID: sessionID, Status: runtimestorage.SessionActive, Version: 1, State: cloneMap(state), CreatedAt: now, UpdatedAt: now}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.sessions[key(tenantID, sessionID)]; ok {
		return runtimestorage.Session{}, runtimestorage.ErrDuplicate
	}
	s.sessions[key(tenantID, sessionID)] = value
	return cloneSession(value), nil
}

func (s *Store) UpdateSessionState(ctx context.Context, tenantID, sessionID string, expectedVersion int64, state map[string]any) (runtimestorage.Session, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.Session{}, err
	}
	if err := runtimestorage.ValidateSession(tenantID, sessionID); err != nil {
		return runtimestorage.Session{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(tenantID, sessionID)
	value, ok := s.sessions[k]
	if !ok {
		return runtimestorage.Session{}, runtimestorage.ErrNotFound
	}
	if value.Version != expectedVersion {
		return runtimestorage.Session{}, runtimestorage.ErrConflict
	}
	value.Version++
	value.State = cloneMap(state)
	value.UpdatedAt = time.Now().UTC()
	s.sessions[k] = value
	return cloneSession(value), nil
}

func (s *Store) RecordMessage(ctx context.Context, input runtimestorage.MessageEventInput) (runtimestorage.MessageEvent, bool, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, false, err
	}
	if runtimestorage.ValidateSession(input.TenantID, input.SessionID) != nil || input.BindingID == "" || input.ExternalMessageID == "" || input.EventID == "" {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	unique := key(input.TenantID, input.BindingID, input.ExternalMessageID)
	if existingID, ok := s.messages[unique]; ok {
		return cloneEvent(s.events[existingID]), true, nil
	}
	sessionKey := key(input.TenantID, input.SessionID)
	sess, ok := s.sessions[sessionKey]
	if !ok {
		return runtimestorage.MessageEvent{}, false, runtimestorage.ErrNotFound
	}
	sess.Version++
	sess.UpdatedAt = time.Now().UTC()
	s.sessions[sessionKey] = sess
	event := runtimestorage.MessageEvent{TenantID: input.TenantID, EventID: input.EventID, SessionID: input.SessionID, BindingID: input.BindingID, ExternalMessageID: input.ExternalMessageID, IdempotencyKey: input.IdempotencyKey, EventSeq: sess.Version, Status: runtimestorage.EventReceived, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	s.events[key(input.TenantID, input.EventID)] = event
	s.messages[unique] = key(input.TenantID, input.EventID)
	return cloneEvent(event), false, nil
}

func (s *Store) GetMessage(ctx context.Context, tenantID, eventID string) (runtimestorage.MessageEvent, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.MessageEvent{}, err
	}
	if err := runtimestorage.ValidateTenant(tenantID); err != nil || eventID == "" {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.events[key(tenantID, eventID)]
	if !ok {
		return runtimestorage.MessageEvent{}, runtimestorage.ErrNotFound
	}
	return cloneEvent(value), nil
}

func (s *Store) EnqueueReply(ctx context.Context, value runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if err := runtimestorage.ValidateTenant(value.TenantID); err != nil || value.ReplyID == "" || value.EventID == "" || value.SegmentIndex < 0 || value.SegmentCount <= value.SegmentIndex {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if value.Status == "" {
		value.Status = runtimestorage.ReplyPending
	}
	if value.Status != runtimestorage.ReplyPending {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	now := time.Now().UTC()
	value.CreatedAt = now
	value.UpdatedAt = now
	s.mu.Lock()
	defer s.mu.Unlock()
	k := replyKey(value.TenantID, value.ReplyID, value.SegmentIndex)
	if existing, ok := s.replies[k]; ok {
		return cloneReply(existing), nil
	}
	s.replies[k] = value
	return cloneReply(value), nil
}

func (s *Store) GetReply(ctx context.Context, tenantID, replyID string, segment int) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || segment < 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.replies[replyKey(tenantID, replyID, segment)]
	if !ok {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	return cloneReply(value), nil
}

func (s *Store) ClaimReply(ctx context.Context, tenantID, replyID string, segment int, owner string, leaseDuration time.Duration) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(tenantID) != nil || replyID == "" || owner == "" || leaseDuration <= 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := replyKey(tenantID, replyID, segment)
	value, ok := s.replies[k]
	if !ok {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	if value.Status != runtimestorage.ReplyPending && value.Status != runtimestorage.ReplyRetryable && !(value.Status == runtimestorage.ReplySending && value.LeaseExpiresAt != nil && !value.LeaseExpiresAt.After(time.Now().UTC())) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
	}
	deadline := time.Now().UTC().Add(leaseDuration)
	value.Status = runtimestorage.ReplySending
	value.Attempts++
	value.FencingToken++
	value.LeaseOwner = owner
	value.LeaseExpiresAt = &deadline
	value.UpdatedAt = time.Now().UTC()
	s.replies[k] = value
	return cloneReply(value), nil
}

func (s *Store) TransitionReply(ctx context.Context, transition runtimestorage.ReplyTransition) (runtimestorage.ReplyOutbox, error) {
	if err := check(ctx); err != nil {
		return runtimestorage.ReplyOutbox{}, err
	}
	if runtimestorage.ValidateTenant(transition.TenantID) != nil || transition.ReplyID == "" || transition.Owner == "" {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrInvalid
	}
	if !runtimestorage.ValidateTransition(transition.From, transition.To) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrIllegalTransition
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := replyKey(transition.TenantID, transition.ReplyID, transition.SegmentIndex)
	value, ok := s.replies[k]
	if !ok {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	if value.Status != transition.From || (value.LeaseOwner != "" && value.LeaseOwner != transition.Owner) || (transition.FencingToken != 0 && value.FencingToken != transition.FencingToken) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
	}
	if value.Status == runtimestorage.ReplySending && value.LeaseExpiresAt != nil && !value.LeaseExpiresAt.After(time.Now().UTC()) {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrConflict
	}
	value.Status = transition.To
	value.LeaseOwner = transition.Owner
	value.FencingToken++
	if transition.To == runtimestorage.ReplySending {
		value.Attempts++
		if transition.LeaseDuration > 0 {
			deadline := time.Now().UTC().Add(transition.LeaseDuration)
			value.LeaseExpiresAt = &deadline
		}
	}
	value.ProviderMessageID = transition.ProviderID
	value.LastErrorClass = transition.ErrorClass
	value.UpdatedAt = time.Now().UTC()
	s.replies[k] = value
	return cloneReply(value), nil
}

func (s *Store) Close() error { return nil }
func check(ctx context.Context) error {
	if ctx == nil {
		return runtimestorage.ErrInvalid
	}
	return ctx.Err()
}
func key(parts ...string) string {
	var out string
	for _, p := range parts {
		out += string(rune(len(p))) + p
	}
	return out
}
func replyKey(tenant, reply string, segment int) string {
	return key(tenant, reply, string(rune(segment)))
}
func cloneMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil
	}
	var output map[string]any
	if err := json.Unmarshal(encoded, &output); err != nil {
		return nil
	}
	return output
}
func cloneSession(value runtimestorage.Session) runtimestorage.Session {
	value.State = cloneMap(value.State)
	return value
}
func cloneEvent(value runtimestorage.MessageEvent) runtimestorage.MessageEvent {
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}
func cloneReply(value runtimestorage.ReplyOutbox) runtimestorage.ReplyOutbox {
	if value.LeaseExpiresAt != nil {
		copy := *value.LeaseExpiresAt
		value.LeaseExpiresAt = &copy
	}
	return value
}

var _ runtimestorage.RuntimeStore = (*Store)(nil)
