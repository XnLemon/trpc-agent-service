package runtimeprofile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	trpcagent "trpc.group/trpc-go/trpc-agent-go/agent"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	trpcrunner "trpc.group/trpc-go/trpc-agent-go/runner"
)

const RemoteProtocolV1 = "runtime.v1"

type RemoteRunRequest struct {
	Protocol                                                                      string
	TenantID, AppID                                                               string
	Revision                                                                      int64
	RuntimeProfileID, RuntimeVersion, RuntimeDigest, RequestID, UserID, SessionID string
	Message                                                                       trpcmodel.Message
	Deadline                                                                      time.Time
}
type RemoteTransport interface {
	Run(context.Context, RemoteRunRequest) (<-chan *trpcevent.Event, error)
	Cancel(context.Context, string) error
	Health(context.Context) error
	Close() error
}

// RemoteRunner adapts the versioned transport contract to tRPC-Agent-Go Runner.
type RemoteRunner struct {
	transport RemoteTransport
	profile   RuntimeProfile
	mu        sync.Mutex
	canceled  map[string]context.CancelFunc
	closed    bool
}

func NewRemoteRunner(transport RemoteTransport, profile RuntimeProfile) (*RemoteRunner, error) {
	if transport == nil {
		return nil, ErrInvalid
	}
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	if profile.ExecutionMode != "remote" {
		return nil, fmt.Errorf("%w: profile is not remote", ErrInvalid)
	}
	return &RemoteRunner{transport: transport, profile: profile, canceled: map[string]context.CancelFunc{}}, nil
}
func (r *RemoteRunner) Run(ctx context.Context, userID, sessionID string, message trpcmodel.Message, opts ...trpcagent.RunOption) (<-chan *trpcevent.Event, error) {
	if ctx == nil {
		return nil, context.Canceled
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("remote runner closed")
	}
	runCtx, cancel := context.WithCancel(ctx)
	requestID := fmt.Sprintf("%d", time.Now().UnixNano())
	r.canceled[requestID] = cancel
	r.mu.Unlock()
	req := RemoteRunRequest{Protocol: RemoteProtocolV1, TenantID: r.profile.TenantID, RuntimeProfileID: r.profile.ProfileID, RuntimeVersion: r.profile.RuntimeVersion, RuntimeDigest: r.profile.ImplementationDigest, UserID: userID, SessionID: sessionID, RequestID: requestID, Message: message}
	if deadline, ok := ctx.Deadline(); ok {
		req.Deadline = deadline
	}
	events, err := r.transport.Run(runCtx, req)
	if err != nil {
		cancel()
		r.mu.Lock()
		delete(r.canceled, requestID)
		r.mu.Unlock()
		return nil, ErrUnavailable
	}
	out := make(chan *trpcevent.Event)
	go func() {
		defer close(out)
		defer cancel()
		defer func() { r.mu.Lock(); delete(r.canceled, requestID); r.mu.Unlock() }()
		seen := map[string]bool{}
		for ev := range events {
			if err := ValidateRemoteEvent(ev, seen); err != nil {
				return
			}
			select {
			case out <- ev:
			case <-runCtx.Done():
				return
			}
		}
	}()
	return out, nil
}

// ValidateRemoteEvent rejects nil, duplicate, or identity-less events.
func ValidateRemoteEvent(ev *trpcevent.Event, seen map[string]bool) error {
	if ev == nil || ev.ID == "" {
		return ErrInvalid
	}
	if seen != nil {
		if seen[ev.ID] {
			return ErrInvalid
		}
		seen[ev.ID] = true
	}
	return nil
}
func (r *RemoteRunner) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	for _, cancel := range r.canceled {
		cancel()
	}
	r.canceled = map[string]context.CancelFunc{}
	r.mu.Unlock()
	return r.transport.Close()
}
func (r *RemoteRunner) Cancel(requestID string) bool {
	r.mu.Lock()
	cancel, ok := r.canceled[requestID]
	r.mu.Unlock()
	if !ok {
		return false
	}
	cancel()
	return true
}
func (r *RemoteRunner) RunStatus(string) (trpcrunner.RunStatus, bool) {
	return trpcrunner.RunStatus{}, false
}
func (r *RemoteRunner) Health(ctx context.Context) error {
	if ctx == nil {
		return context.Canceled
	}
	return r.transport.Health(ctx)
}
