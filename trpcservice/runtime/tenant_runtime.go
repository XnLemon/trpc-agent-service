package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
)

var (
	// ErrInvalidTenantRuntime reports malformed tenant runtime input.
	ErrInvalidTenantRuntime = errors.New("invalid tenant runtime")
	// ErrTenantRuntimeClosed reports use after the registry has been closed.
	ErrTenantRuntimeClosed = errors.New("tenant runtime registry is closed")
)

// TenantRuntime is the narrow runtime capability needed to make a control-
// plane tenant executable. Implementations may resolve secrets and register
// provider factories, but they must not expose those values to callers.
type TenantRuntime interface {
	Ensure(context.Context, string) error
}

// TenantRuntimeInvalidator is an optional mutation hook owned by the runtime
// composition root. The next execution re-materializes the tenant after an
// invalidation.
type TenantRuntimeInvalidator interface {
	InvalidateTenant(string)
}

// TenantRuntimeMaterializer adapts one trusted tenant materialization
// operation to TenantRuntimeRegistry.
type TenantRuntimeMaterializer func(context.Context, string) error

type tenantRuntimeState struct {
	done        chan struct{}
	ready       bool
	invalidated bool
	err         error
}

// TenantRuntimeRegistry provides concurrent, idempotent lazy materialization
// for tenant-scoped runtime providers. Only one materializer runs for a
// tenant at a time; an invalidation cannot be overwritten by an in-flight
// materialization.
type TenantRuntimeRegistry struct {
	mu          sync.Mutex
	materialize TenantRuntimeMaterializer
	states      map[string]*tenantRuntimeState
	closed      bool
}

// NewTenantRuntimeRegistry creates a lazy tenant runtime registry.
func NewTenantRuntimeRegistry(materialize TenantRuntimeMaterializer) (*TenantRuntimeRegistry, error) {
	if materialize == nil {
		return nil, ErrInvalidTenantRuntime
	}
	return &TenantRuntimeRegistry{materialize: materialize, states: make(map[string]*tenantRuntimeState)}, nil
}

// Ensure materializes tenantID once until it is invalidated. A failed
// materialization is not cached, allowing a later configuration/secret update
// to recover without restarting the process.
func (registry *TenantRuntimeRegistry) Ensure(ctx context.Context, tenantID string) error {
	if registry == nil || ctx == nil || strings.TrimSpace(tenantID) == "" {
		return ErrInvalidTenantRuntime
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		registry.mu.Lock()
		if registry.closed {
			registry.mu.Unlock()
			return ErrTenantRuntimeClosed
		}
		state := registry.states[tenantID]
		if state == nil {
			state = &tenantRuntimeState{done: make(chan struct{})}
			registry.states[tenantID] = state
			registry.mu.Unlock()
			return registry.materializeOne(ctx, tenantID, state)
		}
		if state.ready {
			registry.mu.Unlock()
			return nil
		}
		done := state.done
		registry.mu.Unlock()

		select {
		case <-done:
			if err := ctx.Err(); err != nil {
				return err
			}
			if state.err != nil {
				return state.err
			}
			// An invalidation won a race with materialization. Re-check the
			// current state and materialize the fresh version.
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (registry *TenantRuntimeRegistry) materializeOne(ctx context.Context, tenantID string, state *tenantRuntimeState) error {
	err := registry.materialize(ctx, tenantID)
	registry.mu.Lock()
	if current := registry.states[tenantID]; current == state {
		if registry.closed {
			state.err = ErrTenantRuntimeClosed
			delete(registry.states, tenantID)
		} else if state.invalidated && err == nil {
			// Do not publish a stale registration. Waiters will retry through
			// Ensure after this state is released.
			state.err = nil
			delete(registry.states, tenantID)
		} else if err == nil {
			state.ready = true
		} else {
			state.err = err
			delete(registry.states, tenantID)
		}
		close(state.done)
	}
	invalidated := state.invalidated && err == nil
	stateErr := state.err
	registry.mu.Unlock()
	if invalidated {
		return registry.Ensure(ctx, tenantID)
	}
	return stateErr
}

// InvalidateTenant removes the published materialization. It is harmless for
// an unknown tenant and safe to call from a best-effort mutation callback.
func (registry *TenantRuntimeRegistry) InvalidateTenant(tenantID string) {
	if registry == nil || strings.TrimSpace(tenantID) == "" {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return
	}
	if state := registry.states[tenantID]; state != nil {
		if state.ready {
			delete(registry.states, tenantID)
			return
		}
		state.invalidated = true
	}
}

// Close prevents new materialization and drops published state. In-flight
// materializers finish their current operation before their waiters are
// released with ErrTenantRuntimeClosed.
func (registry *TenantRuntimeRegistry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil
	}
	registry.closed = true
	for tenantID, state := range registry.states {
		if state.ready {
			delete(registry.states, tenantID)
		} else {
			state.invalidated = true
		}
	}
	return nil
}

var _ TenantRuntime = (*TenantRuntimeRegistry)(nil)
var _ TenantRuntimeInvalidator = (*TenantRuntimeRegistry)(nil)
