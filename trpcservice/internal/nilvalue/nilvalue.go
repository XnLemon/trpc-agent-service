// Package nilvalue provides the one nil check used at interface boundaries.
//
// A typed nil pointer stored in an interface is not equal to nil. Treating it
// as usable lets an invalid dependency cross a constructor boundary and turn
// a configuration error into a panic later. Keep this helper small and free of
// project dependencies so every service package can fail closed consistently.
package nilvalue

import (
	"context"

	rootnilvalue "github.com/XnLemon/trpc-agent-service/internal/nilvalue"
)

// Is reports whether value is nil, including a nil pointer (or other nil-able
// value) held inside an interface.
func Is(value any) bool { return rootnilvalue.Is(value) }

// ErrInvalidContext indicates a nil, typed-nil, or otherwise unusable context.
var ErrInvalidContext = rootnilvalue.ErrInvalidContext

// ContextErr returns the context cancellation state without panicking at a
// nil, typed-nil, or panic-prone context boundary.
func ContextErr(ctx context.Context) error { return rootnilvalue.ContextErr(ctx) }

// ContextValue reads a context value without allowing a custom context to
// panic at a service boundary.
func ContextValue(ctx context.Context, key any) (any, error) {
	return rootnilvalue.ContextValue(ctx, key)
}

// ContextDoneChannel obtains a context cancellation channel without
// consulting Err again.
func ContextDoneChannel(ctx context.Context) (<-chan struct{}, error) {
	return rootnilvalue.ContextDoneChannel(ctx)
}

// ContextDone obtains a context cancellation channel without allowing a
// panic-prone custom context to escape.
func ContextDone(ctx context.Context) (<-chan struct{}, error) { return rootnilvalue.ContextDone(ctx) }
