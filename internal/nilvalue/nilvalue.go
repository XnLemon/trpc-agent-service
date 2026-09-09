// Package nilvalue provides the one nil check used at interface boundaries.
//
// A typed nil pointer stored in an interface is not equal to nil. Treating it
// as usable lets an invalid dependency cross a constructor boundary and turn
// a configuration error into a panic later. Keep this helper small and free of
// project dependencies so every service package can fail closed consistently.
package nilvalue

import (
	"context"
	"errors"
	"reflect"
)

// ErrInvalidContext indicates a nil, typed-nil, or otherwise unusable context.
var ErrInvalidContext = errors.New("invalid context")

// Is reports whether value is nil, including a nil pointer (or other nil-able
// value) held inside an interface.
func Is(value any) bool {
	if value == nil {
		return true
	}
	candidate := reflect.ValueOf(value)
	switch candidate.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return candidate.IsNil()
	default:
		return false
	}
}

// ContextErr returns the context cancellation state without invoking Err on a
// nil, typed-nil, or panic-prone context. A nil-like context is invalid rather
// than silently treated as active.
func ContextErr(ctx context.Context) (err error) {
	if Is(ctx) {
		return ErrInvalidContext
	}
	defer func() {
		if recover() != nil {
			err = ErrInvalidContext
		}
	}()
	err = ctx.Err()
	if Is(err) {
		return nil
	}
	return err
}

// ContextValue reads a context value without allowing a custom context
// implementation to panic at a service boundary.
func ContextValue(ctx context.Context, key any) (value any, err error) {
	if Is(ctx) {
		return nil, ErrInvalidContext
	}
	defer func() {
		if recover() != nil {
			value = nil
			err = ErrInvalidContext
		}
	}()
	return ctx.Value(key), nil
}

// ContextDoneChannel obtains a context's cancellation channel without
// consulting Err again. It is useful after a caller has already checked Err
// and avoids changing the cancellation-observation ordering of stateful
// context implementations. A nil channel is valid and means that the context
// cannot be canceled.
func ContextDoneChannel(ctx context.Context) (done <-chan struct{}, err error) {
	if Is(ctx) {
		return nil, ErrInvalidContext
	}
	defer func() {
		if recover() != nil {
			done = nil
			err = ErrInvalidContext
		}
	}()
	return ctx.Done(), nil
}

// ContextDone obtains a context's cancellation channel without allowing a
// custom context implementation to panic at a service boundary. A nil channel
// is valid and means that the context cannot be canceled. Callers should
// handle the returned error before selecting on the channel.
func ContextDone(ctx context.Context) (done <-chan struct{}, err error) {
	if contextErr := ContextErr(ctx); contextErr != nil {
		return nil, contextErr
	}
	return ContextDoneChannel(ctx)
}
