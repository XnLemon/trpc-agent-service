// Package nilvalue provides the one nil check used at interface boundaries.
//
// A typed nil pointer stored in an interface is not equal to nil. Treating it
// as usable lets an invalid dependency cross a constructor boundary and turn
// a configuration error into a panic later. Keep this helper small and free of
// project dependencies so every service package can fail closed consistently.
package nilvalue

import "reflect"

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
