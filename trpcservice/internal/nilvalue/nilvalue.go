// Package nilvalue provides the one nil check used at interface boundaries.
//
// A typed nil pointer stored in an interface is not equal to nil. Treating it
// as usable lets an invalid dependency cross a constructor boundary and turn
// a configuration error into a panic later. Keep this helper small and free of
// project dependencies so every service package can fail closed consistently.
package nilvalue

import rootnilvalue "github.com/XnLemon/trpc-agent-service/internal/nilvalue"

// Is reports whether value is nil, including a nil pointer (or other nil-able
// value) held inside an interface.
func Is(value any) bool { return rootnilvalue.Is(value) }
