// Package replies is a compatibility facade for the historical channel reply
// renderer. New code should import gateway/replies because event rendering is a
// Gateway boundary, not a Channel Binding domain concern.
package replies

import (
	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
	gatewayreplies "github.com/XnLemon/trpc-agent-service/trpcservice/gateway/replies"
)

const (
	// KindText identifies a rendered text reply.
	//
	// Deprecated: use gateway/replies.KindText.
	KindText = gatewayreplies.KindText
	// KindFallback identifies a deterministic safe fallback reply.
	//
	// Deprecated: use gateway/replies.KindFallback.
	KindFallback = gatewayreplies.KindFallback
	// StableFallback is the deterministic safe fallback reply.
	//
	// Deprecated: use gateway/replies.StableFallback.
	StableFallback = gatewayreplies.StableFallback
)

// Reply is the historical channel reply result.
//
// Deprecated: use gateway/replies.Reply.
type Reply = gatewayreplies.Reply

// Render preserves the historical import path while delegating ownership to
// the Gateway event boundary.
//
// Deprecated: use gateway/replies.Render.
func Render(events []gateway.DispatchEvent) Reply { return gatewayreplies.Render(events) }
