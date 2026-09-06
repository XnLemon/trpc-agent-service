package replies

import (
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
)

func TestRenderCompatibilityFacade(t *testing.T) {
	got := Render([]gateway.DispatchEvent{{Type: gateway.DispatchEventMessage, Text: "reply"}})
	if got.Kind != KindText || got.Text != "reply" {
		t.Fatalf("compatibility reply = %+v", got)
	}
}
