package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestChannelRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, _, err := r.Create(ctx, channels.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant", "binding"); return err }},
		{"update", func() error { _, _, err := r.UpdateConfiguration(ctx, channels.UpdateConfigurationInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, channels.TransitionStatusInput{}); return err }},
		{"lookup candidates", func() error { _, err := r.LookupCandidates(ctx, "channel", "digest"); return err }},
		{"consume candidate", func() error { _, err := r.ConsumeCandidate(ctx, channels.CandidateBindingContext{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
