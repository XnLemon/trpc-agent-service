package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
)

func TestBackendRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil, nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, _, err := r.Create(ctx, backend.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant", "profile"); return err }},
		{"update", func() error { _, _, err := r.UpdateConfiguration(ctx, backend.UpdateConfigurationInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, backend.TransitionStatusInput{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
