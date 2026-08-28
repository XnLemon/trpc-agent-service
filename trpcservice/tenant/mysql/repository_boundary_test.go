package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestTenantRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, err := r.Create(ctx, tenant.CreateInput{}); return err }},
		{"create first", func() error { _, _, err := r.CreateFirst(ctx, tenant.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant"); return err }},
		{"count", func() error { _, err := r.Count(ctx); return err }},
		{"update", func() error { _, err := r.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, tenant.TransitionStatusInput{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
