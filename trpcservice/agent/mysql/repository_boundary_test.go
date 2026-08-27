package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
)

func TestAgentRepositoryRejectsCancelledContextsBeforeStorage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := NewRepository(nil)
	cases := []struct {
		name string
		call func() error
	}{
		{"create", func() error { _, err := r.Create(ctx, agent.CreateInput{}); return err }},
		{"get", func() error { _, err := r.Get(ctx, "tenant", "app"); return err }},
		{"update metadata", func() error { _, err := r.UpdateMetadata(ctx, agent.UpdateMetadataInput{}); return err }},
		{"create draft", func() error { _, err := r.CreateDraft(ctx, agent.CreateDraftInput{}); return err }},
		{"update draft", func() error { _, err := r.UpdateDraft(ctx, agent.UpdateDraftInput{}); return err }},
		{"get revision", func() error { _, err := r.GetRevision(ctx, "tenant", "app", 1); return err }},
		{"publish", func() error { _, _, _, err := r.Publish(ctx, agent.PublishInput{}); return err }},
		{"set canary", func() error { _, _, err := r.SetCanary(ctx, agent.SetCanaryInput{}); return err }},
		{"rollback", func() error { _, _, err := r.Rollback(ctx, agent.RollbackInput{}); return err }},
		{"transition", func() error { _, _, err := r.TransitionStatus(ctx, agent.TransitionStatusInput{}); return err }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}
