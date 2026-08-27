package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
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

func TestSameAgentRevisionHandlesNilAndValuePairs(t *testing.T) {
	value := int64(7)
	other := int64(8)
	for _, tc := range []struct {
		name        string
		left, right *int64
		want        bool
	}{
		{"both nil", nil, nil, true},
		{"left nil", nil, &value, false},
		{"right nil", &value, nil, false},
		{"equal", &value, &value, true},
		{"different", &value, &other, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sameAgentRevision(tc.left, tc.right); got != tc.want {
				t.Fatalf("sameAgentRevision() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestMaxTimeChoosesLatestTimestamp(t *testing.T) {
	first := time.Unix(10, 0).UTC()
	second := time.Unix(20, 0).UTC()
	if got := maxTime(first, second); !got.Equal(second) {
		t.Fatalf("maxTime(first, second) = %v, want second", got)
	}
	if got := maxTime(second, first); !got.Equal(second) {
		t.Fatalf("maxTime(second, first) = %v, want second", got)
	}
}

func TestReplaceRevisionToolsClearsEmptySet(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := replaceRevisionTools(context.Background(), db, agent.Revision{TenantID: "tenant", AppID: "app", Revision: 1}); err != nil {
		t.Fatalf("replace empty tools = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSetCanaryRejectsInactiveTenantBeforeTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	_, _, err = NewRepository(db).SetCanary(context.Background(), agent.SetCanaryInput{
		TenantID: "tenant", AppID: "app", TenantActive: false,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "inactive", CorrelationID: "inactive-tenant"},
	})
	if !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("inactive tenant error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
