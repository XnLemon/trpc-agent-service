package mysql

import (
	"context"
	"database/sql"
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

func TestSetCanaryGuardsStateAndCandidateBoundaries(t *testing.T) {
	base := newStoredAgentApp(t)
	currentRevision := int64(1)
	base.Status, base.CurrentRevision, base.Version = agent.StatusActive, &currentRevision, 2
	metadata := agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "guard", CorrelationID: "canary-guard"}
	tests := []struct {
		name      string
		app       *agent.App
		candidate *int64
		setup     func(sqlmock.Sqlmock, *agent.App)
		want      error
	}{
		{"disabled app", func() *agent.App { v := base.Clone(); v.Status = agent.StatusSuspended; return &v }(), agentInt64(2), func(m sqlmock.Sqlmock, a *agent.App) { m.ExpectBegin(); expectAgentApp(m, a); m.ExpectRollback() }, agent.ErrInvalid},
		{"unchanged canary", func() *agent.App { v := base.Clone(); return &v }(), nil, func(m sqlmock.Sqlmock, a *agent.App) { m.ExpectBegin(); expectAgentApp(m, a); m.ExpectRollback() }, agent.ErrInvalid},
		{"current revision candidate", func() *agent.App { v := base.Clone(); return &v }(), &currentRevision, func(m sqlmock.Sqlmock, a *agent.App) { m.ExpectBegin(); expectAgentApp(m, a); m.ExpectRollback() }, agent.ErrInvalid},
		{"candidate load error", func() *agent.App { v := base.Clone(); return &v }(), agentInt64(2), func(m sqlmock.Sqlmock, a *agent.App) {
			m.ExpectBegin()
			expectAgentApp(m, a)
			m.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("candidate"))
			m.ExpectRollback()
		}, ErrStorage},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock, tc.app)
			_, _, err = NewRepository(db).SetCanary(context.Background(), agent.SetCanaryInput{TenantID: tc.app.TenantID, AppID: tc.app.AppID, CandidateRevision: tc.candidate, ExpectedAppVersion: tc.app.Version, TenantActive: true, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRollbackRejectsUnpublishedAndUnchangedTargets(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(2)
	app.Status, app.CurrentRevision, app.Version = agent.StatusActive, &currentRevision, 3
	metadata := agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "guard", CorrelationID: "rollback-guard"}
	for _, tc := range []struct {
		name           string
		target         *agent.Revision
		targetRevision int64
		want           error
	}{
		{"unpublished", newStoredAgentRevision(t, app, 1, false), 1, agent.ErrInvalid},
		{"unchanged", newStoredAgentRevision(t, app, 2, true), 2, agent.ErrInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			expectAgentApp(mock, app)
			expectAgentRevision(t, mock, tc.target)
			mock.ExpectRollback()
			_, _, err = NewRepository(db).Rollback(context.Background(), agent.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: tc.targetRevision, ExpectedAppVersion: app.Version, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSetCanaryPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(1)
	app.Status, app.CurrentRevision, app.Version = agent.StatusActive, &currentRevision, 2
	candidate := newStoredAgentRevision(t, app, 2, true)
	updated := app.Clone()
	updated.CanaryRevision = agentInt64(2)
	updated.Version++
	metadata := agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"}
	common := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		expectAgentRevision(t, m, candidate)
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{"update error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnError(errors.New("update"))
			m.ExpectRollback()
		}, ErrStorage},
		{"update conflict", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectRollback()
		}, agent.ErrConflict},
		{"event insert error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnError(errors.New("event"))
			m.ExpectRollback()
		}, ErrStorage},
		{"readback error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}, ErrStorage},
		{"event scan error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
			expectAgentApp(m, &updated)
			m.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("bad"))
			m.ExpectRollback()
		}, ErrStorage},
		{"commit error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET canary_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(7, 1))
			expectAgentApp(m, &updated)
			expectAgentEvent(m, &updated, agent.ChangeCanaryStarted, agent.StatusActive, agent.StatusActive, nil, updated.CanaryRevision, candidate.ContentDigest, app.Version, updated.Version, updated.UpdatedAt)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}, ErrStorage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock)
			_, _, err = NewRepository(db).SetCanary(context.Background(), agent.SetCanaryInput{TenantID: app.TenantID, AppID: app.AppID, CandidateRevision: agentInt64(2), ExpectedAppVersion: app.Version, TenantActive: true, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRollbackPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(2)
	app.Status, app.CurrentRevision, app.Version = agent.StatusActive, &currentRevision, 3
	target := newStoredAgentRevision(t, app, 1, true)
	updated := app.Clone()
	updated.CurrentRevision = agentInt64(1)
	updated.CanaryRevision = nil
	updated.Version++
	metadata := agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"}
	common := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		expectAgentRevision(t, m, target)
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{"update error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnError(errors.New("update"))
			m.ExpectRollback()
		}, ErrStorage},
		{"update conflict", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectRollback()
		}, agent.ErrConflict},
		{"event insert error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnError(errors.New("event"))
			m.ExpectRollback()
		}, ErrStorage},
		{"readback error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(8, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}, ErrStorage},
		{"event scan error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(8, 1))
			expectAgentApp(m, &updated)
			m.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("bad"))
			m.ExpectRollback()
		}, ErrStorage},
		{"commit error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET current_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(8, 1))
			expectAgentApp(m, &updated)
			expectAgentEvent(m, &updated, agent.ChangeRolledBack, agent.StatusActive, agent.StatusActive, app.CurrentRevision, updated.CurrentRevision, target.ContentDigest, app.Version, updated.Version, updated.UpdatedAt)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}, ErrStorage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock)
			_, _, err = NewRepository(db).Rollback(context.Background(), agent.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: 1, ExpectedAppVersion: app.Version, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTransitionStatusPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	currentRevision := int64(1)
	app.Status, app.CurrentRevision, app.Version = agent.StatusActive, &currentRevision, 2
	revision := newStoredAgentRevision(t, app, 1, true)
	updated := app.Clone()
	updated.Status = agent.StatusSuspended
	updated.Version++
	metadata := agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"}
	common := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		expectAgentRevision(t, m, revision)
	}
	cases := []struct {
		name  string
		setup func(sqlmock.Sqlmock)
		want  error
	}{
		{"update error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnError(errors.New("update"))
			m.ExpectRollback()
		}, ErrStorage},
		{"update conflict", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 0))
			m.ExpectRollback()
		}, agent.ErrConflict},
		{"event insert error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnError(errors.New("event"))
			m.ExpectRollback()
		}, ErrStorage},
		{"event id error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewErrorResult(errors.New("last id")))
			m.ExpectRollback()
		}, ErrStorage},
		{"readback error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(9, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}, ErrStorage},
		{"event scan error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(9, 1))
			expectAgentApp(m, &updated)
			m.ExpectQuery("SELECT event_type").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("bad"))
			m.ExpectRollback()
		}, ErrStorage},
		{"commit error", func(m sqlmock.Sqlmock) {
			common(m)
			m.ExpectExec("UPDATE agent_app SET status").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("INSERT INTO agent_app_change_outbox").WillReturnResult(sqlmock.NewResult(9, 1))
			expectAgentApp(m, &updated)
			expectAgentEvent(m, &updated, agent.ChangeSuspended, agent.StatusActive, agent.StatusSuspended, app.CurrentRevision, updated.CurrentRevision, revision.ContentDigest, app.Version, updated.Version, updated.UpdatedAt)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}, ErrStorage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			tc.setup(mock)
			_, _, err = NewRepository(db).TransitionStatus(context.Background(), agent.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: agent.StatusSuspended, Metadata: metadata})
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDraftPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	input := agent.CreateDraftInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Kind: agent.KindLLM, SchemaVersion: agent.SchemaVersionV1, Configuration: agent.DraftConfiguration{Instruction: "draft", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: agent.DefaultRuntimePolicy()}}
	draft := newStoredAgentRevision(t, app, 1, false)
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock
	}
	createPrefix := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		m.ExpectQuery("COALESCE\\(MAX\\(revision\\)").WillReturnRows(sqlmock.NewRows([]string{"next_revision"}).AddRow(1))
	}
	for _, tc := range []struct {
		name  string
		setup func(sqlmock.Sqlmock)
	}{
		{"insert", func(m sqlmock.Sqlmock) {
			createPrefix(m)
			m.ExpectExec("INSERT INTO agent_app_revision").WillReturnError(errors.New("insert"))
			m.ExpectRollback()
		}},
		{"replace tools", func(m sqlmock.Sqlmock) {
			createPrefix(m)
			m.ExpectExec("INSERT INTO agent_app_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnError(errors.New("tools"))
			m.ExpectRollback()
		}},
		{"readback", func(m sqlmock.Sqlmock) {
			createPrefix(m)
			m.ExpectExec("INSERT INTO agent_app_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}},
		{"commit", func(m sqlmock.Sqlmock) {
			createPrefix(m)
			m.ExpectExec("INSERT INTO agent_app_revision").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
			expectAgentRevision(t, m, draft)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}},
	} {
		t.Run("create "+tc.name, func(t *testing.T) {
			db, mock := newDB(t)
			tc.setup(mock)
			if _, err := NewRepository(db).CreateDraft(context.Background(), input); !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
	current := newStoredAgentRevision(t, app, 1, false)
	updateInput := agent.UpdateDraftInput{TenantID: app.TenantID, AppID: app.AppID, Revision: 1, ExpectedAppVersion: app.Version, ExpectedDraftVersion: current.DraftVersion, Configuration: current.Configuration()}
	updated := current.Clone()
	updated.DraftVersion++
	updated.UpdatedAt = current.UpdatedAt.Add(time.Second)
	updatePrefix := func(m sqlmock.Sqlmock) {
		m.ExpectBegin()
		expectAgentApp(m, app)
		expectAgentRevision(t, m, current)
	}
	for _, tc := range []struct {
		name  string
		setup func(sqlmock.Sqlmock)
	}{
		{"update", func(m sqlmock.Sqlmock) {
			updatePrefix(m)
			m.ExpectExec("UPDATE agent_app_revision SET").WillReturnError(errors.New("update"))
			m.ExpectRollback()
		}},
		{"replace tools", func(m sqlmock.Sqlmock) {
			updatePrefix(m)
			m.ExpectExec("UPDATE agent_app_revision SET").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnError(errors.New("tools"))
			m.ExpectRollback()
		}},
		{"readback", func(m sqlmock.Sqlmock) {
			updatePrefix(m)
			m.ExpectExec("UPDATE agent_app_revision SET").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("readback"))
			m.ExpectRollback()
		}},
		{"commit", func(m sqlmock.Sqlmock) {
			updatePrefix(m)
			m.ExpectExec("UPDATE agent_app_revision SET").WillReturnResult(sqlmock.NewResult(0, 1))
			m.ExpectExec("DELETE FROM agent_app_revision_tool").WillReturnResult(sqlmock.NewResult(0, 1))
			expectAgentRevision(t, m, &updated)
			m.ExpectCommit().WillReturnError(errors.New("commit"))
		}},
	} {
		t.Run("update "+tc.name, func(t *testing.T) {
			db, mock := newDB(t)
			tc.setup(mock)
			if _, err := NewRepository(db).UpdateDraft(context.Background(), updateInput); !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v", err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
