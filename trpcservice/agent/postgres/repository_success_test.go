package postgres

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
)

func TestAgentRepositoryGetDecodesStoredApp(t *testing.T) {
	app, err := agent.NewApp(agent.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", AppKey: "primary", DisplayName: "Primary", Description: "stored app",
	})
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(".*").WithArgs(app.TenantID, app.AppID).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "app_key", "display_name", "description", "status", "current_revision", "canary_revision", "version", "created_at", "updated_at",
	}).AddRow(
		app.TenantID, app.AppID, app.AppKey, app.DisplayName, app.Description, string(app.Status), nil, nil, app.Version, app.CreatedAt, app.UpdatedAt,
	))

	stored, err := NewRepository(db).Get(context.Background(), app.TenantID, app.AppID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.AppID != app.AppID || stored.CurrentRevision != nil || stored.Status != agent.StatusDraft {
		t.Fatalf("stored agent app = %+v", stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryGetRevisionDecodesStoredDraft(t *testing.T) {
	app := newStoredAgentApp(t)
	draft := newStoredAgentRevision(t, app, 1, false)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	expectAgentRevision(t, mock, draft)

	stored, err := NewRepository(db).GetRevision(context.Background(), app.TenantID, app.AppID, draft.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Revision != draft.Revision || stored.State != agent.RevisionStateDraft || stored.Instruction != draft.Instruction {
		t.Fatalf("stored draft = %+v", stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryMapsMissingRevisionToNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(".*").WithArgs("tenant", "app", int64(1)).WillReturnError(sql.ErrNoRows)

	_, err = NewRepository(db).GetRevision(context.Background(), "tenant", "app", 1)
	if !errors.Is(err, agent.ErrNotFound) {
		t.Fatalf("missing revision error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryRejectsInvalidInputsBeforeTransactions(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	repository := NewRepository(db)
	ctx := context.Background()
	if _, err := repository.Create(ctx, agent.CreateInput{}); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("Create invalid input error = %v", err)
	}
	if _, _, _, err := repository.Publish(ctx, agent.PublishInput{}); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("Publish invalid metadata error = %v", err)
	}
	if _, _, err := repository.Rollback(ctx, agent.RollbackInput{}); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("Rollback invalid metadata error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, agent.TransitionStatusInput{}); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("TransitionStatus invalid metadata error = %v", err)
	}
	if _, _, err := repository.SetCanary(ctx, agent.SetCanaryInput{}); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("SetCanary invalid metadata error = %v", err)
	}
	if _, _, err := repository.SetCanary(ctx, agent.SetCanaryInput{Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "canary"}}); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("SetCanary inactive tenant error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := repository.SetCanary(canceled, agent.SetCanaryInput{Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "canary"}, TenantActive: true}); !errors.Is(err, context.Canceled) {
		t.Fatalf("SetCanary canceled context error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryRejectsInvalidMetadataUpdate(t *testing.T) {
	current := newStoredAgentApp(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, current)
	mock.ExpectRollback()

	_, err = NewRepository(db).UpdateMetadata(context.Background(), agent.UpdateMetadataInput{
		TenantID: current.TenantID, AppID: current.AppID, ExpectedVersion: current.Version, DisplayName: " ", Description: current.Description,
	})
	if !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("invalid metadata update error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryCreatesApp(t *testing.T) {
	input := agent.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", AppKey: "created", DisplayName: "Created", Description: "created app",
	}
	stored := newStoredAgentApp(t)
	stored.AppKey = input.AppKey
	stored.DisplayName = input.DisplayName
	stored.Description = input.Description

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(".*").WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg()).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "app_key", "display_name", "description", "status", "current_revision", "canary_revision", "version", "created_at", "updated_at",
	}).AddRow(stored.TenantID, stored.AppID, stored.AppKey, stored.DisplayName, stored.Description, string(stored.Status), nil, nil, stored.Version, stored.CreatedAt, stored.UpdatedAt))
	mock.ExpectCommit()

	value, err := NewRepository(db).Create(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if value.AppKey != input.AppKey || value.DisplayName != input.DisplayName || value.Status != agent.StatusDraft {
		t.Fatalf("created app = %+v", value)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryUpdatesMetadata(t *testing.T) {
	current := newStoredAgentApp(t)
	stored := current.Clone()
	stored.DisplayName = "Updated workflow"
	stored.Description = "updated metadata"
	stored.Version++
	stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, current)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	expectAgentApp(mock, &stored)
	mock.ExpectCommit()

	value, err := NewRepository(db).UpdateMetadata(context.Background(), agent.UpdateMetadataInput{
		TenantID: current.TenantID, AppID: current.AppID, ExpectedVersion: current.Version,
		DisplayName: stored.DisplayName, Description: stored.Description,
	})
	if err != nil {
		t.Fatal(err)
	}
	if value.DisplayName != stored.DisplayName || value.Description != stored.Description || value.Version != stored.Version {
		t.Fatalf("updated app = %+v", value)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryCreatesAndUpdatesDraft(t *testing.T) {
	app := newStoredAgentApp(t)
	draft := newStoredAgentRevision(t, app, 1, false)

	createDB, createMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = createDB.Close() })
	createMock.ExpectBegin()
	expectAgentApp(createMock, app)
	createMock.ExpectQuery(".*").WithArgs(app.TenantID, app.AppID).WillReturnRows(sqlmock.NewRows([]string{"next_revision"}).AddRow(draft.Revision))
	createMock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	expectAgentRevision(t, createMock, draft)
	createMock.ExpectCommit()

	created, err := NewRepository(createDB).CreateDraft(context.Background(), agent.CreateDraftInput{
		TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Kind: draft.Kind, SchemaVersion: draft.SchemaVersion,
		Configuration: agent.DraftConfiguration{Instruction: draft.Instruction, ModelProfileID: draft.ModelProfileID, Runtime: draft.Runtime},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Revision != draft.Revision || created.State != agent.RevisionStateDraft {
		t.Fatalf("created draft = %+v", created)
	}
	if err := createMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}

	updated, err := agent.NewRevision(agent.CreateRevisionInput{
		TenantID: app.TenantID, AppID: app.AppID, Revision: draft.Revision, Kind: draft.Kind, SchemaVersion: draft.SchemaVersion,
		Configuration: agent.DraftConfiguration{Instruction: "Updated instruction", ModelProfileID: draft.ModelProfileID, Runtime: draft.Runtime},
	})
	if err != nil {
		t.Fatal(err)
	}
	updated.DraftVersion = draft.DraftVersion + 1
	updated.CreatedAt = draft.CreatedAt
	updated.UpdatedAt = draft.UpdatedAt.Add(time.Second)

	updateDB, updateMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = updateDB.Close() })
	updateMock.ExpectBegin()
	expectAgentApp(updateMock, app)
	expectAgentRevision(t, updateMock, draft)
	updateMock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	expectAgentRevision(t, updateMock, updated)
	updateMock.ExpectCommit()

	value, err := NewRepository(updateDB).UpdateDraft(context.Background(), agent.UpdateDraftInput{
		TenantID: app.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion,
		Configuration: agent.DraftConfiguration{Instruction: updated.Instruction, ModelProfileID: updated.ModelProfileID, Runtime: updated.Runtime},
	})
	if err != nil {
		t.Fatal(err)
	}
	if value.Instruction != updated.Instruction || value.DraftVersion != updated.DraftVersion {
		t.Fatalf("updated draft = %+v", value)
	}
	if err := updateMock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryTransitionsStatus(t *testing.T) {
	current := newStoredAgentApp(t)
	published := newStoredAgentRevision(t, current, 1, true)
	current.Status = agent.StatusActive
	current.CurrentRevision = agentInt64(published.Revision)
	current.Version = 2
	current.UpdatedAt = published.UpdatedAt.Add(time.Second)
	stored := current.Clone()
	stored.Status = agent.StatusSuspended
	stored.Version++
	stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, current)
	expectAgentRevision(t, mock, published)
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(9)))
	expectAgentApp(mock, &stored)
	expectAgentEvent(mock, current, agent.ChangeSuspended, agent.StatusActive, agent.StatusSuspended, current.CurrentRevision, stored.CurrentRevision, published.ContentDigest, current.Version, stored.Version, stored.UpdatedAt)
	mock.ExpectCommit()

	value, event, err := NewRepository(db).TransitionStatus(context.Background(), agent.TransitionStatusInput{
		TenantID: current.TenantID, AppID: current.AppID, ExpectedVersion: current.Version, NextStatus: agent.StatusSuspended,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "suspend", CorrelationID: "agent-suspend"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if value.Status != agent.StatusSuspended || value.Version != stored.Version || event.EventType != agent.ChangeSuspended || event.ContentDigest != published.ContentDigest {
		t.Fatalf("transition result = app=%+v event=%+v", value, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestScanAgentEventDecodesOptionalFields(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	occurredAt := time.Date(2026, 8, 24, 12, 30, 0, 0, time.FixedZone("test", 3600))
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "app_id", "previous_status", "current_status", "previous_revision", "current_revision",
		"content_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow(
		"published", "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", "app_01ARZ3NDEKTSV4RRFFQ69G5FAW", "draft", "active", int64(1), int64(2),
		"digest", "operator", "user-1", "publish", "correlation-1", int64(3), int64(4), occurredAt,
	))

	event, err := scanAgentEvent(db.QueryRowContext(context.Background(), "SELECT 1"))
	if err != nil {
		t.Fatal(err)
	}
	if event.PreviousStatus != agent.StatusDraft || event.CurrentStatus != agent.StatusActive || event.PreviousRevision == nil || *event.PreviousRevision != 1 || event.CurrentRevision == nil || *event.CurrentRevision != 2 || event.ContentDigest != "digest" || event.OccurredAt.Location() != time.UTC {
		t.Fatalf("decoded agent event = %+v", event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryPublishesDraftAndMovesCurrentRevision(t *testing.T) {
	app := newStoredAgentApp(t)
	draft := newStoredAgentRevision(t, app, 1, false)
	publishedValue, err := draft.Publish(draft.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	storedApp := app.Clone()
	storedApp.Status = agent.StatusActive
	storedApp.CurrentRevision = agentInt64(draft.Revision)
	storedApp.Version++
	storedApp.UpdatedAt = publishedValue.UpdatedAt

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WithArgs(app.TenantID).WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	expectAgentApp(mock, app)
	expectAgentRevision(t, mock, draft)
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(7)))
	expectAgentApp(mock, &storedApp)
	expectAgentRevision(t, mock, &publishedValue)
	expectAgentEvent(mock, app, agent.ChangePublished, agent.StatusDraft, agent.StatusActive, nil, storedApp.CurrentRevision, publishedValue.ContentDigest, app.Version, storedApp.Version, publishedValue.UpdatedAt)
	mock.ExpectCommit()

	storedRevisionApp, storedRevision, event, err := NewRepository(db).Publish(context.Background(), agent.PublishInput{
		TenantID: app.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "publish", CorrelationID: "agent-publish"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if storedRevisionApp.Status != agent.StatusActive || storedRevisionApp.CurrentRevision == nil || *storedRevisionApp.CurrentRevision != draft.Revision || storedRevision.State != agent.RevisionStatePublished || event.EventType != agent.ChangePublished {
		t.Fatalf("publish result = app=%+v revision=%+v event=%+v", storedRevisionApp, storedRevision, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryRollsBackToPublishedRevision(t *testing.T) {
	app := newStoredAgentApp(t)
	target := newStoredAgentRevision(t, app, 1, true)
	currentRevision := int64(2)
	app.Status = agent.StatusActive
	app.CurrentRevision = &currentRevision
	app.Version = 3
	app.UpdatedAt = target.UpdatedAt.Add(time.Second)
	stored := app.Clone()
	stored.CurrentRevision = agentInt64(target.Revision)
	stored.Version++
	stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, app)
	expectAgentRevision(t, mock, target)
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(8)))
	expectAgentApp(mock, &stored)
	expectAgentEvent(mock, app, agent.ChangeRolledBack, agent.StatusActive, agent.StatusActive, app.CurrentRevision, stored.CurrentRevision, target.ContentDigest, app.Version, stored.Version, stored.UpdatedAt)
	mock.ExpectCommit()

	result, event, err := NewRepository(db).Rollback(context.Background(), agent.RollbackInput{
		TenantID: app.TenantID, AppID: app.AppID, TargetRevision: target.Revision, ExpectedAppVersion: app.Version,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "rollback", CorrelationID: "agent-rollback"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CurrentRevision == nil || *result.CurrentRevision != target.Revision || result.Version != stored.Version || event.EventType != agent.ChangeRolledBack {
		t.Fatalf("rollback result = app=%+v event=%+v", result, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositorySetsCanaryRevision(t *testing.T) {
	app := newStoredAgentApp(t)
	stable := newStoredAgentRevision(t, app, 1, true)
	candidate := newStoredAgentRevision(t, app, 2, true)
	app.Status = agent.StatusActive
	app.CurrentRevision = agentInt64(stable.Revision)
	app.Version = 2
	stored := app.Clone()
	stored.CanaryRevision = agentInt64(candidate.Revision)
	stored.Version++
	stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, app)
	expectAgentRevision(t, mock, candidate)
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(10)))
	expectAgentApp(mock, &stored)
	expectAgentEvent(mock, app, agent.ChangeCanaryStarted, agent.StatusActive, agent.StatusActive, nil, stored.CanaryRevision, candidate.ContentDigest, app.Version, stored.Version, stored.UpdatedAt)
	mock.ExpectCommit()
	selected, event, err := NewRepository(db).SetCanary(context.Background(), agent.SetCanaryInput{
		TenantID: app.TenantID, AppID: app.AppID, CandidateRevision: stored.CanaryRevision,
		ExpectedAppVersion: app.Version, TenantActive: true,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "canary", CorrelationID: "agent-canary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if selected.CanaryRevision == nil || *selected.CanaryRevision != candidate.Revision || event.EventType != agent.ChangeCanaryStarted {
		t.Fatalf("canary result = app=%+v event=%+v", selected, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryClearsCanaryRevision(t *testing.T) {
	app := newStoredAgentApp(t)
	app.Status = agent.StatusActive
	app.CurrentRevision = agentInt64(1)
	app.CanaryRevision = agentInt64(2)
	app.Version = 3
	stored := app.Clone()
	stored.CanaryRevision = nil
	stored.Version++
	stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, app)
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(12)))
	expectAgentApp(mock, &stored)
	expectAgentEvent(mock, app, agent.ChangeCanaryStopped, agent.StatusActive, agent.StatusActive, app.CanaryRevision, nil, "", app.Version, stored.Version, stored.UpdatedAt)
	mock.ExpectCommit()
	cleared, event, err := NewRepository(db).SetCanary(context.Background(), agent.SetCanaryInput{
		TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, TenantActive: true,
		Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cleared.CanaryRevision != nil || event.EventType != agent.ChangeCanaryStopped || event.PreviousRevision == nil || *event.PreviousRevision != 2 || event.CurrentRevision != nil {
		t.Fatalf("clear result = app=%+v event=%+v", cleared, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositorySetCanaryRejectsTransactionStates(t *testing.T) {
	cases := []struct {
		name      string
		app       func(*agent.App)
		candidate *int64
		prepare   func(sqlmock.Sqlmock, *agent.App)
		wantError error
	}{
		{name: "missing app", prepare: func(mock sqlmock.Sqlmock, _ *agent.App) { mock.ExpectQuery(".*").WillReturnError(sql.ErrNoRows) }, wantError: agent.ErrNotFound},
		{name: "inactive app", app: func(app *agent.App) { app.Status = agent.StatusDraft; app.CurrentRevision = nil }, wantError: agent.ErrInvalid},
		{name: "zero candidate", candidate: agentInt64(0), wantError: agent.ErrInvalid},
		{name: "stable candidate", candidate: agentInt64(1), wantError: agent.ErrInvalid},
		{name: "version conflict", wantError: agent.ErrConflict},
		{name: "unchanged candidate", app: func(app *agent.App) { app.CanaryRevision = agentInt64(2) }, candidate: agentInt64(2), wantError: agent.ErrInvalid},
		{name: "missing candidate", candidate: agentInt64(2), prepare: func(mock sqlmock.Sqlmock, app *agent.App) {
			mock.ExpectQuery(".*").WithArgs(app.TenantID, app.AppID, int64(2)).WillReturnError(sql.ErrNoRows)
		}, wantError: agent.ErrNotFound},
		{name: "draft candidate", candidate: agentInt64(2), prepare: func(mock sqlmock.Sqlmock, app *agent.App) {
			expectAgentRevision(t, mock, newStoredAgentRevision(t, app, 2, false))
		}, wantError: agent.ErrInvalid},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := newStoredAgentApp(t)
			app.Status = agent.StatusActive
			app.CurrentRevision = agentInt64(1)
			app.Version = 2
			if tc.app != nil {
				tc.app(app)
			}
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			if tc.name == "missing app" {
				tc.prepare(mock, app)
			} else {
				expectAgentApp(mock, app)
				if tc.prepare != nil {
					tc.prepare(mock, app)
				}
			}
			mock.ExpectRollback()
			expectedVersion := app.Version
			if tc.name == "version conflict" {
				expectedVersion++
			}
			_, _, err = NewRepository(db).SetCanary(context.Background(), agent.SetCanaryInput{
				TenantID: app.TenantID, AppID: app.AppID, CandidateRevision: tc.candidate, ExpectedAppVersion: expectedVersion, TenantActive: true,
				Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"},
			})
			if !errors.Is(err, tc.wantError) {
				t.Fatalf("SetCanary error = %v, want %v", err, tc.wantError)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAgentRepositorySetCanaryMapsPersistenceErrors(t *testing.T) {
	cases := []struct {
		name  string
		setup func(*agent.App, sqlmock.Sqlmock, *agent.Revision)
	}{
		{name: "event insert", setup: func(app *agent.App, mock sqlmock.Sqlmock, candidate *agent.Revision) {
			expectAgentApp(mock, app)
			expectAgentRevision(t, mock, candidate)
			mock.ExpectQuery(".*").WillReturnError(errors.New("event insert"))
		}},
		{name: "stored app", setup: func(app *agent.App, mock sqlmock.Sqlmock, candidate *agent.Revision) {
			expectAgentApp(mock, app)
			expectAgentRevision(t, mock, candidate)
			mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(11)))
			mock.ExpectQuery(".*").WillReturnError(errors.New("stored app"))
		}},
		{name: "event decode", setup: func(app *agent.App, mock sqlmock.Sqlmock, candidate *agent.Revision) {
			expectAgentApp(mock, app)
			expectAgentRevision(t, mock, candidate)
			mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(11)))
			stored := app.Clone()
			stored.CanaryRevision = agentInt64(candidate.Revision)
			stored.Version++
			stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)
			expectAgentApp(mock, &stored)
			mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_type"}).AddRow("canary_started"))
		}},
		{name: "commit", setup: func(app *agent.App, mock sqlmock.Sqlmock, candidate *agent.Revision) {
			expectAgentApp(mock, app)
			expectAgentRevision(t, mock, candidate)
			mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{"event_id"}).AddRow(int64(11)))
			stored := app.Clone()
			stored.CanaryRevision = agentInt64(candidate.Revision)
			stored.Version++
			stored.UpdatedAt = stored.UpdatedAt.Add(time.Second)
			expectAgentApp(mock, &stored)
			expectAgentEvent(mock, app, agent.ChangeCanaryStarted, agent.StatusActive, agent.StatusActive, nil, stored.CanaryRevision, candidate.ContentDigest, app.Version, stored.Version, stored.UpdatedAt)
			mock.ExpectCommit().WillReturnError(errors.New("commit"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := newStoredAgentApp(t)
			app.Status = agent.StatusActive
			app.CurrentRevision = agentInt64(1)
			app.Version = 2
			candidate := newStoredAgentRevision(t, app, 2, true)
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectBegin()
			tc.setup(app, mock, candidate)
			if tc.name != "commit" {
				mock.ExpectRollback()
			}
			_, _, err = NewRepository(db).SetCanary(context.Background(), agent.SetCanaryInput{TenantID: app.TenantID, AppID: app.AppID, CandidateRevision: agentInt64(candidate.Revision), ExpectedAppVersion: app.Version, TenantActive: true, Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "workflow", CorrelationID: "correlation"}})
			if err == nil {
				t.Fatal("SetCanary unexpectedly succeeded")
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAgentRepositoryRequiresStorage(t *testing.T) {
	repository := NewRepository(nil)
	ctx := context.Background()
	if _, err := repository.Create(ctx, agent.CreateInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create nil-storage error = %v", err)
	}
	if _, err := repository.Get(ctx, "tenant", "app"); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get nil-storage error = %v", err)
	}
	if _, err := repository.UpdateMetadata(ctx, agent.UpdateMetadataInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateMetadata nil-storage error = %v", err)
	}
	if _, err := repository.CreateDraft(ctx, agent.CreateDraftInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("CreateDraft nil-storage error = %v", err)
	}
	if _, err := repository.UpdateDraft(ctx, agent.UpdateDraftInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateDraft nil-storage error = %v", err)
	}
	if _, err := repository.GetRevision(ctx, "tenant", "app", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("GetRevision nil-storage error = %v", err)
	}
	if _, _, _, err := repository.Publish(ctx, agent.PublishInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Publish nil-storage error = %v", err)
	}
	if _, _, err := repository.Rollback(ctx, agent.RollbackInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Rollback nil-storage error = %v", err)
	}
	if _, _, err := repository.TransitionStatus(ctx, agent.TransitionStatusInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("TransitionStatus nil-storage error = %v", err)
	}
	if _, _, err := repository.SetCanary(ctx, agent.SetCanaryInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("SetCanary nil-storage error = %v", err)
	}
}

func newStoredAgentApp(t *testing.T) *agent.App {
	t.Helper()
	app, err := agent.NewApp(agent.CreateInput{
		TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", AppKey: "workflow", DisplayName: "Workflow", Description: "stored app",
	})
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func newStoredAgentRevision(t *testing.T, app *agent.App, revision int64, published bool) *agent.Revision {
	t.Helper()
	value, err := agent.NewRevision(agent.CreateRevisionInput{
		TenantID: app.TenantID, AppID: app.AppID, Revision: revision, Kind: agent.KindLLM, SchemaVersion: agent.SchemaVersionV1,
		Configuration: agent.DraftConfiguration{Instruction: "Answer", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: agent.DefaultRuntimePolicy()},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !published {
		return value
	}
	publishedValue, err := value.Publish(value.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	return &publishedValue
}

func expectAgentApp(mock sqlmock.Sqlmock, value *agent.App) {
	var currentRevision, canaryRevision any
	if value.CurrentRevision != nil {
		currentRevision = *value.CurrentRevision
	}
	if value.CanaryRevision != nil {
		canaryRevision = *value.CanaryRevision
	}
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "app_key", "display_name", "description", "status", "current_revision", "canary_revision", "version", "created_at", "updated_at",
	}).AddRow(value.TenantID, value.AppID, value.AppKey, value.DisplayName, value.Description, string(value.Status), currentRevision, canaryRevision, value.Version, value.CreatedAt, value.UpdatedAt))
}

func expectAgentRevision(t *testing.T, mock sqlmock.Sqlmock, value *agent.Revision) {
	t.Helper()
	generation, runtime, _, err := encodeAgentRevisionParts(*value)
	if err != nil {
		t.Fatal(err)
	}
	var digest, publishedAt any
	if value.ContentDigest != "" {
		digest = value.ContentDigest
	}
	if value.PublishedAt != nil {
		publishedAt = *value.PublishedAt
	}
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID, value.Revision).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "revision", "state", "draft_version", "agent_kind", "schema_version", "description", "instruction", "global_instruction", "model_profile_id", "generation_config", "runtime_policy", "content_digest", "published_at", "created_at", "updated_at",
	}).AddRow(value.TenantID, value.AppID, value.Revision, string(value.State), value.DraftVersion, string(value.Kind), value.SchemaVersion, value.Description, value.Instruction, value.GlobalInstruction, value.ModelProfileID, generation, runtime, digest, publishedAt, value.CreatedAt, value.UpdatedAt))
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID, value.Revision).WillReturnRows(sqlmock.NewRows([]string{"tool_id", "required"}))
}

func expectAgentEvent(mock sqlmock.Sqlmock, app *agent.App, eventType agent.ChangeEventType, previousStatus, currentStatus agent.Status, previousRevision, currentRevision *int64, digest string, previousVersion, nextVersion int64, occurredAt time.Time) {
	var previousStatusValue, previousRevisionValue, currentRevisionValue, digestValue any
	if previousStatus != "" {
		previousStatusValue = string(previousStatus)
	}
	if previousRevision != nil {
		previousRevisionValue = *previousRevision
	}
	if currentRevision != nil {
		currentRevisionValue = *currentRevision
	}
	if digest != "" {
		digestValue = digest
	}
	mock.ExpectQuery(".*").WillReturnRows(sqlmock.NewRows([]string{
		"event_type", "tenant_id", "app_id", "previous_status", "current_status", "previous_revision", "current_revision", "content_digest", "actor_type", "actor_id", "reason", "correlation_id", "previous_version", "next_version", "occurred_at",
	}).AddRow(string(eventType), app.TenantID, app.AppID, previousStatusValue, string(currentStatus), previousRevisionValue, currentRevisionValue, digestValue, "test", "user", "workflow", "correlation", previousVersion, nextVersion, occurredAt))
}
