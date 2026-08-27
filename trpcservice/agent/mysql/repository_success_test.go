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
		"tenant_id", "app_id", "app_key", "display_name", "description", "status", "current_revision", "version", "created_at", "updated_at",
	}).AddRow(
		app.TenantID, app.AppID, app.AppKey, app.DisplayName, app.Description, string(app.Status), nil, app.Version, app.CreatedAt, app.UpdatedAt,
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
	draft.Tools = []agent.ToolAuthorization{{ToolID: "tool", Required: true}}
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
		"tenant_id", "app_id", "app_key", "display_name", "description", "status", "current_revision", "version", "created_at", "updated_at",
	}).AddRow(stored.TenantID, stored.AppID, stored.AppKey, stored.DisplayName, stored.Description, string(stored.Status), nil, stored.Version, stored.CreatedAt, stored.UpdatedAt))
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(9, 1))
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(7, 1))
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
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(8, 1))
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
}

func TestAgentRepositoryBeginAndReadErrors(t *testing.T) {
	newDB := func(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, mock
	}
	metadata := agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "error", CorrelationID: "agent-error"}
	db, mock := newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, err := NewRepository(db).Create(context.Background(), agent.CreateInput{TenantID: "t_01ARZ3NDEKTSV4RRFFQ69G5FAW", AppKey: "error", DisplayName: "Error"}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Create begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, err := NewRepository(db).UpdateMetadata(context.Background(), agent.UpdateMetadataInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateMetadata begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, err := NewRepository(db).CreateDraft(context.Background(), agent.CreateDraftInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("CreateDraft begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, err := NewRepository(db).UpdateDraft(context.Background(), agent.UpdateDraftInput{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("UpdateDraft begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, _, _, err := NewRepository(db).Publish(context.Background(), agent.PublishInput{TenantID: "tenant", AppID: "app", Revision: 1, ExpectedAppVersion: 1, ExpectedDraftVersion: 1, TenantActive: true, Metadata: metadata}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Publish begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, _, err := NewRepository(db).Rollback(context.Background(), agent.RollbackInput{TenantID: "tenant", AppID: "app", TargetRevision: 1, ExpectedAppVersion: 1, Metadata: metadata}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Rollback begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectBegin().WillReturnError(errors.New("begin"))
	if _, _, err := NewRepository(db).TransitionStatus(context.Background(), agent.TransitionStatusInput{TenantID: "tenant", AppID: "app", ExpectedVersion: 1, NextStatus: agent.StatusSuspended, Metadata: metadata}); !errors.Is(err, ErrStorage) {
		t.Fatalf("Transition begin error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectQuery(".*").WillReturnError(errors.New("read"))
	if _, err := NewRepository(db).Get(context.Background(), "tenant", "app"); !errors.Is(err, ErrStorage) {
		t.Fatalf("Get read error = %v", err)
	}
	db, mock = newDB(t)
	mock.ExpectQuery(".*").WillReturnError(errors.New("read"))
	if _, err := NewRepository(db).GetRevision(context.Background(), "tenant", "app", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("GetRevision read error = %v", err)
	}
}

func TestAgentRepositoryPersistenceErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	draft := newStoredAgentRevision(t, app, 1, false)
	published, err := draft.Publish(draft.UpdatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	updatedApp := app.Clone()
	updatedApp.Status = agent.StatusActive
	updatedApp.CurrentRevision = agentInt64(draft.Revision)
	updatedApp.Version++
	updatedApp.UpdatedAt = published.UpdatedAt
	event := agent.ChangeEvent{EventType: agent.ChangePublished, TenantID: app.TenantID, AppID: app.AppID, CurrentStatus: agent.StatusActive, CurrentRevision: agentInt64(draft.Revision), ContentDigest: published.ContentDigest, PreviousVersion: app.Version, NextVersion: updatedApp.Version, OccurredAt: published.UpdatedAt}
	input := agent.PublishInput{TenantID: app.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion}
	newTx := func(t *testing.T) (*sql.DB, *sql.Tx, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectBegin()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		return db, tx, mock
	}
	_, tx, mock := newTx(t)
	mock.ExpectExec(".*").WillReturnError(errors.New("revision update"))
	mock.ExpectRollback()
	if _, err := persistPublishedAgent(context.Background(), tx, input, published, updatedApp, event); !errors.Is(err, ErrStorage) {
		t.Fatalf("published revision error = %v", err)
	}
	_, tx, mock = newTx(t)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if _, err := persistPublishedAgent(context.Background(), tx, input, published, updatedApp, event); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("published revision conflict = %v", err)
	}
	_, tx, mock = newTx(t)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnError(errors.New("app update"))
	mock.ExpectRollback()
	if _, err := persistPublishedAgent(context.Background(), tx, input, published, updatedApp, event); !errors.Is(err, ErrStorage) {
		t.Fatalf("published app error = %v", err)
	}
	_, tx, mock = newTx(t)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if _, err := persistPublishedAgent(context.Background(), tx, input, published, updatedApp, event); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("published app conflict = %v", err)
	}
	_, tx, mock = newTx(t)
	mock.ExpectExec(".*").WillReturnError(errors.New("event"))
	mock.ExpectRollback()
	if _, err := insertAgentEvent(context.Background(), tx, event); !errors.Is(err, ErrStorage) {
		t.Fatalf("event insert error = %v", err)
	}
	_, tx, mock = newTx(t)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewErrorResult(errors.New("last id")))
	mock.ExpectRollback()
	if _, err := insertAgentEvent(context.Background(), tx, event); !errors.Is(err, ErrStorage) {
		t.Fatalf("event id error = %v", err)
	}
	rollbackInput := agent.RollbackInput{TenantID: app.TenantID, AppID: app.AppID, TargetRevision: draft.Revision, ExpectedAppVersion: app.Version}
	_, tx, mock = newTx(t)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if _, err := persistAgentRollback(context.Background(), tx, rollbackInput, updatedApp, event); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("rollback conflict = %v", err)
	}
}

func TestAgentRepositoryDraftAndPublishGuardBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	metadata := agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "guard", CorrelationID: "agent-guard"}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	stale := app.Clone()
	stale.Version++
	expectAgentApp(mock, &stale)
	mock.ExpectRollback()
	if _, err := NewRepository(db).CreateDraft(context.Background(), agent.CreateDraftInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Kind: agent.KindLLM, SchemaVersion: agent.SchemaVersionV1, Configuration: agent.DraftConfiguration{Instruction: "draft", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: agent.DefaultRuntimePolicy()}}); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("stale CreateDraft = %v", err)
	}
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, &stale)
	mock.ExpectRollback()
	if _, err := NewRepository(db).UpdateDraft(context.Background(), agent.UpdateDraftInput{TenantID: app.TenantID, AppID: app.AppID, Revision: 1, ExpectedAppVersion: app.Version, ExpectedDraftVersion: 1, Configuration: agent.DraftConfiguration{Instruction: "draft", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: agent.DefaultRuntimePolicy()}}); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("stale UpdateDraft = %v", err)
	}
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("suspended"))
	mock.ExpectRollback()
	if _, _, _, err := NewRepository(db).Publish(context.Background(), agent.PublishInput{TenantID: app.TenantID, AppID: app.AppID, Revision: 1, ExpectedAppVersion: app.Version, ExpectedDraftVersion: 1, TenantActive: true, Metadata: metadata}); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("inactive publish tenant = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryTransitionsDraftAppWithoutRevision(t *testing.T) {
	app := newStoredAgentApp(t)
	stored := app.Clone()
	stored.Status = agent.StatusDisabled
	stored.Version++
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, app)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(5, 1))
	expectAgentApp(mock, &stored)
	expectAgentEvent(mock, app, agent.ChangeDisabled, agent.StatusDraft, agent.StatusDisabled, nil, nil, "", app.Version, stored.Version, stored.UpdatedAt)
	mock.ExpectCommit()
	result, event, err := NewRepository(db).TransitionStatus(context.Background(), agent.TransitionStatusInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: agent.StatusDisabled, Metadata: agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "disable", CorrelationID: "agent-draft-disable"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != agent.StatusDisabled || event.ContentDigest != "" {
		t.Fatalf("draft transition = app=%+v event=%+v", result, event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryScannerRejectsCorruptRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery("SELECT 1").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("short"))
	if _, err := scanAgentEvent(db.QueryRowContext(context.Background(), "SELECT 1")); err == nil {
		t.Fatal("short agent event row was accepted")
	}
	app := newStoredAgentApp(t)
	mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "revision", "state", "draft_version", "agent_kind", "schema_version", "description", "instruction", "global_instruction", "model_profile_id", "generation_config", "runtime_policy", "content_digest", "published_at", "created_at", "updated_at",
	}).AddRow(app.TenantID, app.AppID, int64(1), string(agent.RevisionStateDraft), int64(1), string(agent.KindLLM), agent.SchemaVersionV1, "", "Answer", "", "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", []byte("not-json"), []byte("{}"), nil, nil, app.CreatedAt, app.UpdatedAt))
	if _, err := loadAgentRevision(context.Background(), db, app.TenantID, app.AppID, 1, false); !errors.Is(err, ErrStorage) {
		t.Fatalf("corrupt revision error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRepositoryDraftAndMetadataConflictBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, app)
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()
	if _, err := NewRepository(db).UpdateMetadata(context.Background(), agent.UpdateMetadataInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, DisplayName: "Updated", Description: app.Description}); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("metadata conflict = %v", err)
	}
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, app)
	mock.ExpectQuery("COALESCE\\(MAX\\(revision\\)").WillReturnError(errors.New("revision query"))
	mock.ExpectRollback()
	if _, err := NewRepository(db).CreateDraft(context.Background(), agent.CreateDraftInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Kind: agent.KindLLM, SchemaVersion: agent.SchemaVersionV1, Configuration: agent.DraftConfiguration{Instruction: "draft", ModelProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", Runtime: agent.DefaultRuntimePolicy()}}); !errors.Is(err, ErrStorage) {
		t.Fatalf("draft revision query error = %v", err)
	}
	draft := newStoredAgentRevision(t, app, 1, true)
	db, mock, err = sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectBegin()
	expectAgentApp(mock, app)
	expectAgentRevision(t, mock, draft)
	mock.ExpectRollback()
	if _, err := NewRepository(db).UpdateDraft(context.Background(), agent.UpdateDraftInput{TenantID: app.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, Configuration: agent.DraftConfiguration{Instruction: "draft", ModelProfileID: draft.ModelProfileID, Runtime: draft.Runtime}}); !errors.Is(err, agent.ErrImmutableRevision) {
		t.Fatalf("immutable draft update = %v", err)
	}
}

func TestAgentRepositoryPublishStateGuardBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	draft := newStoredAgentRevision(t, app, 1, false)
	metadata := agent.ChangeMetadata{ActorType: "test", ActorID: "user", Reason: "publish", CorrelationID: "agent-publish-guards"}
	input := agent.PublishInput{TenantID: app.TenantID, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, TenantActive: true, Metadata: metadata}
	newTx := func(t *testing.T) (*sql.DB, *sql.Tx, sqlmock.Sqlmock) {
		t.Helper()
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		mock.ExpectBegin()
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		return db, tx, mock
	}
	_ = newTx
	_, tx, mock := newTx(t)
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	mock.ExpectQuery(".*").WillReturnError(errors.New("app read"))
	mock.ExpectRollback()
	if _, _, err := loadPublishState(context.Background(), tx, input); !errors.Is(err, ErrStorage) {
		t.Fatalf("publish app read error = %v", err)
	}
	_, tx, mock = newTx(t)
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	staleApp := app.Clone()
	staleApp.Version++
	expectAgentApp(mock, &staleApp)
	mock.ExpectRollback()
	if _, _, err := loadPublishState(context.Background(), tx, input); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("publish app conflict = %v", err)
	}
	_, tx, mock = newTx(t)
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	expectAgentApp(mock, app)
	mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnError(errors.New("revision read"))
	mock.ExpectRollback()
	if _, _, err := loadPublishState(context.Background(), tx, input); !errors.Is(err, ErrStorage) {
		t.Fatalf("publish revision read error = %v", err)
	}
	published := newStoredAgentRevision(t, app, 1, true)
	_, tx, mock = newTx(t)
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	expectAgentApp(mock, app)
	expectAgentRevision(t, mock, published)
	mock.ExpectRollback()
	if _, _, err := loadPublishState(context.Background(), tx, input); !errors.Is(err, agent.ErrImmutableRevision) {
		t.Fatalf("publish immutable revision = %v", err)
	}
	staleDraft := draft.Clone()
	staleDraft.DraftVersion++
	_, tx, mock = newTx(t)
	mock.ExpectQuery("SELECT status FROM tenant").WillReturnRows(sqlmock.NewRows([]string{"status"}).AddRow("active"))
	expectAgentApp(mock, app)
	expectAgentRevision(t, mock, &staleDraft)
	mock.ExpectRollback()
	if _, _, err := loadPublishState(context.Background(), tx, input); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("publish draft version conflict = %v", err)
	}
}

func TestAgentRepositoryReadAndToolErrorBranches(t *testing.T) {
	app := newStoredAgentApp(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	mock.ExpectQuery(".*").WillReturnError(sql.ErrNoRows)
	if _, err := NewRepository(db).Get(context.Background(), app.TenantID, app.AppID); !errors.Is(err, agent.ErrNotFound) {
		t.Fatalf("missing app = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery(".*").WillReturnError(errors.New("current app"))
	mock.ExpectRollback()
	if _, err := NewRepository(db).UpdateMetadata(context.Background(), agent.UpdateMetadataInput{TenantID: app.TenantID, AppID: app.AppID, ExpectedVersion: app.Version, DisplayName: app.DisplayName, Description: app.Description}); !errors.Is(err, ErrStorage) {
		t.Fatalf("metadata read error = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectExec(".*").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(".*").WillReturnError(errors.New("readback"))
	mock.ExpectRollback()
	if _, err := NewRepository(db).Create(context.Background(), agent.CreateInput{TenantID: app.TenantID, AppKey: "readback", DisplayName: "Readback"}); !errors.Is(err, ErrStorage) {
		t.Fatalf("create readback error = %v", err)
	}
	shortDB, shortMock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = shortDB.Close() })
	shortMock.ExpectQuery("SELECT tenant_id, app_id, app_key").WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("short"))
	if _, err := loadAgentApp(context.Background(), shortDB, app.TenantID, app.AppID, false); err == nil {
		t.Fatal("short app row was accepted")
	}
	revision := newStoredAgentRevision(t, app, 1, false)
	generation, runtime, _, err := encodeAgentRevisionParts(*revision)
	if err != nil {
		t.Fatal(err)
	}
	rootRows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"tenant_id", "app_id", "revision", "state", "draft_version", "agent_kind", "schema_version", "description", "instruction", "global_instruction", "model_profile_id", "generation_config", "runtime_policy", "content_digest", "published_at", "created_at", "updated_at"}).AddRow(revision.TenantID, revision.AppID, revision.Revision, string(revision.State), revision.DraftVersion, string(revision.Kind), revision.SchemaVersion, revision.Description, revision.Instruction, revision.GlobalInstruction, revision.ModelProfileID, generation, runtime, nil, nil, revision.CreatedAt, revision.UpdatedAt)
	}
	toolCases := []struct {
		name string
		rows *sqlmock.Rows
	}{
		{name: "query", rows: nil},
		{name: "scan", rows: sqlmock.NewRows([]string{"tool_id", "required"}).AddRow("tool", "invalid")},
		{name: "rows", rows: sqlmock.NewRows([]string{"tool_id", "required"}).AddRow("tool", true).RowError(0, errors.New("rows"))},
	}
	for _, test := range toolCases {
		t.Run("tools "+test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			mock.ExpectQuery("SELECT tenant_id, app_id, revision").WillReturnRows(rootRows())
			if test.rows == nil {
				mock.ExpectQuery("SELECT tool_id").WillReturnError(errors.New("tools query"))
			} else {
				mock.ExpectQuery("SELECT tool_id").WillReturnRows(test.rows)
			}
			if _, err := loadAgentRevision(context.Background(), db, revision.TenantID, revision.AppID, revision.Revision, false); err == nil {
				t.Fatalf("tools %s error was nil", test.name)
			}
		})
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
	var currentRevision any
	if value.CurrentRevision != nil {
		currentRevision = *value.CurrentRevision
	}
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID).WillReturnRows(sqlmock.NewRows([]string{
		"tenant_id", "app_id", "app_key", "display_name", "description", "status", "current_revision", "version", "created_at", "updated_at",
	}).AddRow(value.TenantID, value.AppID, value.AppKey, value.DisplayName, value.Description, string(value.Status), currentRevision, value.Version, value.CreatedAt, value.UpdatedAt))
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
	tools := sqlmock.NewRows([]string{"tool_id", "required"})
	for _, tool := range value.Tools {
		tools.AddRow(tool.ToolID, tool.Required)
	}
	mock.ExpectQuery(".*").WithArgs(value.TenantID, value.AppID, value.Revision).WillReturnRows(tools)
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
