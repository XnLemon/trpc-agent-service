package postgres

import (
	"context"
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
