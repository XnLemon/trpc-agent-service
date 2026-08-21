package inmemory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
)

const (
	tenantOne = "t_01J1K9ZQTVE4PAWF1TSB2WMHNP"
	tenantTwo = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"
)

func TestCreateScopesKeysByTenantAndReturnsCopies(t *testing.T) {
	r := NewRepository()
	first := createApp(t, r, tenantOne, "support")
	if first.Status != agent.StatusDraft || first.Version != 1 {
		t.Fatalf("unexpected app root: %+v", first)
	}
	if _, err := r.Create(context.Background(), createInput(tenantOne, "support")); !errors.Is(err, agent.ErrDuplicateKey) {
		t.Fatalf("expected tenant-local duplicate rejection, got %v", err)
	}
	second := createApp(t, r, tenantTwo, "support")
	if second.AppID == first.AppID {
		t.Fatal("generated app identities must differ")
	}
	if _, err := r.Get(context.Background(), tenantTwo, first.AppID); !errors.Is(err, agent.ErrNotFound) {
		t.Fatalf("cross-tenant lookup must not find app, got %v", err)
	}

	first.DisplayName = "mutated"
	stored, err := r.Get(context.Background(), tenantOne, first.AppID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.DisplayName != "Support" {
		t.Fatalf("caller mutation leaked into repository: %+v", stored)
	}
}

func TestDraftCRUDUsesAppAndDraftVersions(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "drafts")
	first := createDraft(t, r, app, draftConfiguration("first"))
	second := createDraft(t, r, app, draftConfiguration("second"))
	if first.Revision != 1 || second.Revision != 2 {
		t.Fatalf("revision allocation must be App-local and monotonic: %d %d", first.Revision, second.Revision)
	}

	updated, err := r.UpdateDraft(context.Background(), agent.UpdateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, Revision: first.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: first.DraftVersion,
		Configuration: draftConfiguration("updated"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.DraftVersion != 2 || updated.Instruction != "updated instruction" {
		t.Fatalf("unexpected updated draft: %+v", updated)
	}
	if _, err := r.UpdateDraft(context.Background(), agent.UpdateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, Revision: first.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: first.DraftVersion,
		Configuration: draftConfiguration("stale"),
	}); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("expected stale draft conflict, got %v", err)
	}

	updated.Tools[0].ToolID = "leaked"
	*updated.Generation.Temperature = 1.7
	stored, err := r.GetRevision(context.Background(), tenantOne, app.AppID, first.Revision)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Tools[0].ToolID == "leaked" || *stored.Generation.Temperature == 1.7 {
		t.Fatal("returned draft did not have defensive slice and pointer copies")
	}

	metadata, err := r.UpdateMetadata(context.Background(), agent.UpdateMetadataInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version,
		DisplayName: " Updated ", Description: " Changed ",
	})
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Version != 2 || metadata.DisplayName != "Updated" || metadata.Description != "Changed" {
		t.Fatalf("unexpected metadata update: %+v", metadata)
	}
	if _, err := r.CreateDraft(context.Background(), agent.CreateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: app.Version,
		Configuration: draftConfiguration("stale-app"),
	}); !errors.Is(err, agent.ErrConflict) {
		t.Fatalf("expected stale App version conflict, got %v", err)
	}
	if _, err := r.GetRevision(context.Background(), tenantTwo, app.AppID, first.Revision); !errors.Is(err, agent.ErrNotFound) {
		t.Fatalf("cross-tenant revision lookup must fail, got %v", err)
	}
}

func TestPublishRollbackAndLifecycleReturnCompleteEvents(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "lifecycle")
	first := createDraft(t, r, app, draftConfiguration("first"))

	inactive := publishInput(app, first)
	inactive.TenantActive = false
	if _, _, _, err := r.Publish(context.Background(), inactive); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("expected inactive tenant publication rejection, got %v", err)
	}
	unchanged, _ := r.Get(context.Background(), tenantOne, app.AppID)
	if unchanged.Version != app.Version || unchanged.Status != agent.StatusDraft {
		t.Fatalf("failed publication changed root: %+v", unchanged)
	}

	publishedApp, publishedFirst, event, err := r.Publish(context.Background(), publishInput(app, first))
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, event, agent.ChangePublished, 1, 2, nil, int64Pointer(1), publishedFirst.ContentDigest)
	if publishedApp.Status != agent.StatusActive || publishedApp.CurrentRevision == nil || *publishedApp.CurrentRevision != 1 {
		t.Fatalf("first publication did not activate App: %+v", publishedApp)
	}
	if _, err := r.UpdateDraft(context.Background(), agent.UpdateDraftInput{
		TenantID: tenantOne, AppID: app.AppID, Revision: first.Revision,
		ExpectedAppVersion: publishedApp.Version, ExpectedDraftVersion: first.DraftVersion,
		Configuration: draftConfiguration("immutable"),
	}); !errors.Is(err, agent.ErrImmutableRevision) {
		t.Fatalf("published revision accepted draft update, got %v", err)
	}

	second := createDraft(t, r, publishedApp, draftConfiguration("second"))
	secondApp, publishedSecond, secondEvent, err := r.Publish(context.Background(), publishInput(publishedApp, second))
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, secondEvent, agent.ChangePublished, 2, 3, int64Pointer(1), int64Pointer(2), publishedSecond.ContentDigest)

	rolledBack, rollbackEvent, err := r.Rollback(context.Background(), agent.RollbackInput{
		TenantID: tenantOne, AppID: app.AppID, TargetRevision: 1, ExpectedAppVersion: secondApp.Version, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, rollbackEvent, agent.ChangeRolledBack, 3, 4, int64Pointer(2), int64Pointer(1), publishedFirst.ContentDigest)
	storedFirst, _ := r.GetRevision(context.Background(), tenantOne, app.AppID, 1)
	if storedFirst.ContentDigest != publishedFirst.ContentDigest || storedFirst.Revision != 1 {
		t.Fatal("rollback copied or mutated immutable target revision")
	}

	suspended, suspendEvent, err := r.TransitionStatus(context.Background(), agent.TransitionStatusInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: rolledBack.Version, NextStatus: agent.StatusSuspended, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	assertEvent(t, suspendEvent, agent.ChangeSuspended, 4, 5, int64Pointer(1), int64Pointer(1), publishedFirst.ContentDigest)
	resumed, resumeEvent, err := r.TransitionStatus(context.Background(), agent.TransitionStatusInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: suspended.Version, NextStatus: agent.StatusActive, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if resumeEvent.EventType != agent.ChangeResumed || resumed.Status != agent.StatusActive {
		t.Fatalf("unexpected resume result: %+v %+v", resumed, resumeEvent)
	}
	disabled, disabledEvent, err := r.TransitionStatus(context.Background(), agent.TransitionStatusInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: resumed.Version, NextStatus: agent.StatusDisabled, Metadata: changeMetadata(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if disabledEvent.EventType != agent.ChangeDisabled || disabled.Status != agent.StatusDisabled {
		t.Fatalf("unexpected disable result: %+v %+v", disabled, disabledEvent)
	}
	if _, err := r.UpdateMetadata(context.Background(), agent.UpdateMetadataInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: disabled.Version, DisplayName: "No", Description: ""}); !errors.Is(err, agent.ErrDisabled) {
		t.Fatalf("terminal App accepted mutation, got %v", err)
	}

	event.CurrentRevision = int64Pointer(99)
	if publishedApp.CurrentRevision == nil || *publishedApp.CurrentRevision != 1 {
		t.Fatal("event pointer mutation leaked into returned App")
	}
}

func TestPublishAndDraftUpdateHaveSingleConcurrentWinner(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "publish-race")
		draft := createDraft(t, r, app, draftConfiguration("race"))
		input := publishInput(app, draft)
		errorsCh := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, _, err := r.Publish(context.Background(), input)
				errorsCh <- err
			}()
		}
		wg.Wait()
		close(errorsCh)
		assertOneWinner(t, errorsCh)
	})

	t.Run("draft update", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "draft-race")
		draft := createDraft(t, r, app, draftConfiguration("race"))
		errorsCh := make(chan error, 2)
		var wg sync.WaitGroup
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				_, err := r.UpdateDraft(context.Background(), agent.UpdateDraftInput{
					TenantID: tenantOne, AppID: app.AppID, Revision: draft.Revision,
					ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion,
					Configuration: draftConfiguration("race-update"),
				})
				errorsCh <- err
			}(i)
		}
		wg.Wait()
		close(errorsCh)
		assertOneWinner(t, errorsCh)
	})

	t.Run("publish versus draft update", func(t *testing.T) {
		r := NewRepository()
		app := createApp(t, r, tenantOne, "mixed-race")
		draft := createDraft(t, r, app, draftConfiguration("race"))
		errorsCh := make(chan error, 2)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, _, err := r.Publish(context.Background(), publishInput(app, draft))
			errorsCh <- err
		}()
		go func() {
			defer wg.Done()
			_, err := r.UpdateDraft(context.Background(), agent.UpdateDraftInput{
				TenantID: tenantOne, AppID: app.AppID, Revision: draft.Revision,
				ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion,
				Configuration: draftConfiguration("mixed-update"),
			})
			errorsCh <- err
		}()
		wg.Wait()
		close(errorsCh)
		assertOneWinner(t, errorsCh)
	})
}

func TestAuditValidationAndRollbackGuards(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "guards")
	draft := createDraft(t, r, app, draftConfiguration("guard"))
	invalid := publishInput(app, draft)
	invalid.Metadata.Reason = ""
	if _, _, _, err := r.Publish(context.Background(), invalid); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("expected audit metadata rejection, got %v", err)
	}
	if _, _, err := r.Rollback(context.Background(), agent.RollbackInput{
		TenantID: tenantOne, AppID: app.AppID, TargetRevision: draft.Revision,
		ExpectedAppVersion: app.Version, Metadata: changeMetadata(),
	}); !errors.Is(err, agent.ErrInvalid) {
		t.Fatalf("expected unpublished rollback target rejection, got %v", err)
	}
	if _, _, err := r.TransitionStatus(context.Background(), agent.TransitionStatusInput{
		TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version,
		NextStatus: agent.StatusSuspended, Metadata: changeMetadata(),
	}); !errors.Is(err, agent.ErrInvalidTransition) {
		t.Fatalf("expected draft-to-suspended transition rejection, got %v", err)
	}
}

func TestOperationsHonorContextCancellationWhileWaiting(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "cancel")
	if err := r.mu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.mu.unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := r.Get(ctx, tenantOne, app.AppID)
		done <- err
	}()
	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("operation did not observe cancellation while waiting for lock")
	}
}

func TestReadLocksRemainConcurrent(t *testing.T) {
	r := NewRepository()
	if err := r.mu.rlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.mu.runlock()
	acquired := make(chan error, 1)
	go func() {
		err := r.mu.rlock(context.Background())
		if err == nil {
			r.mu.runlock()
		}
		acquired <- err
	}()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("concurrent readers were serialized")
	}
}

func TestEveryOperationChecksAlreadyCancelledContext(t *testing.T) {
	r := NewRepository()
	app := createApp(t, r, tenantOne, "cancelled")
	draft := createDraft(t, r, app, draftConfiguration("cancelled"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	operations := []struct {
		name string
		run  func() error
	}{
		{name: "create", run: func() error { _, err := r.Create(ctx, createInput(tenantOne, "cancelled-create")); return err }},
		{name: "get", run: func() error { _, err := r.Get(ctx, tenantOne, app.AppID); return err }},
		{name: "metadata", run: func() error {
			_, err := r.UpdateMetadata(ctx, agent.UpdateMetadataInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version, DisplayName: "Name"})
			return err
		}},
		{name: "create draft", run: func() error {
			_, err := r.CreateDraft(ctx, agent.CreateDraftInput{TenantID: tenantOne, AppID: app.AppID, ExpectedAppVersion: app.Version, Configuration: draftConfiguration("new")})
			return err
		}},
		{name: "update draft", run: func() error {
			_, err := r.UpdateDraft(ctx, agent.UpdateDraftInput{TenantID: tenantOne, AppID: app.AppID, Revision: draft.Revision, ExpectedAppVersion: app.Version, ExpectedDraftVersion: draft.DraftVersion, Configuration: draftConfiguration("updated")})
			return err
		}},
		{name: "get revision", run: func() error { _, err := r.GetRevision(ctx, tenantOne, app.AppID, draft.Revision); return err }},
		{name: "publish", run: func() error { _, _, _, err := r.Publish(ctx, publishInput(app, draft)); return err }},
		{name: "rollback", run: func() error {
			_, _, err := r.Rollback(ctx, agent.RollbackInput{TenantID: tenantOne, AppID: app.AppID, TargetRevision: draft.Revision, ExpectedAppVersion: app.Version, Metadata: changeMetadata()})
			return err
		}},
		{name: "transition", run: func() error {
			_, _, err := r.TransitionStatus(ctx, agent.TransitionStatusInput{TenantID: tenantOne, AppID: app.AppID, ExpectedVersion: app.Version, NextStatus: agent.StatusDisabled, Metadata: changeMetadata()})
			return err
		}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); !errors.Is(err, context.Canceled) {
				t.Fatalf("expected context cancellation, got %v", err)
			}
		})
	}
	if _, err := r.Get(nil, tenantOne, app.AppID); err != nil {
		t.Fatalf("nil context should behave like a non-cancelled context: %v", err)
	}
}

func TestWriterLockHonorsCancellationWhileWaiting(t *testing.T) {
	r := NewRepository()
	if err := r.mu.lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.mu.unlock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.mu.lock(ctx) }()
	time.Sleep(25 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected writer cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting writer did not observe cancellation")
	}
}

func createInput(tenantID, key string) agent.CreateInput {
	return agent.CreateInput{TenantID: tenantID, AppKey: key, DisplayName: "Support", Description: "Answers questions"}
}

func createApp(t *testing.T, r *InMemoryRepository, tenantID, key string) *agent.App {
	t.Helper()
	app, err := r.Create(context.Background(), createInput(tenantID, key))
	if err != nil {
		t.Fatal(err)
	}
	return app
}

func draftConfiguration(name string) agent.DraftConfiguration {
	return agent.DraftConfiguration{
		Description: name, Instruction: name + " instruction", ModelProfileID: "model-primary",
		Generation: agent.GenerationConfig{Temperature: float64Pointer(0.2), MaxOutputTokens: intPointer(1024)},
		Runtime:    agent.DefaultRuntimePolicy(), Tools: []agent.ToolAuthorization{{ToolID: "search", Required: true}},
	}
}

func createDraft(t *testing.T, r *InMemoryRepository, app *agent.App, configuration agent.DraftConfiguration) *agent.Revision {
	t.Helper()
	draft, err := r.CreateDraft(context.Background(), agent.CreateDraftInput{
		TenantID: app.TenantID, AppID: app.AppID, ExpectedAppVersion: app.Version, Configuration: configuration,
	})
	if err != nil {
		t.Fatal(err)
	}
	return draft
}

func publishInput(app *agent.App, revision *agent.Revision) agent.PublishInput {
	return agent.PublishInput{
		TenantID: app.TenantID, AppID: app.AppID, Revision: revision.Revision,
		ExpectedAppVersion: app.Version, ExpectedDraftVersion: revision.DraftVersion,
		TenantActive: true, Metadata: changeMetadata(),
	}
}

func changeMetadata() agent.ChangeMetadata {
	return agent.ChangeMetadata{ActorType: "admin", ActorID: "user-1", Reason: "requested change", CorrelationID: "corr-1"}
}

func assertEvent(t *testing.T, event agent.ChangeEvent, eventType agent.ChangeEventType, previousVersion, nextVersion int64, previousRevision, currentRevision *int64, digest string) {
	t.Helper()
	if event.EventType != eventType || event.TenantID != tenantOne || event.AppID == "" || event.PreviousVersion != previousVersion || event.NextVersion != nextVersion || event.ContentDigest != digest || event.ActorType != "admin" || event.ActorID != "user-1" || event.Reason != "requested change" || event.CorrelationID != "corr-1" || event.OccurredAt.IsZero() {
		t.Fatalf("incomplete event: %+v", event)
	}
	if !sameOptionalInt(event.PreviousRevision, previousRevision) || !sameOptionalInt(event.CurrentRevision, currentRevision) {
		t.Fatalf("unexpected event revision movement: %+v", event)
	}
}

func assertOneWinner(t *testing.T, results <-chan error) {
	t.Helper()
	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, agent.ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("expected one winner and one conflict, got %d and %d", successes, conflicts)
	}
}

func sameOptionalInt(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func float64Pointer(value float64) *float64 { return &value }
func intPointer(value int) *int             { return &value }
