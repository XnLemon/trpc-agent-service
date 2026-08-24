package outbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

type providerStub struct {
	deliverID       string
	deliverErr      error
	reconcileStatus outbox.DeliveryStatus
	reconcileID     string
	reconcileErr    error
	deliveries      int
	reconciliations int
}

func (p *providerStub) Deliver(context.Context, runtimestorage.ReplyOutbox) (string, error) {
	p.deliveries++
	return p.deliverID, p.deliverErr
}
func (p *providerStub) Reconcile(context.Context, runtimestorage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	p.reconciliations++
	return p.reconcileStatus, p.reconcileID, p.reconcileErr
}

func seedReply(t *testing.T, store *inmemory.Store, tenant, event, reply string) {
	t.Helper()
	if _, err := store.CreateSession(context.Background(), tenant, "session-"+event, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: tenant, SessionID: "session-" + event, BindingID: "binding-" + event, ExternalMessageID: "external-" + event, EventID: event}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: tenant, ReplyID: reply, EventID: event, SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
}

func TestWorkerDeliversAndFencesProviderReceipt(t *testing.T) {
	store := inmemory.New()
	seedReply(t, store, "tenant-a", "event-1", "reply-1")
	event, err := store.GetMessage(context.Background(), "tenant-a", "event-1")
	if err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "runner", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "runner", FencingToken: running.FencingToken}); err != nil {
		t.Fatal(err)
	}
	provider := &providerStub{deliverID: "provider-1"}
	worker, err := outbox.New(outbox.Config{Store: store, Provider: provider, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := worker.RunOnce(context.Background())
	if err != nil || processed != 1 {
		t.Fatalf("run = %d err=%v", processed, err)
	}
	value, err := store.GetReply(context.Background(), "tenant-a", "reply-1", 0)
	if err != nil || value.Status != runtimestorage.ReplySent || value.ProviderMessageID != "provider-1" || provider.deliveries != 1 {
		t.Fatalf("reply = %+v deliveries=%d err=%v", value, provider.deliveries, err)
	}
	event, err = store.GetMessage(context.Background(), "tenant-a", "event-1")
	if err != nil || event.Status != runtimestorage.EventReplied {
		t.Fatalf("event lifecycle = %+v err=%v", event, err)
	}
}

func TestWorkerRetriesThenDeadLettersStableProviderErrors(t *testing.T) {
	store := inmemory.New()
	seedReply(t, store, "tenant-a", "event-2", "reply-2")
	provider := &providerStub{deliverErr: &outbox.DeliveryError{Class: "rate_limited", Retryable: true}}
	worker, err := outbox.New(outbox.Config{Store: store, Provider: provider, TenantID: "tenant-a", Owner: "worker-a", LeaseDuration: time.Second, MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	value, err := store.GetReply(context.Background(), "tenant-a", "reply-2", 0)
	if err != nil || value.Status != runtimestorage.ReplyDeadLetter || value.LastErrorClass != "rate_limited" {
		t.Fatalf("dead letter = %+v err=%v", value, err)
	}
}

func TestWorkerReconcilesExpiredSendingBeforeRedelivery(t *testing.T) {
	store := inmemory.New()
	seedReply(t, store, "tenant-a", "event-3", "reply-3")
	event, err := store.GetMessage(context.Background(), "tenant-a", "event-3")
	if err != nil {
		t.Fatal(err)
	}
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "runner", LeaseDuration: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "runner", FencingToken: running.FencingToken}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimReply(context.Background(), "tenant-a", "reply-3", 0, "old-worker", time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Millisecond)
	provider := &providerStub{reconcileStatus: outbox.DeliveryAccepted, reconcileID: "provider-recovered"}
	worker, err := outbox.New(outbox.Config{Store: store, Provider: provider, TenantID: "tenant-a", Owner: "new-worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	value, err := store.GetReply(context.Background(), "tenant-a", "reply-3", 0)
	if err != nil || value.Status != runtimestorage.ReplySent || value.ProviderMessageID != "provider-recovered" || provider.reconciliations != 1 || provider.deliveries != 0 {
		t.Fatalf("reconciled = %+v provider=%+v err=%v", value, provider, err)
	}
	updated, err := store.GetMessage(context.Background(), "tenant-a", event.EventID)
	if err != nil || updated.Status != runtimestorage.EventReplied {
		t.Fatalf("reconciled event status = %+v err=%v", updated, err)
	}
}

func TestWorkerValidationAndCancellation(t *testing.T) {
	if _, err := outbox.New(outbox.Config{}); !errors.Is(err, outbox.ErrInvalid) {
		t.Fatalf("invalid worker = %v", err)
	}
	store := inmemory.New()
	worker, err := outbox.New(outbox.Config{Store: store, Provider: &providerStub{}, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := worker.RunOnce(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled run = %v", err)
	}
}

func TestMaterializerSegmentsIdempotently(t *testing.T) {
	store := inmemory.New()
	seedReply(t, store, "tenant-a", "event-materialize", "unused")
	m, err := outbox.NewMaterializer(outbox.MaterializerConfig{Store: store, SegmentSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	count, err := m.Materialize(context.Background(), outbox.MaterializeInput{TenantID: "tenant-a", EventID: "event-materialize", ReplyID: "reply-materialize", Payload: "abcdef"})
	if err != nil || count != 2 {
		t.Fatalf("materialize = %d err=%v", count, err)
	}
	if count, err = m.Materialize(context.Background(), outbox.MaterializeInput{TenantID: "tenant-a", EventID: "event-materialize", ReplyID: "reply-materialize", Payload: "abcdef"}); err != nil || count != 2 {
		t.Fatalf("idempotent materialize = %d err=%v", count, err)
	}
	if _, err := m.Materialize(context.Background(), outbox.MaterializeInput{TenantID: "tenant-a", EventID: "event-materialize", ReplyID: "reply-materialize", Payload: "xyz"}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("materialization conflict = %v", err)
	}
	values, err := store.ListReplyCandidates(context.Background(), "tenant-a")
	if err != nil || len(values) != 3 {
		t.Fatalf("materialized rows = %d err=%v", len(values), err)
	}
}

func TestWorkerRunStopsOnCancellationAndRejectsConcurrentRun(t *testing.T) {
	store := inmemory.New()
	provider := &providerStub{}
	worker, err := outbox.New(outbox.Config{Store: store, Provider: provider, TenantID: "tenant-a", Owner: "worker", LeaseDuration: time.Second, BackoffBase: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx, time.Millisecond) }()
	time.Sleep(2 * time.Millisecond)
	if err := worker.Run(ctx, time.Millisecond); !errors.Is(err, outbox.ErrAlreadyRunning) {
		t.Fatalf("concurrent run = %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run cancellation = %v", err)
	}
	if err := worker.Close(); err != nil {
		t.Fatal(err)
	}
}
