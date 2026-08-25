package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestWorkerInternalBranchCoverage(t *testing.T) {
	if eligible(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplySending}) {
		t.Fatal("sending without lease should not be eligible")
	}
	if !eligible(runtimestorage.ReplyOutbox{Status: runtimestorage.ReplySending, LeaseExpiresAt: ptrTime(time.Now().Add(-time.Second))}) {
		t.Fatal("expired sending should be eligible")
	}
	if class, retryable := classify(errors.New("provider")); class != "provider_error" || !retryable {
		t.Fatalf("default classification = %s/%v", class, retryable)
	}
	store := inmemory.New()
	worker := &Worker{store: store, tenantID: "tenant-a", owner: "worker"}
	worker.advanceEvent(context.Background(), "")
	worker.advanceEvent(context.Background(), "missing")
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event", SessionID: "session", BindingID: "binding", ExternalMessageID: "external"}); err != nil {
		t.Fatal(err)
	}
	worker.advanceEvent(context.Background(), "event")
}

func ptrTime(value time.Time) *time.Time { return &value }

type branchStore struct {
	runtimestorage.RuntimeStore
	candidates         []runtimestorage.ReplyOutbox
	listErr            error
	event              runtimestorage.MessageEvent
	getErr             error
	transitionErr      error
	messageTransitions []runtimestorage.MessageTransition
	replyTransitions   []runtimestorage.ReplyTransition
}

func (s *branchStore) ClaimReply(_ context.Context, _ string, _ string, _ int, _ string, _ time.Duration) (runtimestorage.ReplyOutbox, error) {
	if len(s.candidates) == 0 {
		return runtimestorage.ReplyOutbox{}, runtimestorage.ErrNotFound
	}
	claimed := s.candidates[0]
	claimed.Status = runtimestorage.ReplySending
	claimed.FencingToken++
	return claimed, nil
}

func (s *branchStore) ListReplyCandidates(context.Context, string) ([]runtimestorage.ReplyOutbox, error) {
	return s.candidates, s.listErr
}

func (s *branchStore) GetMessage(context.Context, string, string) (runtimestorage.MessageEvent, error) {
	return s.event, s.getErr
}

func (s *branchStore) TransitionMessage(_ context.Context, transition runtimestorage.MessageTransition) (runtimestorage.MessageEvent, error) {
	s.messageTransitions = append(s.messageTransitions, transition)
	return s.event, s.transitionErr
}

func (s *branchStore) TransitionReply(_ context.Context, transition runtimestorage.ReplyTransition) (runtimestorage.ReplyOutbox, error) {
	s.replyTransitions = append(s.replyTransitions, transition)
	return runtimestorage.ReplyOutbox{}, s.transitionErr
}

type branchProvider struct {
	status DeliveryStatus
	id     string
	err    error
}

func (p branchProvider) Deliver(context.Context, runtimestorage.ReplyOutbox) (string, error) {
	return p.id, p.err
}

func (p branchProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (DeliveryStatus, string, error) {
	return p.status, p.id, p.err
}

func TestAdvanceEventBranchCoverage(t *testing.T) {
	ctx := context.Background()
	base := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", Status: runtimestorage.ReplySent}
	cases := []struct {
		name            string
		store           *branchStore
		wantTransitions int
	}{
		{name: "list error", store: &branchStore{listErr: errors.New("list")}},
		{name: "no matching event", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{{EventID: "other", Status: runtimestorage.ReplySent}}}},
		{name: "incomplete replies", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{{EventID: "event", Status: runtimestorage.ReplyPending}}}},
		{name: "get error", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}, getErr: errors.New("get")}},
		{name: "completed transition error", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}, event: runtimestorage.MessageEvent{Status: runtimestorage.EventCompleted}, transitionErr: errors.New("transition")}, wantTransitions: 1},
		{name: "reply pending advances", store: &branchStore{candidates: []runtimestorage.ReplyOutbox{base}, event: runtimestorage.MessageEvent{Status: runtimestorage.EventReplyPending}, transitionErr: nil}, wantTransitions: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker := &Worker{store: tc.store, tenantID: "tenant-a", owner: "worker"}
			worker.advanceEvent(ctx, "event")
			if got := len(tc.store.messageTransitions); got != tc.wantTransitions {
				t.Fatalf("message transitions = %d, want %d", got, tc.wantTransitions)
			}
		})
	}
}

func TestReconcileBranchCoverage(t *testing.T) {
	ctx := context.Background()
	claimed := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, FencingToken: 7, Status: runtimestorage.ReplySending}
	cases := []struct {
		name     string
		provider branchProvider
		store    *branchStore
		want     bool
		wantTo   string
	}{
		{name: "provider error", provider: branchProvider{err: errors.New("reconcile")}, store: &branchStore{}},
		{name: "unknown", provider: branchProvider{status: DeliveryUnknown}, store: &branchStore{}},
		{name: "rejected", provider: branchProvider{status: DeliveryRejected, id: "provider"}, store: &branchStore{}, want: true, wantTo: runtimestorage.ReplyRetryable},
		{name: "accepted transition error", provider: branchProvider{status: DeliveryAccepted, id: "provider"}, store: &branchStore{transitionErr: errors.New("transition")}},
		{name: "accepted", provider: branchProvider{status: DeliveryAccepted, id: "provider"}, store: &branchStore{}, want: true, wantTo: runtimestorage.ReplySent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			worker := &Worker{store: tc.store, provider: tc.provider, tenantID: "tenant-a", owner: "worker"}
			if got := worker.reconcile(ctx, claimed); got != tc.want {
				t.Fatalf("reconcile = %v, want %v", got, tc.want)
			}
			if tc.wantTo != "" {
				if len(tc.store.replyTransitions) != 1 || tc.store.replyTransitions[0].To != tc.wantTo {
					t.Fatalf("reply transition = %+v, want %s", tc.store.replyTransitions, tc.wantTo)
				}
			}
		})
	}
}

func TestClassifyTypedDeliveryErrors(t *testing.T) {
	if class, retryable := classify(&DeliveryError{Class: "", Retryable: false}); class != "provider_error" || !retryable {
		t.Fatalf("empty class = %s/%v", class, retryable)
	}
	if class, retryable := classify(&DeliveryError{Class: "permanent token=secret", Retryable: false}); class != "provider_error" || retryable {
		t.Fatalf("typed class = %s/%v", class, retryable)
	}
	if metricErrorClass("provider_rejected") != "error" || metricErrorClass("rate_limited") != "rate_limited" {
		t.Fatal("provider classes were not reduced to low-cardinality metric classes")
	}
}

type countingProvider struct {
	deliveries int
	status     DeliveryStatus
	err        error
}

func (p *countingProvider) Deliver(context.Context, runtimestorage.ReplyOutbox) (string, error) {
	p.deliveries++
	return "", nil
}

func (p *countingProvider) Reconcile(context.Context, runtimestorage.ReplyOutbox) (DeliveryStatus, string, error) {
	return p.status, "", p.err
}

func TestRunOnceDoesNotRedeliverUnknownSendingLease(t *testing.T) {
	provider := &countingProvider{status: DeliveryUnknown}
	store := &branchStore{candidates: []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", Status: runtimestorage.ReplySending, LeaseExpiresAt: ptrTime(time.Now().Add(-time.Second))}}}
	worker := &Worker{store: store, provider: provider, tenantID: "tenant-a", owner: "worker", leaseDuration: time.Second}
	if processed, err := worker.RunOnce(context.Background()); err != nil || processed != 1 {
		t.Fatalf("run = %d/%v", processed, err)
	}
	if provider.deliveries != 0 {
		t.Fatalf("unknown reconciliation redelivered %d times", provider.deliveries)
	}
}
