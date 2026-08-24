package inmemory_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestStoreTenantIsolationAndCAS(t *testing.T) {
	store := inmemory.New()
	first, err := store.CreateSession(context.Background(), "tenant-a", "session-1", map[string]any{"nested": map[string]any{"n": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSession(context.Background(), "tenant-b", first.SessionID); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant read = %v", err)
	}
	if _, err := store.UpdateSessionState(context.Background(), first.TenantID, first.SessionID, 99, nil); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("CAS error = %v", err)
	}
	updated, err := store.UpdateSessionState(context.Background(), first.TenantID, first.SessionID, first.Version, map[string]any{"nested": map[string]any{"n": 2}})
	if err != nil || updated.Version != 2 {
		t.Fatalf("update = %+v, %v", updated, err)
	}
	updated.State["nested"].(map[string]any)["n"] = 9
	readBack, err := store.GetSession(context.Background(), "tenant-a", "session-1")
	if err != nil || readBack.State["nested"].(map[string]any)["n"] == 9 {
		t.Fatalf("nested state aliased: %+v", readBack.State)
	}
}

func TestStoreDuplicateMessageAndConcurrentSequence(t *testing.T) {
	store := inmemory.New()
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-1", nil); err != nil {
		t.Fatal(err)
	}
	input := runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1", IdempotencyKey: "idem-1"}
	input.EventID = "event-1"
	first, duplicate, err := store.RecordMessage(context.Background(), input)
	if err != nil || duplicate {
		t.Fatalf("first = %+v duplicate=%v err=%v", first, duplicate, err)
	}
	second, duplicate, err := store.RecordMessage(context.Background(), input)
	if err != nil || !duplicate || second.EventID != first.EventID {
		t.Fatalf("duplicate = %+v duplicate=%v err=%v", second, duplicate, err)
	}

	var wg sync.WaitGroup
	results := make(chan int64, 2)
	for _, id := range []string{"event-2", "event-3"} {
		wg.Add(1)
		go func(eventID string) {
			defer wg.Done()
			value, _, callErr := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session-1", BindingID: eventID, ExternalMessageID: eventID, EventID: eventID})
			if callErr == nil {
				results <- value.EventSeq
			}
		}(id)
	}
	wg.Wait()
	close(results)
	var seq []int64
	for value := range results {
		seq = append(seq, value)
	}
	if len(seq) != 2 || seq[0] == seq[1] {
		t.Fatalf("concurrent sequences = %v", seq)
	}
}

func TestStoreReplyStateMachineAndFencing(t *testing.T) {
	store := inmemory.New()
	reply, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-1", EventID: "event-1", SegmentIndex: 0, SegmentCount: 1, Payload: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	sending, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: reply.TenantID, ReplyID: reply.ReplyID, SegmentIndex: 0, From: runtimestorage.ReplyPending, To: runtimestorage.ReplySending, Owner: "worker-a"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: reply.TenantID, ReplyID: reply.ReplyID, SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: "worker-b", FencingToken: sending.FencingToken}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale owner = %v", err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: reply.TenantID, ReplyID: reply.ReplyID, SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: "worker-a", FencingToken: sending.FencingToken, ProviderID: "provider-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: reply.TenantID, ReplyID: reply.ReplyID, SegmentIndex: 0, From: runtimestorage.ReplySent, To: runtimestorage.ReplySending, Owner: "worker-a"}); !errors.Is(err, runtimestorage.ErrIllegalTransition) {
		t.Fatalf("illegal transition = %v", err)
	}
}
