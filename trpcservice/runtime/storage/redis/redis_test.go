package redis_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	redisstore "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
	"github.com/alicebob/miniredis/v2"
	redisclient "github.com/redis/go-redis/v9"
)

func newStore(t *testing.T, server *miniredis.Miniredis) *redisstore.Store {
	t.Helper()
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	store, err := redisstore.New(client, "test:runtime:v1")
	if err != nil {
		_ = client.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.Close()
		_ = client.Close()
	})
	return store
}

func seedEvent(t *testing.T, store *redisstore.Store, tenantID, sessionID, eventID string) runtimestorage.MessageEvent {
	t.Helper()
	if _, err := store.CreateSession(context.Background(), tenantID, sessionID, nil); err != nil {
		t.Fatal(err)
	}
	event, duplicate, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{
		TenantID: tenantID, SessionID: sessionID, BindingID: "binding-" + eventID,
		ExternalMessageID: "external-" + eventID, EventID: eventID,
	})
	if err != nil || duplicate {
		t.Fatalf("seed event = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	return event
}

func TestRedisTenantIsolationAndReconnect(t *testing.T) {
	server := miniredis.RunT(t)
	first := newStore(t, server)
	second := newStore(t, server)
	ctx := context.Background()
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if _, err := first.CreateSession(ctx, tenantID, "same-session", map[string]any{"tenant": tenantID}); err != nil {
			t.Fatal(err)
		}
		event, _, err := first.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: tenantID, SessionID: "same-session", BindingID: "same-binding", ExternalMessageID: "same-external", EventID: "same-event"})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := first.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: tenantID, ReplyID: "same-reply", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: tenantID}); err != nil {
			t.Fatal(err)
		}
		if _, err := first.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: tenantID, MemoryID: "same-memory", UserID: "user", Content: tenantID}); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := second.GetSession(ctx, "tenant-a", "same-session"); err != nil || got.State["tenant"] != "tenant-a" {
		t.Fatalf("tenant-a session = %+v, %v", got, err)
	}
	if got, err := second.GetSession(ctx, "tenant-b", "same-session"); err != nil || got.State["tenant"] != "tenant-b" {
		t.Fatalf("tenant-b session = %+v, %v", got, err)
	}
	if got, err := second.GetReply(ctx, "tenant-b", "same-reply", 0); err != nil || got.Payload != "tenant-b" {
		t.Fatalf("tenant-b reply = %+v, %v", got, err)
	}
	if _, err := second.GetMemory(ctx, "tenant-a", "same-memory"); err != nil {
		t.Fatal(err)
	}

	// A fresh client sees committed state after the original store is closed.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := newStore(t, server)
	if got, err := reopened.GetMessage(ctx, "tenant-a", "same-event"); err != nil || got.SessionID != "same-session" {
		t.Fatalf("reopened event = %+v, %v", got, err)
	}
}

func TestRedisDuplicateDeliveryAndConcurrentSequence(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	input := runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event-1"}
	first, duplicate, err := store.RecordMessage(ctx, input)
	if err != nil || duplicate {
		t.Fatalf("first = %+v duplicate=%v err=%v", first, duplicate, err)
	}
	input.EventID = "event-duplicate"
	replayed, duplicate, err := store.RecordMessage(ctx, input)
	if err != nil || !duplicate || replayed.EventID != first.EventID {
		t.Fatalf("replayed = %+v duplicate=%v err=%v", replayed, duplicate, err)
	}

	var wg sync.WaitGroup
	results := make(chan int64, 2)
	for _, eventID := range []string{"event-2", "event-3"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			value, _, callErr := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: id, ExternalMessageID: id, EventID: id})
			if callErr == nil {
				results <- value.EventSeq
			}
		}(eventID)
	}
	wg.Wait()
	close(results)
	var sequences []int64
	for value := range results {
		sequences = append(sequences, value)
	}
	if len(sequences) != 2 || sequences[0] == sequences[1] {
		t.Fatalf("concurrent sequences = %v", sequences)
	}
}

func TestRedisHistoryOrderingAndDefensiveCopies(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	seedEvent(t, store, "tenant-a", "session", "inbound")
	payload := []byte(`{"id":1}`)
	first, err := store.AppendEventPayload(context.Background(), runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner-1", Payload: payload})
	if err != nil || first.HistorySeq != 1 {
		t.Fatalf("first history = %+v, %v", first, err)
	}
	first.Payload[0] = 'x'
	if _, err := store.AppendEventPayload(context.Background(), runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner-1", Payload: []byte(`{"id":1}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendEventPayload(context.Background(), runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner-1", Payload: []byte(`{"id":2}`)}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("history conflict = %v", err)
	}
	second, err := store.AppendEventPayload(context.Background(), runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner-2", Payload: []byte(`{"id":2}`)})
	if err != nil || second.HistorySeq != 2 {
		t.Fatalf("second history = %+v, %v", second, err)
	}
	items, err := store.ListEventPayloads(context.Background(), "tenant-a", "session")
	if err != nil || len(items) != 2 || items[0].HistorySeq != 1 || items[1].HistorySeq != 2 || string(items[0].Payload) != `{"id":1}` {
		t.Fatalf("history = %+v, %v", items, err)
	}
}

func TestRedisMessageLeaseExpiryAndFencing(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session", "event")
	running, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker-a", LeaseDuration: 15 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	reconciling, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: "worker-b"})
	if err != nil || reconciling.FencingToken != running.FencingToken+1 || reconciling.LeaseOwner != "" {
		t.Fatalf("reconciling = %+v, %v", reconciling, err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: event.EventID, From: runtimestorage.EventExecutionReconciling, To: runtimestorage.EventRunning, Owner: "worker-b", LeaseDuration: time.Second}); err != nil {
		t.Fatal(err)
	}
}

func TestRedisReplyBatchRetryAndDeadLetter(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session", "event")
	batch := []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 2, Payload: "one"},
		{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 2, Payload: "two"},
	}
	rows, err := store.EnqueueReplies(context.Background(), batch)
	if err != nil || len(rows) != 2 || rows[0].Status != runtimestorage.ReplyPending {
		t.Fatalf("batch = %+v, %v", rows, err)
	}
	if _, err := store.EnqueueReplies(context.Background(), []runtimestorage.ReplyOutbox{batch[0], {TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 2, Payload: "changed"}}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("batch conflict = %v", err)
	}
	claimed, err := store.ClaimReply(context.Background(), "tenant-a", "reply", 0, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplyRetryable, Owner: "worker-b", FencingToken: claimed.FencingToken}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale reply worker = %v", err)
	}
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplyRetryable, Owner: "worker-a", FencingToken: claimed.FencingToken, ErrorClass: "rate_limited"}); err != nil {
		t.Fatal(err)
	}
	retry, err := store.ClaimReply(context.Background(), "tenant-a", "reply", 0, "worker-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dead, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, From: runtimestorage.ReplySending, To: runtimestorage.ReplyDeadLetter, Owner: "worker-b", FencingToken: retry.FencingToken, ErrorClass: "permanent"})
	if err != nil || dead.Status != runtimestorage.ReplyDeadLetter || dead.LeaseOwner != "" || dead.LeaseExpiresAt != nil {
		t.Fatalf("dead letter = %+v, %v", dead, err)
	}
}

func TestRedisMemoryDurabilityAndIndexHandoff(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	value, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "memory", UserID: "user", Content: "likes coffee", Metadata: map[string]any{"kind": "fact"}, Embedding: []float64{1, 0}})
	if err != nil {
		t.Fatal(err)
	}
	value.Metadata["kind"] = "changed"
	if err := store.EnqueueMemoryIndex(context.Background(), value); err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForMemoryIndex(context.Background(), "tenant-a", "memory", value.Version); err != nil {
		t.Fatalf("index handoff = %v", err)
	}
	value2, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "memory", UserID: "user", Content: "likes tea"})
	if err != nil || value2.Version != 2 {
		t.Fatalf("memory update = %+v, %v", value2, err)
	}
	if err := store.WaitForMemoryIndex(context.Background(), "tenant-a", "memory", value2.Version); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("missing new handoff = %v", err)
	}
	if err := store.EnqueueMemoryIndex(context.Background(), value2); err != nil {
		t.Fatal(err)
	}
	if err := store.WaitForMemoryIndex(context.Background(), "tenant-a", "memory", value2.Version); err != nil {
		t.Fatal(err)
	}
	reopened := newStore(t, server)
	got, err := reopened.GetMemory(context.Background(), "tenant-a", "memory")
	if err != nil || got.Content != "likes tea" || got.Metadata == nil {
		t.Fatalf("reopened memory = %+v, %v", got, err)
	}
}

func TestRedisCancellationCloseOwnershipAndRedaction(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	borrowed, err := redisstore.New(client, "test:runtime:v1")
	if err != nil {
		t.Fatal(err)
	}
	if err := borrowed.Close(); err != nil || client.Ping(context.Background()).Err() != nil {
		t.Fatalf("borrowed close = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := borrowed.GetSession(canceled, "tenant-a", "session"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = %v", err)
	}
	addr := server.Addr()
	server.Close()
	if _, err := redisstore.NewFromConfig(context.Background(), redisstore.Config{Addr: addr, Password: "super-secret"}); err == nil || strings.Contains(err.Error(), addr) || strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("redacted unavailable error = %v", err)
	}
	_ = client.Close()
}

func TestRedisScopedKeyCannotCollide(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	event := seedEvent(t, store, "tenant-a", "session", "event")
	for _, reply := range []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "ab", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: "first"},
		{TenantID: "tenant-a", ReplyID: "a", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 1, Payload: "second"},
	} {
		if _, err := store.EnqueueReply(context.Background(), reply); err != nil {
			t.Fatal(err)
		}
	}
	for replyID, payload := range map[string]string{"ab": "first", "a": "second"} {
		got, err := store.GetReply(context.Background(), "tenant-a", replyID, 0)
		if err != nil || got.Payload != payload {
			t.Fatalf("reply %q = %+v, %v", replyID, got, err)
		}
	}
}
