package redis_test

import (
	"context"
	"encoding/hex"
	"errors"
	"math"
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

func TestRedisSessionCASAndDeleteCascade(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	created, err := store.CreateSession(ctx, "tenant-a", "session", map[string]any{"nested": map[string]any{"value": "one"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); !errors.Is(err, runtimestorage.ErrDuplicate) {
		t.Fatalf("duplicate session = %v", err)
	}
	if _, err := store.UpdateSessionState(ctx, "tenant-a", "session", created.Version+1, nil); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale session update = %v", err)
	}
	updated, err := store.UpdateSessionState(ctx, "tenant-a", "session", created.Version, map[string]any{"nested": map[string]any{"value": "two"}})
	if err != nil || updated.Version != created.Version+1 {
		t.Fatalf("session update = %+v, %v", updated, err)
	}
	updated.State["nested"].(map[string]any)["value"] = "mutated"
	got, err := store.GetSession(ctx, "tenant-a", "session")
	if err != nil || got.State["nested"].(map[string]any)["value"] != "two" {
		t.Fatalf("session copy = %+v, %v", got, err)
	}

	event, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"})
	if err != nil || duplicate {
		t.Fatalf("event = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	if _, err := store.AppendEventPayload(ctx, runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "runner", Payload: []byte(`{"kind":"event"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: event.EventID, SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteSession(ctx, "tenant-a", "session"); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func() error{
		"session": func() error { _, err := store.GetSession(ctx, "tenant-a", "session"); return err },
		"event":   func() error { _, err := store.GetMessage(ctx, "tenant-a", event.EventID); return err },
		"reply":   func() error { _, err := store.GetReply(ctx, "tenant-a", "reply", 0); return err },
		"history": func() error { _, err := store.ListEventPayloads(ctx, "tenant-a", "session"); return err },
	} {
		if err := check(); !errors.Is(err, runtimestorage.ErrNotFound) {
			t.Fatalf("deleted %s = %v", name, err)
		}
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	if _, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"}); err != nil || duplicate {
		t.Fatalf("reused inbound identity duplicate=%v err=%v", duplicate, err)
	}
}

func TestRedisReplyCorrelationAndCandidateOrdering(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	const traceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	target := runtimestorage.ReplyTarget{BindingID: "binding", ConversationKind: "direct", ReceiverID: "receiver"}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); err != nil {
		t.Fatal(err)
	}
	event, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event", ReplyTarget: target})
	if err != nil || duplicate {
		t.Fatalf("event = %+v duplicate=%v err=%v", event, duplicate, err)
	}
	correlation := runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: event.EventID, RequestID: "request", TraceID: "trace", TraceParent: traceParent}
	batch := []runtimestorage.ReplyOutbox{
		{TenantID: "tenant-a", ReplyID: "reply-b", EventID: event.EventID, SegmentIndex: 1, SegmentCount: 2, Payload: "two", ReplyTarget: target},
		{TenantID: "tenant-a", ReplyID: "reply-b", EventID: event.EventID, SegmentIndex: 0, SegmentCount: 2, Payload: "one", ReplyTarget: target},
	}
	if _, err := store.EnqueueRepliesWithCorrelation(ctx, correlation, batch); err != nil {
		t.Fatal(err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(ctx, correlation, batch); err != nil {
		t.Fatalf("idempotent correlated enqueue = %v", err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(ctx, runtimestorage.ReplyCorrelation{TenantID: "tenant-a", EventID: event.EventID, RequestID: "other"}, batch); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("correlation conflict = %v", err)
	}
	got, err := store.GetReplyCorrelation(ctx, "tenant-a", event.EventID)
	if err != nil || got != correlation {
		t.Fatalf("correlation = %+v, %v", got, err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-a", EventID: event.EventID, SegmentCount: 1, Payload: "first", ReplyTarget: target}); err != nil {
		t.Fatal(err)
	}
	candidates, err := store.ListReplyCandidates(ctx, "tenant-a")
	if err != nil || len(candidates) != 3 || candidates[0].ReplyID != "reply-a" || candidates[1].SegmentIndex != 0 || candidates[2].SegmentIndex != 1 {
		t.Fatalf("sorted candidates = %+v, %v", candidates, err)
	}
	if _, err := store.GetReplyCorrelation(ctx, "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing correlation = %v", err)
	}
}

func TestRedisMemoryQueriesAndTombstone(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	for _, value := range []runtimestorage.MemoryInput{
		{TenantID: "tenant-a", MemoryID: "both", UserID: "user", Content: "coffee and tea", Topics: []string{"drink"}, Metadata: map[string]any{"source": "test"}, Embedding: []float64{1, 0}},
		{TenantID: "tenant-a", MemoryID: "coffee", UserID: "user", Content: "coffee", Embedding: []float64{0, 1}},
		{TenantID: "tenant-a", MemoryID: "other-user", UserID: "other", Content: "coffee and tea"},
	} {
		if _, err := store.PutMemory(ctx, value); err != nil {
			t.Fatal(err)
		}
	}
	values, err := store.ListMemories(ctx, "tenant-a", "user", 1)
	if err != nil || len(values) != 1 || values[0].UserID != "user" {
		t.Fatalf("limited memories = %+v, %v", values, err)
	}
	results, err := store.SearchMemories(ctx, "tenant-a", "user", "coffee tea", 10)
	if err != nil || len(results) != 2 || results[0].Memory.MemoryID != "both" || results[0].Score != 1 || results[1].Memory.MemoryID != "coffee" || results[1].Score != 0.5 {
		t.Fatalf("memory search = %+v, %v", results, err)
	}
	record, err := store.GetMemory(ctx, "tenant-a", "both")
	if err != nil {
		t.Fatal(err)
	}
	record.Topics[0] = "changed"
	record.Metadata["source"] = "changed"
	record.Embedding[0] = 9
	persisted, err := store.GetMemory(ctx, "tenant-a", "both")
	if err != nil || persisted.Topics[0] != "drink" || persisted.Metadata["source"] != "test" || persisted.Embedding[0] != 1 {
		t.Fatalf("memory defensive copy = %+v, %v", persisted, err)
	}
	if err := store.EnqueueMemoryIndex(ctx, persisted); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteMemory(ctx, "tenant-a", "both"); err != nil {
		t.Fatal(err)
	}
	for name, check := range map[string]func() error{
		"read":  func() error { _, err := store.GetMemory(ctx, "tenant-a", "both"); return err },
		"index": func() error { return store.EnqueueMemoryIndex(ctx, persisted) },
		"wait":  func() error { return store.WaitForMemoryIndex(ctx, "tenant-a", "both", persisted.Version) },
	} {
		if err := check(); !errors.Is(err, runtimestorage.ErrNotFound) {
			t.Fatalf("deleted memory %s = %v", name, err)
		}
	}
	values, err = store.ListMemories(ctx, "tenant-a", "user", 10)
	if err != nil || len(values) != 1 || values[0].MemoryID != "coffee" {
		t.Fatalf("tombstoned memories = %+v, %v", values, err)
	}
}

func TestRedisOwnedConstructorsAndPing(t *testing.T) {
	server := miniredis.RunT(t)
	ctx := context.Background()
	if _, err := redisstore.New(nil, ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil client = %v", err)
	}
	if _, err := redisstore.NewFromURL(ctx, "not a redis URL"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid url = %v", err)
	}
	fromURL, err := redisstore.NewFromURL(ctx, "redis://"+server.Addr()+"/2")
	if err != nil {
		t.Fatal(err)
	}
	if err := fromURL.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fromURL.Close(); err != nil {
		t.Fatal(err)
	}
	if err := fromURL.Ping(ctx); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("closed url client ping = %v", err)
	}
	fromConfig, err := redisstore.NewFromConfig(ctx, redisstore.Config{Addr: server.Addr(), DB: 1, KeyPrefix: "custom", PoolSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := fromConfig.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := fromConfig.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRedisValidationAndCancellationBoundaries(t *testing.T) {
	server := miniredis.RunT(t)
	store := newStore(t, server)
	ctx := context.Background()
	var nilStore *redisstore.Store
	if err := nilStore.Ping(ctx); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil store ping = %v", err)
	}
	if err := store.Ping(nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil context ping = %v", err)
	}
	if _, err := store.GetSession(nil, "tenant-a", "session"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil context read = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := redisstore.NewFromURL(canceled, "redis://"+server.Addr()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled URL constructor = %v", err)
	}
	if _, err := redisstore.NewFromConfig(canceled, redisstore.Config{Addr: server.Addr()}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled config constructor = %v", err)
	}
	for name, err := range map[string]error{
		"empty config address": func() error { _, err := redisstore.NewFromConfig(ctx, redisstore.Config{}); return err }(),
		"negative config DB": func() error {
			_, err := redisstore.NewFromConfig(ctx, redisstore.Config{Addr: server.Addr(), DB: -1})
			return err
		}(),
		"nil URL context": func() error { _, err := redisstore.NewFromURL(nil, "redis://"+server.Addr()); return err }(),
		"empty URL":       func() error { _, err := redisstore.NewFromURL(ctx, ""); return err }(),
	} {
		if !errors.Is(err, runtimestorage.ErrInvalid) {
			t.Fatalf("%s = %v", name, err)
		}
	}
	if _, err := store.GetReplyCorrelation(ctx, "", "event"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid correlation read = %v", err)
	}
	if _, err := store.GetReplyCorrelation(ctx, "tenant-a", ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty correlation event = %v", err)
	}
	if _, err := store.GetSession(ctx, "", "session"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid session tenant = %v", err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "", nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty session ID = %v", err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "bad-state", map[string]any{"channel": make(chan int)}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("unencodable session state = %v", err)
	}
	if _, err := store.UpdateSessionState(ctx, "tenant-a", "missing", 1, map[string]any{"channel": make(chan int)}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("unencodable update state = %v", err)
	}
	if err := store.DeleteSession(ctx, "", "session"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid session delete = %v", err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session"}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("incomplete inbound event = %v", err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event", ReplyTarget: runtimestorage.ReplyTarget{BindingID: "other", ConversationKind: "direct", ReceiverID: "receiver"}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("mismatched reply target = %v", err)
	}
	for name, transition := range map[string]runtimestorage.MessageTransition{
		"missing owner":         {TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventFailed},
		"illegal transition":    {TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventCompleted, Owner: "worker"},
		"running without lease": {TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker"},
	} {
		if _, err := store.TransitionMessage(ctx, transition); (name == "illegal transition" && !errors.Is(err, runtimestorage.ErrIllegalTransition)) || (name != "illegal transition" && !errors.Is(err, runtimestorage.ErrInvalid)) {
			t.Fatalf("%s = %v", name, err)
		}
	}
	if _, err := store.GetMessage(ctx, "", "event"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid message read = %v", err)
	}
	if _, err := store.AppendEventPayload(ctx, runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "event", Payload: []byte("not-json")}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid history payload = %v", err)
	}
	if _, err := store.ListEventPayloads(ctx, "", "session"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid history tenant = %v", err)
	}
	invalidReply := runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 0, SegmentCount: 1, Status: runtimestorage.ReplySent}
	if _, err := store.EnqueueReply(ctx, invalidReply); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("non-pending reply = %v", err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 1, SegmentCount: 1}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("out-of-range reply segment = %v", err)
	}
	if _, err := store.EnqueueReplies(ctx, []runtimestorage.ReplyOutbox{{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 0, SegmentCount: 2}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("incomplete reply batch = %v", err)
	}
	if _, err := store.EnqueueRepliesWithCorrelation(ctx, runtimestorage.ReplyCorrelation{}, nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty correlation enqueue = %v", err)
	}
	if _, err := store.GetReply(ctx, "tenant-a", "", 0); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty reply ID = %v", err)
	}
	if _, err := store.ListReplyCandidates(ctx, ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid candidates tenant = %v", err)
	}
	if _, err := store.ClaimReply(ctx, "tenant-a", "reply", 0, "", time.Second); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty reply owner = %v", err)
	}
	if _, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", SegmentIndex: 0, From: runtimestorage.ReplySent, To: runtimestorage.ReplySending, Owner: "worker"}); !errors.Is(err, runtimestorage.ErrIllegalTransition) {
		t.Fatalf("illegal reply transition = %v", err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "", Content: "content"}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty memory user = %v", err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content", Embedding: []float64{math.NaN()}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("non-finite memory embedding = %v", err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content", Metadata: map[string]any{"channel": make(chan int)}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("unencodable memory metadata = %v", err)
	}
	if _, err := store.GetMemory(ctx, "", "memory"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid memory read = %v", err)
	}
	if _, err := store.ListMemories(ctx, "tenant-a", "", 0); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty memory user = %v", err)
	}
	if _, err := store.ListMemories(ctx, "tenant-a", "user", -1); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("negative memory limit = %v", err)
	}
	if _, err := store.SearchMemories(ctx, "tenant-a", "user", "", 0); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty memory query = %v", err)
	}
	if err := store.DeleteMemory(ctx, "tenant-a", ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty memory delete = %v", err)
	}
	if err := store.EnqueueMemoryIndex(ctx, runtimestorage.MemoryRecord{TenantID: "tenant-a", MemoryID: "memory"}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid memory index = %v", err)
	}
	if err := store.WaitForMemoryIndex(ctx, "tenant-a", "memory", 0); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid memory handoff wait = %v", err)
	}
}

func TestRedisMalformedStateAndUnavailableCommandsFailClosed(t *testing.T) {
	server := miniredis.RunT(t)
	client := redisclient.NewClient(&redisclient.Options{Addr: server.Addr()})
	store, err := redisstore.New(client, "test:runtime:v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close(); _ = client.Close() })
	ctx := context.Background()
	key := "test:runtime:v1:" + hex.EncodeToString([]byte("tenant-a"))
	if err := client.Set(ctx, key, "{", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSession(ctx, "tenant-a", "session"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("malformed state read = %v", err)
	}
	if err := client.Set(ctx, key, `{"version":99}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetSession(ctx, "tenant-a", "session"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("unsupported state version = %v", err)
	}
	server.Close()
	if _, err := store.GetSession(ctx, "tenant-a", "session"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("unavailable read = %v", err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("unavailable mutation = %v", err)
	}
}
