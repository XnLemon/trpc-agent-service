package inmemory_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestCapabilitiesAreTenantScopedAndDefensive(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	value, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m1", UserID: "u1", Content: "likes coffee", Metadata: map[string]any{"kind": "fact"}, Embedding: []float64{1, 0}})
	if err != nil || value.Version != 1 {
		t.Fatalf("PutMemory = %+v, %v", value, err)
	}
	value.Metadata["kind"] = "changed"
	got, err := store.GetMemory(context.Background(), "tenant-a", "m1")
	if err != nil || got.Metadata["kind"] != "fact" {
		t.Fatalf("memory copy = %+v, %v", got, err)
	}
	if _, err := store.GetMemory(context.Background(), "tenant-b", "m1"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross tenant memory = %v", err)
	}
	if _, err := store.PutKnowledge(context.Background(), runtimestorage.KnowledgeDocument{TenantID: "tenant-a", DocumentID: "doc", Content: "coffee", Embedding: []float64{1, 0}}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutKnowledge(context.Background(), runtimestorage.KnowledgeDocument{TenantID: "tenant-b", DocumentID: "doc", Content: "tea", Embedding: []float64{0, 1}}); err != nil {
		t.Fatal(err)
	}
	results, err := store.SearchKnowledge(context.Background(), "tenant-a", []float64{1, 0}, 10)
	if err != nil || len(results) != 1 || results[0].Document.TenantID != "tenant-a" {
		t.Fatalf("tenant knowledge search = %+v, %v", results, err)
	}
}

func TestAsyncMemoryIndexAndObjects(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	value, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", MemoryID: "m-index", UserID: "u", Content: "indexed", Embedding: []float64{1, 0}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.WaitForMemoryIndex(ctx, "tenant-a", "m-index", value.Version); err != nil {
		t.Fatal(err)
	}
	indexed, err := store.SearchVectors(ctx, "tenant-a", []float64{1, 0}, 1)
	if err != nil || len(indexed) != 1 || indexed[0].Record.DocumentID != "m-index" {
		t.Fatalf("async vector index = %+v, %v", indexed, err)
	}
	info, err := store.PutObject(context.Background(), "tenant-a", "a/file.txt", strings.NewReader("hello"), "text/plain")
	if err != nil || info.Size != 5 {
		t.Fatalf("PutObject = %+v, %v", info, err)
	}
	body, gotInfo, err := store.GetObject(context.Background(), "tenant-a", "a/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	data, _ := io.ReadAll(body)
	if string(data) != "hello" || gotInfo.ETag != info.ETag {
		t.Fatalf("object = %q, %+v", data, gotInfo)
	}
	if _, _, err := store.GetObject(context.Background(), "tenant-b", "a/file.txt"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross tenant object = %v", err)
	}
}

func TestSummaryOrderingAndSharedBackendVisibility(t *testing.T) {
	backend := inmemory.NewBackend()
	nodeA, nodeB := inmemory.NewWithBackend(backend), inmemory.NewWithBackend(backend)
	defer nodeA.Close()
	_, err := nodeA.CreateSession(context.Background(), "tenant-a", "session-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodeA.PutSummary(context.Background(), runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session-1", FilterKey: "default", Text: "new", EventSeq: 2}); err != nil {
		t.Fatal(err)
	}
	if _, err := nodeB.PutSummary(context.Background(), runtimestorage.SummaryRecord{TenantID: "tenant-a", SessionID: "session-1", FilterKey: "default", Text: "stale", EventSeq: 1}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("stale summary = %v", err)
	}
	got, err := nodeB.GetSummary(context.Background(), "tenant-a", "session-1", "default")
	if err != nil || got.Text != "new" || got.EventSeq != 2 {
		t.Fatalf("shared summary = %+v, %v", got, err)
	}
	if _, err := nodeB.GetSummary(context.Background(), "tenant-b", "session-1", "default"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross tenant summary = %v", err)
	}
}

func TestCapabilitiesRejectCanceledContext(t *testing.T) {
	store := inmemory.New()
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "x"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("PutMemory = %v", err)
	}
	if _, err := store.SearchKnowledge(ctx, "tenant-a", []float64{1}, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchKnowledge = %v", err)
	}
	if err := store.DeleteVector(ctx, "tenant-a", "doc"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteVector = %v", err)
	}
}
