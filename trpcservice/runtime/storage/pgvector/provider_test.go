package pgvector_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	pgvector "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/pgvector"
)

func TestDeterministicEmbedderIsStableAndBounded(t *testing.T) {
	embedder := pgvector.NewDeterministicEmbedder(4)
	first, err := embedder.Embed(context.Background(), "same text")
	if err != nil {
		t.Fatal(err)
	}
	second, err := embedder.Embed(context.Background(), "same text")
	if err != nil || len(first) != 4 || len(second) != 4 {
		t.Fatalf("vectors = %v, %v, err=%v", first, second, err)
	}
	for index := range first {
		if first[index] != second[index] || first[index] < -1 || first[index] > 1 {
			t.Fatalf("unstable or unbounded vector = %v, %v", first, second)
		}
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := embedder.Embed(canceled, "text"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled embed = %v", err)
	}
}

func TestNewRejectsUnsafeConfiguration(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	cases := []pgvector.Config{
		{Schema: "public;drop"},
		{Collection: "knowledge-name"},
		{Dimension: 4097},
		{Workers: 33},
	}
	for _, config := range cases {
		if store, err := pgvector.New(db, "tenant-a", config); !errors.Is(err, runtimestorage.ErrInvalid) || store != nil {
			t.Fatalf("unsafe config = store %v, err %v", store, err)
		}
	}
	if store, err := pgvector.New(db, "", pgvector.Config{}); !errors.Is(err, runtimestorage.ErrInvalid) || store != nil {
		t.Fatalf("unsafe tenant = store %v, err %v", store, err)
	}
}

func TestUpsertPersistsBeforeAsynchronousIndexAndRejectsModelChanges(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	var embedCalled sync.Once
	embedDone := make(chan struct{})
	embedder := pgvector.EmbeddingFunc(func(context.Context, string) ([]float64, error) {
		embedCalled.Do(func() { close(embedDone) })
		return []float64{1, 0}, nil
	})
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1").WithArgs("tenant-a", "knowledge").WillReturnRows(sqlmock.NewRows([]string{"knowledge_id", "document_id", "chunk_id"}))
	store, err := pgvector.New(db, "tenant-a", pgvector.Config{Dimension: 2, Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"tenant_id", "knowledge_id", "document_id", "chunk_id", "source", "source_version", "content", "content_ref", "metadata", "embedding_model", "embedding_version", "embedding_dimension", "checksum", "version", "index_status", "attempts", "last_error_class", "deleted_at", "created_at", "updated_at"}).AddRow("tenant-a", "knowledge", "doc", "chunk", "", "", "text", "", []byte(`{"scope":"allowed"}`), "deterministic", "v1", 2, "checksum", int64(1), "pending", 0, "", nil, when, when)
	mock.ExpectQuery("INSERT INTO public\\.runtime_pgvector_documents").WithArgs("tenant-a", "knowledge", "knowledge", "doc", "chunk", "", "", "text", "", []byte(`{"scope":"allowed"}`), "deterministic", "v1", 2, "checksum", "pending", "").WillReturnRows(rows)
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WithArgs("tenant-a", "knowledge", "knowledge", "doc", "chunk").WillReturnRows(sqlmock.NewRows([]string{"content", "embedding_model", "embedding_version", "embedding_dimension", "version", "index_status", "embedding"}).AddRow("text", "deterministic", "v1", 2, 1, "pending", ""))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET embedding=").WithArgs("tenant-a", "knowledge", "knowledge", "doc", "chunk", "[1,0]", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	value, err := store.Upsert(context.Background(), pgvector.DocumentInput{KnowledgeID: "knowledge", DocumentID: "doc", ChunkID: "chunk", Content: "text", Metadata: map[string]any{"scope": "allowed"}, Checksum: "checksum"})
	if err != nil || value.IndexStatus != pgvector.IndexPending {
		t.Fatalf("upsert = %+v, %v", value, err)
	}
	select {
	case <-embedDone:
	case <-time.After(time.Second):
		t.Fatal("embedding worker did not run")
	}
	time.Sleep(20 * time.Millisecond)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upsert(context.Background(), pgvector.DocumentInput{KnowledgeID: "knowledge", DocumentID: "other", ChunkID: "chunk", Content: "text", EmbeddingModel: "other-model"}); !errors.Is(err, pgvector.ErrIncompatible) {
		t.Fatalf("model mismatch = %v", err)
	}
}

func TestSearchFiltersBeforeRankingAndCopiesResults(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1").WithArgs("tenant-a", "knowledge").WillReturnRows(sqlmock.NewRows([]string{"knowledge_id", "document_id", "chunk_id"}))
	store, err := pgvector.New(db, "tenant-a", pgvector.Config{Dimension: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"tenant_id", "knowledge_id", "document_id", "chunk_id", "source", "source_version", "content", "content_ref", "metadata", "embedding_model", "embedding_version", "embedding_dimension", "checksum", "version", "index_status", "attempts", "last_error_class", "deleted_at", "created_at", "updated_at", "embedding"})
	rows.AddRow("tenant-a", "k", "secret", "c", "", "", "secret", "", []byte(`{"scope":"denied"}`), "deterministic", "v1", 2, "secret", 1, "ready", 0, "", nil, when, when, "[1,0]")
	rows.AddRow("tenant-a", "k", "allowed", "c", "", "", "allowed", "", []byte(`{"scope":"allowed"}`), "deterministic", "v1", 2, "allowed", 1, "ready", 0, "", nil, when, when, "[0.8,0.6]")
	rows.AddRow("tenant-a", "k", "incompatible", "c", "", "", "incompatible", "", []byte(`{"scope":"allowed"}`), "other-model", "v1", 2, "incompatible", 1, "ready", 0, "", nil, when, when, "[1,0]")
	rows.AddRow("tenant-b", "k", "foreign", "c", "", "", "foreign", "", []byte(`{"scope":"allowed"}`), "deterministic", "v1", 2, "foreign", 1, "ready", 0, "", nil, when, when, "[1,0]")
	mock.ExpectQuery("SELECT tenant_id,knowledge_id,document_id,chunk_id").WithArgs("tenant-a", "knowledge", "deterministic", "v1", 2).WillReturnRows(rows)
	values, err := store.Search(context.Background(), []float64{1, 0}, pgvector.SearchOptions{Limit: 1, Metadata: map[string]string{"scope": "allowed"}})
	if err != nil || len(values) != 1 || values[0].Document.DocumentID != "allowed" {
		t.Fatalf("filtered search = %+v, %v", values, err)
	}
	values[0].Document.Metadata["scope"] = "mutated"
	if values[0].Document.Metadata["scope"] != "mutated" {
		t.Fatal("result metadata was not writable by caller")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewRecoversPendingDocuments(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	embedDone := make(chan struct{})
	embedder := pgvector.EmbeddingFunc(func(context.Context, string) ([]float64, error) {
		close(embedDone)
		return []float64{1, 0}, nil
	})
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1").WithArgs("tenant-a", "knowledge").WillReturnRows(sqlmock.NewRows([]string{"knowledge_id", "document_id", "chunk_id"}).AddRow("knowledge", "recovered", "chunk"))
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WithArgs("tenant-a", "knowledge", "knowledge", "recovered", "chunk").WillReturnRows(sqlmock.NewRows([]string{"content", "embedding_model", "embedding_version", "embedding_dimension", "version", "index_status", "embedding"}).AddRow("text", "deterministic", "v1", 2, 1, "pending", ""))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET embedding=").WithArgs("tenant-a", "knowledge", "knowledge", "recovered", "chunk", "[1,0]", 1).WillReturnResult(sqlmock.NewResult(0, 1))
	store, err := pgvector.New(db, "tenant-a", pgvector.Config{Dimension: 2, Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	select {
	case <-embedDone:
	case <-time.After(time.Second):
		t.Fatal("pending document was not recovered")
	}
	time.Sleep(20 * time.Millisecond)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCloseCancelsBlockingEmbedder(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1").WithArgs("tenant-a", "knowledge").WillReturnRows(sqlmock.NewRows([]string{"knowledge_id", "document_id", "chunk_id"}))
	started := make(chan struct{})
	release := make(chan struct{})
	embedder := pgvector.EmbeddingFunc(func(context.Context, string) ([]float64, error) {
		close(started)
		<-release
		return []float64{1, 0}, nil
	})
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := sqlmock.NewRows([]string{"tenant_id", "knowledge_id", "document_id", "chunk_id", "source", "source_version", "content", "content_ref", "metadata", "embedding_model", "embedding_version", "embedding_dimension", "checksum", "version", "index_status", "attempts", "last_error_class", "deleted_at", "created_at", "updated_at"}).AddRow("tenant-a", "knowledge", "doc", "chunk", "", "", "text", "", []byte(`{}`), "deterministic", "v1", 2, "checksum", int64(1), "pending", 0, "", nil, when, when)
	mock.ExpectQuery("INSERT INTO public\\.runtime_pgvector_documents").WillReturnRows(rows)
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnRows(sqlmock.NewRows([]string{"content", "embedding_model", "embedding_version", "embedding_dimension", "version", "index_status", "embedding"}).AddRow("text", "deterministic", "v1", 2, 1, "pending", ""))
	store, err := pgvector.New(db, "tenant-a", pgvector.Config{Dimension: 2, Embedder: embedder})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Upsert(context.Background(), pgvector.DocumentInput{KnowledgeID: "knowledge", DocumentID: "doc", ChunkID: "chunk", Content: "text", Checksum: "checksum"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("embedding worker did not start")
	}
	closed := make(chan struct{})
	go func() {
		_ = store.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close waited for a blocking embedder")
	}
	close(release)
}

func TestProviderCompatibilityBoundariesRejectForeignTenants(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1").WithArgs("tenant-a", "knowledge").WillReturnRows(sqlmock.NewRows([]string{"knowledge_id", "document_id", "chunk_id"}))
	store, err := pgvector.New(db, "tenant-a", pgvector.Config{Dimension: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	foreign := runtimestorage.KnowledgeDocument{TenantID: "tenant-b", DocumentID: "doc", Content: "text"}
	if _, err := store.PutDocument(context.Background(), pgvector.DocumentInput{}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("PutDocument invalid input = %v", err)
	}
	if _, err := store.GetDocument(context.Background(), "", "", ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("GetDocument invalid key = %v", err)
	}
	if _, err := store.SearchDocuments(context.Background(), []float64{1}, pgvector.SearchOptions{}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("SearchDocuments invalid embedding = %v", err)
	}
	if err := store.Reindex(context.Background(), "", "", ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("Reindex invalid key = %v", err)
	}
	if _, err := store.PutKnowledge(context.Background(), foreign); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("PutKnowledge foreign tenant = %v", err)
	}
	if _, err := store.GetKnowledge(context.Background(), "tenant-b", "doc"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("GetKnowledge foreign tenant = %v", err)
	}
	if _, err := store.SearchKnowledge(context.Background(), "tenant-b", []float64{1, 0}, 1); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("SearchKnowledge foreign tenant = %v", err)
	}
	if err := store.DeleteKnowledge(context.Background(), "tenant-b", "doc"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("DeleteKnowledge foreign tenant = %v", err)
	}
	vector := runtimestorage.VectorRecord{TenantID: "tenant-b", DocumentID: "doc"}
	if err := store.UpsertVector(context.Background(), vector); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("UpsertVector foreign tenant = %v", err)
	}
	if _, err := store.SearchVectors(context.Background(), "tenant-b", []float64{1, 0}, 1); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("SearchVectors foreign tenant = %v", err)
	}
	if err := store.DeleteVector(context.Background(), "tenant-b", "doc"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("DeleteVector foreign tenant = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProviderPingAndDeleteBoundaries(t *testing.T) {
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1").WithArgs("tenant-a", "knowledge").WillReturnRows(sqlmock.NewRows([]string{"knowledge_id", "document_id", "chunk_id"}))
	store, err := pgvector.New(db, "tenant-a", pgvector.Config{Dimension: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	mock.ExpectPing()
	if err := store.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status='deleted'").WithArgs("tenant-a", "knowledge", "knowledge", "doc", "chunk").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.Delete(context.Background(), "knowledge", "doc", "chunk"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddingFailureIsFencedAndClassified(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1").WithArgs("tenant-a", "knowledge").WillReturnRows(sqlmock.NewRows([]string{"knowledge_id", "document_id", "chunk_id"}))
	embedder := pgvector.EmbeddingFunc(func(context.Context, string) ([]float64, error) {
		return nil, pgvector.ErrEmbedding
	})
	store, err := pgvector.New(db, "tenant-a", pgvector.Config{Dimension: 2, Embedder: embedder, MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	row := sqlmock.NewRows([]string{"tenant_id", "knowledge_id", "document_id", "chunk_id", "source", "source_version", "content", "content_ref", "metadata", "embedding_model", "embedding_version", "embedding_dimension", "checksum", "version", "index_status", "attempts", "last_error_class", "deleted_at", "created_at", "updated_at"}).AddRow("tenant-a", "knowledge", "doc", "chunk", "", "", "text", "", []byte(`{}`), "deterministic", "v1", 2, "checksum", int64(1), "pending", 0, "", nil, when, when)
	mock.ExpectQuery("INSERT INTO public\\.runtime_pgvector_documents").WillReturnRows(row)
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnRows(sqlmock.NewRows([]string{"content", "embedding_model", "embedding_version", "embedding_dimension", "version", "index_status", "embedding"}).AddRow("text", "deterministic", "v1", 2, 1, "pending", ""))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status=CASE").WithArgs("tenant-a", "knowledge", "knowledge", "doc", "chunk", 1, 2, "provider_error").WillReturnResult(sqlmock.NewResult(0, 1))
	if _, err := store.Upsert(context.Background(), pgvector.DocumentInput{KnowledgeID: "knowledge", DocumentID: "doc", ChunkID: "chunk", Content: "text", Checksum: "checksum"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
