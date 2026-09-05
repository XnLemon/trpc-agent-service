package pgvector

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestPureHelpersRejectInvalidValuesAndPreserveOwnership(t *testing.T) {
	if got := NewDeterministicEmbedder(0); got.dimension != defaultDimension {
		t.Fatalf("default dimension = %d", got.dimension)
	}
	if _, err := (DeterministicEmbedder{}).Embed(nil, "text"); !errors.Is(err, context.Canceled) {
		t.Fatalf("nil context error = %v", err)
	}
	if _, err := encodeVector(nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty vector error = %v", err)
	}
	if _, err := encodeVector([]float64{math.NaN()}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid vector error = %v", err)
	}
	if _, err := decodeVector(""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty encoded vector error = %v", err)
	}
	if _, err := decodeVector("[NaN]"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("invalid encoded vector error = %v", err)
	}
	if got, err := decodeVector("[1, 2]"); err != nil || len(got) != 2 {
		t.Fatalf("decoded vector = %v, %v", got, err)
	}
	if defaultString("", "fallback") != "fallback" || defaultString("value", "fallback") != "value" {
		t.Fatal("default string did not preserve explicit value")
	}
}

func TestPureHelpersValidateKeysAndOwnership(t *testing.T) {
	for _, value := range []string{"", "bad-name", "a" + string(make([]byte, 64))} {
		if validIdentifier(value) && value != "" && value != "bad-name" {
			t.Fatalf("invalid identifier accepted: %q", value)
		}
	}
	if validModelValue("model value") || validModelValue("") || validKey("line\nbreak", false) {
		t.Fatal("invalid model/key accepted")
	}
	metadata := map[string]any{"scope": "allowed"}
	copyValue := cloneDocument(Document{Metadata: metadata, DeletedAt: func() *time.Time { value := time.Now(); return &value }()})
	copyValue.Metadata["scope"] = "changed"
	if metadata["scope"] != "allowed" || copyValue.DeletedAt == nil {
		t.Fatal("document ownership was not preserved")
	}
	if cloneMap(map[string]any{"bad": func() {}}) != nil {
		t.Fatal("unsupported metadata was accepted")
	}
}

func TestPureSearchAndQueueBoundaries(t *testing.T) {
	if _, ok := cosine(nil, []float64{1}); ok {
		t.Fatal("empty cosine was accepted")
	}
	if _, ok := cosine([]float64{0}, []float64{1}); ok {
		t.Fatal("zero-norm cosine was accepted")
	}
	if _, ok := cosine([]float64{1}, []float64{1, 0}); ok {
		t.Fatal("mismatched cosine dimensions were accepted")
	}
	if metadataMatches(map[string]any{"scope": "allowed"}, map[string]string{"scope": "denied"}) {
		t.Fatal("metadata mismatch was accepted")
	}
	values := []SearchResult{{Document: Document{KnowledgeID: "z", DocumentID: "same", ChunkID: "a"}, Score: 0.5}, {Document: Document{KnowledgeID: "a", DocumentID: "same", ChunkID: "z"}, Score: 0.5}, {Document: Document{KnowledgeID: "a", DocumentID: "same", ChunkID: "a"}, Score: 0.5}}
	sortSearchResults(values)
	if values[0].Document.KnowledgeID != "a" || values[0].Document.ChunkID != "a" || len(limitSearchResults(values, 2)) != 2 {
		t.Fatalf("search ordering/limit = %+v", values)
	}
	store := &Store{queue: make(chan indexJob, 1), stop: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if !errors.Is(store.enqueue(ctx, documentKey{}), context.Canceled) {
		t.Fatal("canceled enqueue was accepted")
	}
	store.queue <- indexJob{}
	if !errors.Is(store.enqueue(context.Background(), documentKey{}), runtimestorage.ErrConflict) {
		t.Fatal("full enqueue was accepted")
	}
	close(store.stop)
	if !errors.Is(store.enqueue(context.Background(), documentKey{}), ErrClosed) {
		t.Fatal("closed enqueue was accepted")
	}
}

func TestNormalizeConfigRejectsInvalidLimits(t *testing.T) {
	cases := []Config{
		{EmbeddingModel: "bad model"},
		{Dimension: 2001},
		{QueueSize: 10001},
		{MaxAttempts: 101},
	}
	for _, value := range cases {
		if _, err := normalizeConfig(value); !errors.Is(err, runtimestorage.ErrInvalid) {
			t.Fatalf("config %#v error = %v", value, err)
		}
	}
}

func TestPrepareUpsertRejectsInvalidMetadataAndCompatibility(t *testing.T) {
	store := &Store{model: "deterministic", modelVer: "v1", dimension: 2}
	if _, _, _, err := store.prepareUpsert(DocumentInput{KnowledgeID: "k", DocumentID: "d", ChunkID: "c", Content: "text", Metadata: map[string]any{"unsupported": func() {}}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("unsupported metadata error = %v", err)
	}
	if _, _, _, err := store.prepareUpsert(DocumentInput{KnowledgeID: "k", DocumentID: "d", ChunkID: "c", Content: "text", EmbeddingModel: "other", Metadata: map[string]any{}}); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("model mismatch error = %v", err)
	}
	if _, _, _, err := store.prepareUpsert(DocumentInput{KnowledgeID: "k", DocumentID: "d", ChunkID: "c", Content: "text", Checksum: "line\nbreak", Metadata: map[string]any{}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("checksum error = %v", err)
	}
	if _, _, _, err := store.prepareUpsert(DocumentInput{KnowledgeID: "k", DocumentID: "d", ChunkID: "c", Content: "text", Embedding: []float64{1, 2, 3}, Metadata: map[string]any{}}); !errors.Is(err, ErrIncompatible) {
		t.Fatalf("dimension mismatch error = %v", err)
	}
}

func TestPureLifecycleBoundaries(t *testing.T) {
	if _, err := (DeterministicEmbedder{}).Embed(context.Background(), "text"); err != nil {
		t.Fatal(err)
	}
	var nilStore *Store
	if err := nilStore.Close(); err != nil {
		t.Fatal(err)
	}
	if err := nilStore.check(context.Background()); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("nil store check = %v", err)
	}
	if err := (&Store{}).check(nil); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("nil context check = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&Store{}).check(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled check = %v", err)
	}
	closed := &Store{db: &sql.DB{}, stop: make(chan struct{})}
	close(closed.stop)
	if err := closed.check(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed check = %v", err)
	}
	if err := closed.DeleteVector(context.Background(), "tenant-a", "doc"); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed vector delete = %v", err)
	}
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", stop: make(chan struct{})}
	mock.ExpectPing()
	if err := store.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	mock.ExpectPing().WillReturnError(errors.New("down"))
	if err := store.Ping(context.Background()); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("failed ping = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryAndReconcileBoundaries(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", queue: make(chan indexJob, 2), stop: make(chan struct{}), workerCtx: context.Background(), queries: newDocumentQueries("public.runtime_pgvector_documents")}
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1 AND collection=\\$2 AND index_status='pending'").WillReturnError(errors.New("database unavailable"))
	if err := store.recoverPending(); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("recovery error = %v", err)
	}
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1 AND collection=\\$2.*index_status IN").WithArgs("tenant-a", "knowledge").WillReturnRows(sqlmock.NewRows([]string{"knowledge_id", "document_id", "chunk_id"}).AddRow("knowledge", "repair", "chunk"))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status='pending'").WithArgs("tenant-a", "knowledge", "knowledge", "repair", "chunk").WillReturnResult(sqlmock.NewResult(0, 1))
	if count, err := store.Reconcile(context.Background()); err != nil || count != 1 {
		t.Fatalf("reconcile = %d, %v", count, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRetryDeleteAndGetErrorsMapToStorageContracts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", queue: make(chan indexJob, 1), stop: make(chan struct{}), queries: newDocumentQueries("public.runtime_pgvector_documents")}
	mock.ExpectQuery("SELECT tenant_id,knowledge_id,document_id,chunk_id").WillReturnError(sql.ErrNoRows)
	if _, err := store.Get(context.Background(), "knowledge", "missing", "chunk"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing get = %v", err)
	}
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status='deleted'").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := store.Delete(context.Background(), "knowledge", "missing", "chunk"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing delete = %v", err)
	}
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status='pending'").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := store.Retry(context.Background(), "knowledge", "missing", "chunk"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing retry = %v", err)
	}
	mock.ExpectQuery("SELECT knowledge_id,document_id,chunk_id FROM public\\.runtime_pgvector_documents WHERE tenant_id=\\$1 AND collection=\\$2.*index_status IN").WithArgs("tenant-a", "knowledge").WillReturnError(errors.New("down"))
	if _, err := store.Reconcile(context.Background()); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("failed reconcile = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetExcludesDeletedRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", stop: make(chan struct{}), queries: newDocumentQueries("public.runtime_pgvector_documents")}
	mock.ExpectQuery("SELECT tenant_id,knowledge_id,document_id,chunk_id.*deleted_at IS NULL").WithArgs("tenant-a", "knowledge", "deleted", "deleted", "default").WillReturnError(sql.ErrNoRows)
	if _, err := store.GetKnowledge(context.Background(), "tenant-a", "deleted"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("deleted knowledge get = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestScanBoundariesRejectMalformedRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", dimension: 2, model: "deterministic", modelVer: "v1", stop: make(chan struct{}), queries: newDocumentQueries("public.runtime_pgvector_documents")}
	mock.ExpectQuery("SELECT tenant_id,knowledge_id,document_id,chunk_id").WillReturnRows(sqlmock.NewRows([]string{"tenant_id"}).AddRow("tenant-a"))
	if _, err := store.Get(context.Background(), "knowledge", "bad", "chunk"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("malformed get = %v", err)
	}
	when := time.Now().UTC()
	rows := sqlmock.NewRows([]string{"tenant_id", "knowledge_id", "document_id", "chunk_id", "source", "source_version", "content", "content_ref", "metadata", "embedding_model", "embedding_version", "embedding_dimension", "checksum", "version", "index_status", "attempts", "last_error_class", "deleted_at", "created_at", "updated_at", "embedding"})
	rows.AddRow("tenant-a", "knowledge", "doc", "chunk", "", "", "text", "", []byte("not-json"), "deterministic", "v1", 2, "digest", 1, "ready", 0, "", nil, when, when, "[1,0]")
	mock.ExpectQuery("SELECT tenant_id,knowledge_id,document_id,chunk_id").WillReturnRows(rows)
	if _, err := store.Search(context.Background(), []float64{1, 0}, SearchOptions{Limit: 1}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("malformed search = %v", err)
	}
}

func TestSearchAndMutationErrorsAreMapped(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", model: "deterministic", modelVer: "v1", dimension: 2, stop: make(chan struct{}), queue: make(chan indexJob, 1), queries: newDocumentQueries("public.runtime_pgvector_documents")}
	if _, err := store.Search(context.Background(), []float64{1, 0}, SearchOptions{Metadata: map[string]string{"": "value"}}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid search filter = %v", err)
	}
	mock.ExpectQuery("SELECT tenant_id,knowledge_id,document_id,chunk_id").WillReturnError(errors.New("query down"))
	if _, err := store.Search(context.Background(), []float64{1, 0}, SearchOptions{}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("failed search = %v", err)
	}
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status='deleted'").WillReturnError(errors.New("delete down"))
	if err := store.Delete(context.Background(), "knowledge", "doc", "chunk"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("failed delete = %v", err)
	}
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status='pending'").WillReturnError(errors.New("retry down"))
	if err := store.Retry(context.Background(), "knowledge", "doc", "chunk"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("failed retry = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexClassifiesCompatibilityAndRetriesStorageFailures(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	queries := newDocumentQueries("public.runtime_pgvector_documents")
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", model: "deterministic", modelVer: "v1", dimension: 2, maxAttempts: 2, queries: queries}
	row := func(model string, dimension int, vector string) *sqlmock.Rows {
		return sqlmock.NewRows([]string{"content", "embedding_model", "embedding_version", "embedding_dimension", "version", "index_status", "embedding"}).AddRow("text", model, "v1", dimension, 1, "pending", vector)
	}
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnRows(row("other", 2, ""))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status=CASE").WillReturnResult(sqlmock.NewResult(0, 1))
	if store.index(context.Background(), documentKey{knowledgeID: "k", documentID: "d", chunkID: "c"}) {
		t.Fatal("incompatible row was requeued")
	}
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnRows(sqlmock.NewRows([]string{"content", "embedding_model", "embedding_version", "embedding_dimension", "version", "index_status", "embedding"}).AddRow("text", "other", "v1", 2, 1, "dead_letter", ""))
	if store.index(context.Background(), documentKey{knowledgeID: "k", documentID: "d", chunkID: "c"}) {
		t.Fatal("dead-letter row was requeued")
	}
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnRows(row("deterministic", 2, "[1,0]"))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET embedding=").WillReturnResult(sqlmock.NewResult(0, 1))
	if store.index(context.Background(), documentKey{knowledgeID: "k", documentID: "d", chunkID: "c"}) {
		t.Fatal("ready row was requeued")
	}
	store.embedder = EmbeddingFunc(func(context.Context, string) ([]float64, error) { return []float64{1}, nil })
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnRows(row("deterministic", 2, ""))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status=CASE").WillReturnResult(sqlmock.NewResult(0, 1))
	if store.index(context.Background(), documentKey{knowledgeID: "k", documentID: "d", chunkID: "c"}) {
		t.Fatal("incompatible dimension was requeued")
	}
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnError(errors.New("temporary"))
	if !store.index(context.Background(), documentKey{knowledgeID: "k", documentID: "d", chunkID: "c"}) {
		t.Fatal("load failure was not requeued")
	}
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnError(sql.ErrNoRows)
	if store.index(context.Background(), documentKey{knowledgeID: "k", documentID: "d", chunkID: "c"}) {
		t.Fatal("missing row was requeued")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestIndexRetriesWhenFailureStateCannotBePersisted(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", model: "deterministic", modelVer: "v1", dimension: 2, maxAttempts: 2, queries: newDocumentQueries("public.runtime_pgvector_documents")}
	mock.ExpectQuery("SELECT content,embedding_model,embedding_version").WillReturnRows(sqlmock.NewRows([]string{"content", "embedding_model", "embedding_version", "embedding_dimension", "version", "index_status", "embedding"}).AddRow("text", "deterministic", "v1", 2, 1, "pending", "[1,0]"))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET embedding=").WillReturnError(errors.New("index down"))
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status=CASE").WillReturnError(errors.New("state down"))
	if !store.index(context.Background(), documentKey{knowledgeID: "k", documentID: "d", chunkID: "c"}) {
		t.Fatal("failed state persistence was not retried")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestVectorAdapterPersistsSearchesAndDeletes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	queries := newDocumentQueries("public.runtime_pgvector_documents")
	store := &Store{db: db, tenantID: "tenant-a", collection: "knowledge", model: "deterministic", modelVer: "v1", dimension: 2, queue: make(chan indexJob, 1), stop: make(chan struct{}), queries: queries}
	when := time.Now().UTC()
	row := sqlmock.NewRows([]string{"tenant_id", "knowledge_id", "document_id", "chunk_id", "source", "source_version", "content", "content_ref", "metadata", "embedding_model", "embedding_version", "embedding_dimension", "checksum", "version", "index_status", "attempts", "last_error_class", "deleted_at", "created_at", "updated_at"}).AddRow("tenant-a", "generic", "doc", "vector", "generic", "", "content", "", []byte(`{}`), "deterministic", "v1", 2, "digest", 1, "pending", 0, "", nil, when, when)
	mock.ExpectQuery("INSERT INTO public\\.runtime_pgvector_documents").WithArgs("tenant-a", "knowledge", "generic", "doc", "vector", "generic", "", "content", "", []byte(`{}`), "deterministic", "v1", 2, sqlmock.AnyArg(), "pending", "[1,0]").WillReturnRows(row)
	if err := store.UpsertVector(context.Background(), runtimestorage.VectorRecord{TenantID: "tenant-a", DocumentID: "doc", Content: "content", Embedding: []float64{1, 0}}); err != nil {
		t.Fatal(err)
	}
	searchRows := sqlmock.NewRows([]string{"tenant_id", "knowledge_id", "document_id", "chunk_id", "source", "source_version", "content", "content_ref", "metadata", "embedding_model", "embedding_version", "embedding_dimension", "checksum", "version", "index_status", "attempts", "last_error_class", "deleted_at", "created_at", "updated_at", "embedding"}).AddRow("tenant-a", "generic", "doc", "vector", "generic", "", "content", "", []byte(`{}`), "deterministic", "v1", 2, "digest", 1, "ready", 0, "", nil, when, when, "[1,0]")
	mock.ExpectQuery("SELECT tenant_id,knowledge_id,document_id,chunk_id").WithArgs("tenant-a", "knowledge", "deterministic", "v1", 2).WillReturnRows(searchRows)
	values, err := store.SearchVectors(context.Background(), "tenant-a", []float64{1, 0}, 1)
	if err != nil || len(values) != 1 || values[0].Record.Source != runtimestorage.VectorSourceGeneric {
		t.Fatalf("vector search = %+v, %v", values, err)
	}
	mock.ExpectExec("UPDATE public\\.runtime_pgvector_documents SET index_status='deleted'").WithArgs("tenant-a", "knowledge", "doc").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.DeleteVector(context.Background(), "tenant-a", "doc"); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
