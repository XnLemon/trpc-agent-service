package postgres_test

import (
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
)

func TestCapabilityMethodsRejectNilStore(t *testing.T) {
	var store *runtimepostgres.Store
	_, err := store.PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content"})
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("PutMemory on nil store = %v", err)
	}
}

func TestSearchVectorsComputesCosineAndScopesTenant(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT tenant_id,document_id,content,metadata,embedding,version,updated_at FROM public.runtime_vector_index WHERE tenant_id=$1")).
		WithArgs("tenant-a").
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "document_id", "content", "metadata", "embedding", "version", "updated_at"}).
			AddRow("tenant-a", "doc-1", "coffee", []byte("{\"kind\":\"fact\"}"), []byte("[1,0]"), int64(3), when).
			AddRow("tenant-a", "doc-2", "tea", []byte("{}"), []byte("[0,1]"), int64(1), when))
	results, err := runtimepostgres.New(db).SearchVectors(context.Background(), "tenant-a", []float64{1, 0}, 10)
	if err != nil || len(results) != 2 {
		t.Fatalf("SearchVectors = %+v, %v", results, err)
	}
	if results[0].Record.DocumentID != "doc-1" || results[0].Score != 1 {
		t.Fatalf("top vector = %+v", results[0])
	}
	if results[1].Score != 0 {
		t.Fatalf("orthogonal score = %v", results[1].Score)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPutMemoryNormalizesNilJSONValues(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("INSERT INTO public.runtime_memory")).
		WithArgs("tenant-a", sqlmock.AnyArg(), "user", "", "content", []byte("[]"), []byte("{}"), []byte("[]")).
		WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "memory_id", "user_id", "session_id", "content", "topics", "metadata", "embedding", "version", "deleted_at", "created_at", "updated_at"}).
			AddRow("tenant-a", "mem-generated", "user", "", "content", []byte("[]"), []byte("{}"), []byte("[]"), int64(1), nil, when, when))
	value, err := runtimepostgres.New(db).PutMemory(context.Background(), runtimestorage.MemoryInput{TenantID: "tenant-a", UserID: "user", Content: "content"})
	if err != nil || value.MemoryID != "mem-generated" {
		t.Fatalf("PutMemory = %+v, %v", value, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
