package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	knowledgepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge/postgres"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
)

func TestStoreUsesTenantPredicateAndRoundTripsDocument(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, err := knowledgepostgres.New(db, "t_00000000000000000000000000", knowledgepostgres.WithDimension(2))
	if err != nil {
		t.Fatal(err)
	}
	created := time.Unix(10, 0).UTC()
	updated := created.Add(time.Minute)
	doc := &document.Document{ID: "doc-a", Name: "Guide", Content: "hello", Metadata: map[string]any{"kind": "guide"}}
	mock.ExpectQuery("INSERT INTO public.runtime_knowledge_document").
		WithArgs("t_00000000000000000000000000", "doc-a", "Guide", "hello", "", sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"created_at", "updated_at"}).AddRow(created, updated))
	if err := store.Add(context.Background(), doc, []float64{1, 0}); err != nil {
		t.Fatalf("Add() error = %v", err)
	}
	mock.ExpectQuery("SELECT document_id, name, content, embedding_text, metadata, embedding, created_at, updated_at").
		WithArgs("t_00000000000000000000000000", "doc-a").
		WillReturnRows(sqlmock.NewRows([]string{"document_id", "name", "content", "embedding_text", "metadata", "embedding", "created_at", "updated_at"}).
			AddRow("doc-a", "Guide", "hello", "", []byte(`{"kind":"guide"}`), []byte(`[1,0]`), created, updated))
	loaded, embedding, err := store.Get(context.Background(), "doc-a")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if loaded.ID != "doc-a" || loaded.Metadata["kind"] != "guide" || len(embedding) != 2 || embedding[0] != 1 || !loaded.CreatedAt.Equal(created) {
		t.Fatalf("round trip document = %#v, embedding = %#v", loaded, embedding)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreSearchFiltersAndScoresPersistedRows(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, err := knowledgepostgres.New(db, "t_00000000000000000000000000", knowledgepostgres.WithDimension(2), knowledgepostgres.WithMaxResults(5))
	if err != nil {
		t.Fatal(err)
	}
	created := time.Unix(10, 0).UTC()
	mock.ExpectQuery("SELECT document_id, name, content, embedding_text, metadata, embedding, created_at, updated_at").
		WithArgs("t_00000000000000000000000000").
		WillReturnRows(sqlmock.NewRows([]string{"document_id", "name", "content", "embedding_text", "metadata", "embedding", "created_at", "updated_at"}).
			AddRow("doc-a", "A", "alpha", "", []byte(`{"kind":"guide"}`), []byte(`[1,0]`), created, created).
			AddRow("doc-b", "B", "beta", "", []byte(`{"kind":"note"}`), []byte(`[0,1]`), created.Add(time.Second), created.Add(time.Second)))
	result, err := store.Search(context.Background(), &vectorstore.SearchQuery{
		Vector: []float64{1, 0}, Limit: 10, MinScore: 0.5,
		Filter: &vectorstore.SearchFilter{Metadata: map[string]any{"kind": "guide"}},
	})
	if err != nil {
		t.Fatalf("Search() error = %v", err)
	}
	if len(result.Results) != 1 || result.Results[0].Document.ID != "doc-a" || result.Results[0].Score < 0.99 {
		t.Fatalf("Search() result = %#v", result.Results)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStoreRedactsDatabaseErrorsAndClosesWithoutClosingPool(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store, err := knowledgepostgres.New(db, "t_00000000000000000000000000")
	if err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT document_id, name, content, embedding_text, metadata, embedding, created_at, updated_at").
		WithArgs("t_00000000000000000000000000", "missing").
		WillReturnError(errors.New("dial tcp password=do-not-leak"))
	_, _, err = store.Get(context.Background(), "missing")
	if !errors.Is(err, knowledgepostgres.ErrStorage) || strings.Contains(err.Error(), "do-not-leak") {
		t.Fatalf("redacted Get() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(store.Delete(context.Background(), "doc-a"), knowledgepostgres.ErrClosed) {
		t.Fatalf("closed store error = %v", store.Delete(context.Background(), "doc-a"))
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("Store.Close closed borrowed db: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
