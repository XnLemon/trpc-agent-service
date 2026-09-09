package knowledge

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore"
	vectorinmemory "trpc.group/trpc-go/trpc-agent-go/knowledge/vectorstore/inmemory"
)

type managementEmbedder struct{ calls int }

func (embedder *managementEmbedder) GetEmbedding(ctx context.Context, text string) ([]float64, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	embedder.calls++
	if text == "replacement" {
		return []float64{0, 1}, nil
	}
	return []float64{1, 0}, nil
}
func (embedder *managementEmbedder) GetEmbeddingWithUsage(ctx context.Context, text string) ([]float64, map[string]any, error) {
	value, err := embedder.GetEmbedding(ctx, text)
	return value, nil, err
}
func (embedder *managementEmbedder) GetDimensions() int { return 2 }

type managementBackend struct {
	store    vectorstore.VectorStore
	embedder *managementEmbedder
}

func (backend *managementBackend) Open(context.Context, Scope) (Backend, error) {
	return Backend{Store: backend.store, Embedder: backend.embedder}, nil
}

func TestManagerUsesUpdateForReplacementAndAddForImportUpsert(t *testing.T) {
	backend := &managementBackend{store: vectorinmemory.New(), embedder: &managementEmbedder{}}
	manager, err := NewManager(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "tenant-a", AppID: "app-a"}
	created, err := manager.CreateDocument(context.Background(), scope, DocumentInput{ID: "doc-a", Name: "Guide", Content: "original"})
	if err != nil {
		t.Fatalf("CreateDocument() error = %v", err)
	}
	if created.Metadata[AppMetadataKey] != scope.AppID || created.Metadata[PublishedMetadataKey] != false {
		t.Fatalf("created metadata = %#v", created.Metadata)
	}
	updated, err := manager.UpdateDocument(context.Background(), scope, "doc-a", UpdateInput{Content: stringPointer("replacement")})
	if err != nil {
		t.Fatalf("UpdateDocument() error = %v", err)
	}
	if updated.Content != "replacement" || backend.embedder.calls != 2 {
		t.Fatalf("updated document = %#v, embed calls = %d", updated, backend.embedder.calls)
	}
	imported, err := manager.ImportDocuments(context.Background(), scope, ImportInput{Documents: []DocumentInput{{ID: "doc-a", Content: "imported"}}})
	if err != nil {
		t.Fatalf("ImportDocuments() error = %v", err)
	}
	if imported.Count != 1 || imported.Items[0].Content != "imported" {
		t.Fatalf("import result = %#v", imported)
	}
	if got, _, err := backend.store.Get(context.Background(), "doc-a"); err != nil || got.Content != "imported" {
		t.Fatalf("stored import = %#v, error = %v", got, err)
	}
}

func TestManagerRejectsCrossAppOverwriteAndPublishesIdempotently(t *testing.T) {
	backend := &managementBackend{store: vectorinmemory.New(), embedder: &managementEmbedder{}}
	manager, err := NewManager(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	firstScope := Scope{TenantID: "tenant-a", AppID: "app-a"}
	secondScope := Scope{TenantID: "tenant-a", AppID: "app-b"}
	if _, err := manager.CreateDocument(ctx, firstScope, DocumentInput{ID: "shared", Content: "private"}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateDocument(ctx, secondScope, DocumentInput{ID: "shared", Content: "overwrite"}); !errors.Is(err, ErrScope) {
		t.Fatalf("cross-app create error = %v", err)
	}
	if _, err := manager.ImportDocuments(ctx, secondScope, ImportInput{Documents: []DocumentInput{{ID: "shared", Content: "overwrite"}}}); !errors.Is(err, ErrScope) {
		t.Fatalf("cross-app import error = %v", err)
	}
	version, err := manager.Publish(ctx, firstScope, "admin")
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	versionAgain, err := manager.Publish(ctx, firstScope, "another-admin")
	if err != nil {
		t.Fatalf("idempotent Publish() error = %v", err)
	}
	if version.Version != 1 || versionAgain.Version != version.Version || versionAgain.ContentDigest != version.ContentDigest {
		t.Fatalf("published versions = %#v and %#v", version, versionAgain)
	}
	versions, err := manager.ListVersions(ctx, firstScope)
	if err != nil || len(versions) != 1 || versions[0].Version != 1 {
		t.Fatalf("versions = %#v, error = %v", versions, err)
	}
}

func TestManagerDoesNotReturnVectorsAndValidatesScope(t *testing.T) {
	backend := &managementBackend{store: vectorinmemory.New(), embedder: &managementEmbedder{}}
	manager, err := NewManager(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.GetDocument(context.Background(), Scope{TenantID: "tenant-a", AppID: "app-a"}, "bad\nid"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid document error = %v", err)
	}
	if _, err := manager.CreateDocument(context.Background(), Scope{TenantID: "", AppID: "app-a"}, DocumentInput{Content: "x"}); !errors.Is(err, ErrScope) {
		t.Fatalf("invalid scope error = %v", err)
	}
	if _, err := manager.CreateDocument(context.Background(), Scope{TenantID: "tenant-a", AppID: "app-a"}, DocumentInput{Content: "x", Metadata: map[string]any{AppMetadataKey: "other"}}); !errors.Is(err, ErrScope) {
		t.Fatalf("reserved app metadata error = %v", err)
	}
}

func TestManagerCRUDListRebuildAndVersionScope(t *testing.T) {
	backend := &managementBackend{store: vectorinmemory.New(), embedder: &managementEmbedder{}}
	manager, err := NewManager(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	scope := Scope{TenantID: "tenant-a", AppID: "app-a"}
	foreign := Scope{TenantID: "tenant-a", AppID: "app-b"}
	first, err := manager.CreateDocument(ctx, scope, DocumentInput{ID: "doc-a", Name: "A", Content: "alpha", Metadata: map[string]any{"kind": "guide"}})
	if err != nil {
		t.Fatal(err)
	}
	if first.TenantID != scope.TenantID || first.AppID != scope.AppID || first.Metadata[AppMetadataKey] != scope.AppID {
		t.Fatalf("created scope = %#v", first)
	}
	if _, err := manager.CreateDocument(ctx, scope, DocumentInput{ID: "doc-b", Content: "beta"}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CreateDocument(ctx, foreign, DocumentInput{ID: "doc-c", Content: "foreign"}); err != nil {
		t.Fatal(err)
	}

	page, err := manager.ListDocuments(ctx, scope, "", 1)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != "doc-a" || page.NextCursor != "1" {
		t.Fatalf("first page = %#v, error = %v", page, err)
	}
	page, err = manager.ListDocuments(ctx, scope, page.NextCursor, 10)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != "doc-b" || page.NextCursor != "" {
		t.Fatalf("second page = %#v, error = %v", page, err)
	}
	if _, err := manager.ListDocuments(ctx, scope, "01", 10); !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-canonical cursor error = %v", err)
	}

	got, err := manager.GetDocument(ctx, scope, "doc-a")
	if err != nil || got.Content != "alpha" {
		t.Fatalf("get = %#v, error = %v", got, err)
	}
	newName := "Renamed"
	updated, err := manager.UpdateDocument(ctx, scope, "doc-a", UpdateInput{Name: &newName, Metadata: map[string]any{"kind": "note"}})
	if err != nil || updated.Name != newName || updated.Metadata["kind"] != "note" || updated.Metadata[AppMetadataKey] != scope.AppID || updated.Metadata[PublishedMetadataKey] != false {
		t.Fatalf("patch = %#v, error = %v", updated, err)
	}
	if _, err := manager.UpdateDocument(ctx, foreign, "doc-a", UpdateInput{Content: stringPointer("cross-app")}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign patch error = %v", err)
	}

	rebuilt, err := manager.Rebuild(ctx, scope)
	if err != nil || rebuilt.Count != 2 {
		t.Fatalf("rebuild = %#v, error = %v", rebuilt, err)
	}
	if backend.embedder.calls < 4 {
		t.Fatalf("rebuild did not re-embed all app documents, calls = %d", backend.embedder.calls)
	}
	if err := manager.DeleteDocument(ctx, scope, "doc-b"); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.GetDocument(ctx, scope, "doc-b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted document error = %v", err)
	}
	if err := manager.DeleteDocument(ctx, foreign, "doc-a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("foreign delete error = %v", err)
	}

	version, err := manager.Publish(ctx, scope, "admin")
	if err != nil || version.Version != 1 || version.DocumentCount != 1 {
		t.Fatalf("publish = %#v, error = %v", version, err)
	}
	versions, err := manager.ListVersions(ctx, scope)
	if err != nil || len(versions) != 1 || versions[0].AppID != scope.AppID {
		t.Fatalf("versions = %#v, error = %v", versions, err)
	}
	foreignVersions, err := manager.ListVersions(ctx, foreign)
	if err != nil || len(foreignVersions) != 0 {
		t.Fatalf("foreign versions = %#v, error = %v", foreignVersions, err)
	}
}

func TestManagerRejectsUnnormalizedScopeAndAllOrNothingValidation(t *testing.T) {
	backend := &managementBackend{store: vectorinmemory.New(), embedder: &managementEmbedder{}}
	manager, err := NewManager(backend, nil)
	if err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "tenant-a", AppID: "app-a"}
	if _, err := manager.ListDocuments(context.Background(), Scope{TenantID: " tenant-a", AppID: "app-a"}, "", 10); !errors.Is(err, ErrScope) {
		t.Fatalf("space-padded scope error = %v", err)
	}
	_, err = manager.ImportDocuments(context.Background(), scope, ImportInput{Documents: []DocumentInput{
		{ID: "valid", Content: "written only after validation"},
		{ID: "bad\nidentifier", Content: "must fail"},
	}})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid later import error = %v", err)
	}
	if _, err := manager.GetDocument(context.Background(), scope, "valid"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("partial import document = %v", err)
	}
	if _, err := manager.ImportDocuments(context.Background(), scope, ImportInput{Documents: []DocumentInput{{ID: "same", Content: "one"}, {ID: "same", Content: "two"}}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate import error = %v", err)
	}
}

func stringPointer(value string) *string { return &value }
