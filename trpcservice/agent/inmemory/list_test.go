package inmemory

import (
	"context"
	"testing"
)

func TestListScopesFiltersAndPaginatesAppsAndRevisions(t *testing.T) {
	repository := NewRepository()
	first := createApp(t, repository, tenantOne, "first")
	second := createApp(t, repository, tenantOne, "second")
	_ = createApp(t, repository, tenantTwo, "second")

	page, next, err := repository.List(context.Background(), tenantOne, "", "draft", "", 1)
	if err != nil || len(page) != 1 || next == "" {
		t.Fatalf("first app page = items=%+v next=%q err=%v", page, next, err)
	}
	page, next, err = repository.List(context.Background(), tenantOne, "", "draft", next, 1)
	if err != nil || len(page) != 1 || next != "" {
		t.Fatalf("second app page = items=%+v next=%q err=%v", page, next, err)
	}
	filtered, _, err := repository.List(context.Background(), tenantOne, "second", "", "", 50)
	if err != nil || len(filtered) != 1 || filtered[0].AppID != second.AppID || filtered[0].AppID == first.AppID {
		t.Fatalf("filtered app list = items=%+v err=%v", filtered, err)
	}

	firstDraft := createDraft(t, repository, first, draftConfiguration("first"))
	secondDraft := createDraft(t, repository, first, draftConfiguration("second"))
	revisions, next, err := repository.ListRevisions(context.Background(), tenantOne, first.AppID, "", "draft", "", 1)
	if err != nil || len(revisions) != 1 || next == "" || revisions[0].Revision != firstDraft.Revision {
		t.Fatalf("first revision page = items=%+v next=%q err=%v", revisions, next, err)
	}
	revisions, next, err = repository.ListRevisions(context.Background(), tenantOne, first.AppID, "", "draft", next, 1)
	if err != nil || len(revisions) != 1 || next != "" || revisions[0].Revision != secondDraft.Revision {
		t.Fatalf("second revision page = items=%+v next=%q err=%v", revisions, next, err)
	}
}
