package inmemory_test

import (
	"context"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend/inmemory"
)

func TestListScopesFiltersAndPaginatesProfiles(t *testing.T) {
	repository := inmemory.NewRepository(testCatalog(t))
	first, _, err := repository.Create(context.Background(), createInput(tenantOne, "primary"))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := repository.Create(context.Background(), createInput(tenantOne, "secondary"))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.Create(context.Background(), createInput(tenantTwo, "primary")); err != nil {
		t.Fatal(err)
	}

	page, next, err := repository.List(context.Background(), tenantOne, "", "", "", 1)
	if err != nil || len(page) != 1 || next == "" {
		t.Fatalf("first backend page = items=%+v next=%q err=%v", page, next, err)
	}
	page, next, err = repository.List(context.Background(), tenantOne, "", "", next, 1)
	if err != nil || len(page) != 1 || next != "" {
		t.Fatalf("second backend page = items=%+v next=%q err=%v", page, next, err)
	}
	filtered, _, err := repository.List(context.Background(), tenantOne, "secondary", "", "", 50)
	if err != nil || len(filtered) != 1 || filtered[0].ProfileID != second.ProfileID || filtered[0].ProfileID == first.ProfileID {
		t.Fatalf("filtered backend list = items=%+v err=%v", filtered, err)
	}
}
