package agent

import (
	"context"
	"errors"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	storagefactory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
)

func TestAuthorizedMemoryServiceFiltersToolsAndWrites(t *testing.T) {
	base := memoryinmemory.NewMemoryService()
	service := newAuthorizedMemoryService(base, []appmodel.ToolAuthorization{{ToolID: memory.SearchToolName}})
	defer service.Close()

	for _, candidate := range service.Tools() {
		if candidate.Declaration().Name != memory.SearchToolName {
			t.Fatalf("unauthorized memory tool exposed: %s", candidate.Declaration().Name)
		}
	}
	if err := service.AddMemory(context.Background(), memory.UserKey{AppName: "app", UserID: "user"}, "secret", nil); !errors.Is(err, storagefactory.ErrCapabilityUnavailable) {
		t.Fatalf("unauthorized memory write error = %v", err)
	}
	if _, err := service.SearchMemories(context.Background(), memory.UserKey{AppName: "app", UserID: "user"}, "secret"); err != nil {
		t.Fatalf("authorized memory search error = %v", err)
	}
}

func TestAuthorizedMemoryServiceControlsAutomaticExtraction(t *testing.T) {
	base := memoryinmemory.NewMemoryService()
	withoutPermission := newAuthorizedMemoryService(base, nil)
	if err := withoutPermission.EnqueueAutoMemoryJob(context.Background(), nil); err != nil {
		t.Fatalf("disabled auto extraction error = %v", err)
	}
	withPermission := newAuthorizedMemoryService(base, []appmodel.ToolAuthorization{{ToolID: "memory_auto_extract"}})
	if err := withPermission.EnqueueAutoMemoryJob(context.Background(), nil); err != nil {
		t.Fatalf("enabled auto extraction error = %v", err)
	}
}
