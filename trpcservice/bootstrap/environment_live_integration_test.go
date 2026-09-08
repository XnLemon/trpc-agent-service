package bootstrap

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memorychromadb "trpc.group/trpc-go/trpc-agent-go/memory/chromadb"
)

// These tests are protected live integration checks rather than deterministic
// CI. They require an operator-owned ChromaDB/COS account and deliberately use
// unique namespaces so that two independently materialized workers can write
// concurrently before both are recreated and asked to read committed state.
func requireLiveIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("TRPC_LIVE_INTEGRATION") != "1" {
		t.Skip("TRPC_LIVE_INTEGRATION=1 is required")
	}
}

func TestChromaMemoryDualWorkerRestartLive(t *testing.T) {
	requireLiveIntegration(t)
	endpoint := strings.TrimSpace(os.Getenv("TRPC_CHROMA_LIVE_ENDPOINT"))
	if endpoint == "" {
		t.Skip("TRPC_CHROMA_LIVE_ENDPOINT is not configured")
	}

	nonce := strconv.FormatInt(time.Now().UnixNano(), 10)
	collection := strings.TrimSpace(os.Getenv("TRPC_CHROMA_LIVE_COLLECTION"))
	if collection == "" {
		collection = "trpc_live_" + nonce
	}
	chromaTenant := liveOptionOrDefault(os.Getenv("TRPC_CHROMA_LIVE_TENANT"), "default_tenant")
	database := liveOptionOrDefault(os.Getenv("TRPC_CHROMA_LIVE_DATABASE"), "default_database")
	apiKey := strings.TrimSpace(os.Getenv("TRPC_CHROMA_LIVE_API_KEY"))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	newWorker := func() *memorychromadb.Service {
		opts := []memorychromadb.ServiceOpt{
			memorychromadb.WithBaseURL(endpoint),
			memorychromadb.WithTenant(chromaTenant),
			memorychromadb.WithDatabase(database),
			memorychromadb.WithCollectionName(collection),
			memorychromadb.WithEmbedder(environmentHashEmbedder{}),
			memorychromadb.WithIndexDimension(32),
			memorychromadb.WithTimeout(10 * time.Second),
		}
		if apiKey != "" {
			opts = append(opts, memorychromadb.WithAPIKey(apiKey))
		}
		worker, err := memorychromadb.NewService(opts...)
		if err != nil {
			t.Fatalf("create Chroma worker: %v", err)
		}
		return worker
	}

	workerA := newWorker()
	workerB := newWorker()
	appA := "live-app-a-" + nonce
	appB := "live-app-b-" + nonce
	keyA := memory.UserKey{AppName: appA, UserID: "live-user-a"}
	keyB := memory.UserKey{AppName: appB, UserID: "live-user-b"}
	write := func(worker *memorychromadb.Service, key memory.UserKey, label string) error {
		for index := 0; index < 3; index++ {
			if err := worker.AddMemory(ctx, key, fmt.Sprintf("%s committed memory %d", label, index), []string{"live"}); err != nil {
				return err
			}
		}
		return nil
	}
	var group sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for _, operation := range []func() error{
		func() error { return write(workerA, keyA, "worker-a") },
		func() error { return write(workerB, keyB, "worker-b") },
	} {
		operation := operation
		group.Add(1)
		go func() {
			defer group.Done()
			if err := operation(); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
			}
		}()
	}
	group.Wait()
	if firstErr != nil {
		_ = workerA.Close()
		_ = workerB.Close()
		t.Fatalf("concurrent Chroma writes: %v", firstErr)
	}
	if err := workerA.Close(); err != nil {
		t.Fatalf("close Chroma worker A: %v", err)
	}
	if err := workerB.Close(); err != nil {
		t.Fatalf("close Chroma worker B: %v", err)
	}

	// Recreating both clients is the worker restart boundary. Reads below must
	// use the same immutable collection and return only each worker's namespace.
	restartedA := newWorker()
	restartedB := newWorker()
	defer func() {
		_ = restartedA.Close()
		_ = restartedB.Close()
	}()
	assertMemoryCount(t, restartedA, keyA, "worker-a", 3)
	assertMemoryCount(t, restartedB, keyB, "worker-b", 3)
	assertMemoryCount(t, restartedA, keyA, "worker-b", 0)
	assertMemoryCount(t, restartedB, keyB, "worker-a", 0)
}

func assertMemoryCount(t *testing.T, service *memorychromadb.Service, key memory.UserKey, contains string, want int) {
	t.Helper()
	entries, err := service.ReadMemories(context.Background(), key, 20)
	if err != nil {
		t.Fatalf("read Chroma memories for %s/%s: %v", key.AppName, key.UserID, err)
	}
	count := 0
	for _, entry := range entries {
		if entry != nil && entry.Memory != nil && strings.Contains(entry.Memory.Memory, contains) {
			count++
		}
	}
	if count != want {
		t.Fatalf("Chroma memories for %s/%s containing %q = %d, want %d", key.AppName, key.UserID, contains, count, want)
	}
}

func TestCOSArtifactDualWorkerRestartLive(t *testing.T) {
	requireLiveIntegration(t)
	endpoint := strings.TrimSpace(os.Getenv("TRPC_COS_LIVE_ENDPOINT"))
	secretID := strings.TrimSpace(os.Getenv("TRPC_COS_LIVE_SECRET_ID"))
	secretKey := strings.TrimSpace(os.Getenv("TRPC_COS_LIVE_SECRET_KEY"))
	if endpoint == "" || secretID == "" || secretKey == "" {
		t.Skip("TRPC_COS_LIVE_ENDPOINT, TRPC_COS_LIVE_SECRET_ID and TRPC_COS_LIVE_SECRET_KEY are required")
	}

	nonce := strconv.FormatInt(time.Now().UnixNano(), 10)
	namespace := "t_cos_live_" + nonce
	secret, err := modelprofile.NewSecretValue(secretID + ":" + secretKey)
	if err != nil {
		t.Fatal(err)
	}
	binding := backendBindingForLiveCOS(endpoint)
	newWorker := func() artifact.Service {
		value, providerErr := (environmentCOSCapabilityProvider{}).New(context.Background(), backendStorageInput(namespace), binding, secret)
		if providerErr != nil {
			t.Fatalf("create COS worker: %v", providerErr)
		}
		service, ok := value.(artifact.Service)
		if !ok || service == nil {
			t.Fatalf("COS worker type = %T", value)
		}
		return service
	}

	workerA := newWorker()
	workerB := newWorker()
	base := artifact.SessionInfo{AppName: "live-app", UserID: "live-user", SessionID: namespace}
	files := []struct {
		worker artifact.Service
		name   string
		data   string
	}{
		{worker: workerA, name: "worker-a.txt", data: "worker-a committed artifact"},
		{worker: workerB, name: "worker-b.txt", data: "worker-b committed artifact"},
	}
	var group sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex
	for _, file := range files {
		file := file
		group.Add(1)
		go func() {
			defer group.Done()
			if _, saveErr := file.worker.SaveArtifact(context.Background(), base, file.name, &artifact.Artifact{Data: []byte(file.data), MimeType: "text/plain"}); saveErr != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = saveErr
				}
				errMu.Unlock()
			}
		}()
	}
	group.Wait()
	if firstErr != nil {
		t.Fatalf("concurrent COS writes: %v", firstErr)
	}

	// COS services are stateless adapters over the bucket; constructing fresh
	// instances models two workers after a process restart.
	restartedA := newWorker()
	restartedB := newWorker()
	for _, file := range files {
		service := restartedA
		if file.name == "worker-b.txt" {
			service = restartedB
		}
		loaded, loadErr := service.LoadArtifact(context.Background(), base, file.name, nil)
		if loadErr != nil || loaded == nil || string(loaded.Data) != file.data {
			t.Fatalf("restarted COS artifact %s = %#v, %v", file.name, loaded, loadErr)
		}
		_ = service.DeleteArtifact(context.Background(), base, file.name)
	}
}

func liveOptionOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func backendBindingForLiveCOS(endpoint string) backend.CapabilityBinding {
	return backend.CapabilityBinding{Capability: backend.CapabilityArtifact, Provider: "cos", Endpoint: endpoint}
}

func backendStorageInput(_ string) backend.StorageFactoryInput {
	return backend.StorageFactoryInput{TenantID: "t_00000000000000000000000000"}
}
