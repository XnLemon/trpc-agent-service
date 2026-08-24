package sessionpostgres_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/sessionpostgres"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type failingCreateStore struct {
	runtimestorage.RuntimeStore
}

func (failingCreateStore) CreateSession(context.Context, string, string, map[string]any) (runtimestorage.Session, error) {
	return runtimestorage.Session{}, fmt.Errorf("create unavailable")
}

func TestServicePersistsSessionStateAndEventsForFixedTenant(t *testing.T) {
	store := runtimestorageinmemory.New()
	delegate := sessioninmemory.NewSessionService()
	service, err := sessionpostgres.New("tenant-a", delegate, store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	created, err := service.CreateSession(context.Background(), key, session.StateMap{"count": []byte("1")})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateSessionState(context.Background(), key, session.StateMap{"count": []byte("2")}); err != nil {
		t.Fatal(err)
	}
	if err := service.AppendEvent(context.Background(), created, &trpcevent.Event{ID: "event-1"}); err != nil {
		t.Fatal(err)
	}
	persisted, err := store.GetSession(context.Background(), "tenant-a", "session")
	if err != nil || persisted.Version != 2 {
		t.Fatalf("persisted session = %+v, err=%v", persisted, err)
	}
}

func TestServiceRejectsInvalidConstructionAndKeys(t *testing.T) {
	store := runtimestorageinmemory.New()
	delegate := sessioninmemory.NewSessionService()
	if _, err := sessionpostgres.New("", delegate, store); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("empty tenant error = %v", err)
	}
	service, err := sessionpostgres.New("tenant-a", delegate, store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{AppName: "app", UserID: "user"}); !errors.Is(err, session.ErrSessionIDRequired) {
		t.Fatalf("invalid key error = %v", err)
	}
}

func TestServiceTreatsMissingDurableSessionAsUpstreamMiss(t *testing.T) {
	service, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), runtimestorageinmemory.New())
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "cold-start"}
	value, err := service.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if value != nil {
		t.Fatalf("missing durable session = %+v, want upstream miss", value)
	}
}

func TestServiceRecoversStateWithFreshDelegate(t *testing.T) {
	store := runtimestorageinmemory.New()
	first, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "restart"}
	if _, err := first.CreateSession(context.Background(), key, session.StateMap{"answer": []byte("42")}); err != nil {
		t.Fatal(err)
	}
	second, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := second.GetSession(context.Background(), key)
	if err != nil || string(recovered.State["answer"]) != "42" {
		t.Fatalf("recovered = %+v, err=%v", recovered, err)
	}
}

func TestServiceRefreshesWarmDelegateFromDurableState(t *testing.T) {
	store := runtimestorageinmemory.New()
	first, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	second, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "warm"}
	if _, err := first.CreateSession(context.Background(), key, session.StateMap{"value": []byte("old"), "removed": []byte("stale")}); err != nil {
		t.Fatal(err)
	}
	if _, err := second.GetSession(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := second.UpdateSessionState(context.Background(), key, session.StateMap{"value": []byte("new")}); err != nil {
		t.Fatal(err)
	}
	refreshed, err := first.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if string(refreshed.State["value"]) != "new" {
		t.Fatalf("warm delegate state = %q, want new", refreshed.State["value"])
	}
	if _, ok := refreshed.State["removed"]; ok {
		t.Fatalf("warm delegate retained removed durable state: %+v", refreshed.State)
	}
}

func TestServiceCompensatesDelegateWhenDurableCreateFails(t *testing.T) {
	delegate := sessioninmemory.NewSessionService()
	service, err := sessionpostgres.New("tenant-a", delegate, failingCreateStore{})
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "rollback"}
	if _, err := service.CreateSession(context.Background(), key, nil); err == nil {
		t.Fatal("CreateSession succeeded despite durable failure")
	}
	if existing, err := delegate.GetSession(context.Background(), key); err != nil || existing != nil {
		t.Fatal("delegate session remained after durable failure")
	}
}
