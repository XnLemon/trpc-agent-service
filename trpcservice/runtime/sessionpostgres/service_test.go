package sessionpostgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/sessionpostgres"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimestorageinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
	trpcevent "trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

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
