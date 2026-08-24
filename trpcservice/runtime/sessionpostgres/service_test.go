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
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type failingCreateStore struct {
	runtimestorage.RuntimeStore
}

func (failingCreateStore) CreateSession(context.Context, string, string, map[string]any) (runtimestorage.Session, error) {
	return runtimestorage.Session{}, fmt.Errorf("create unavailable")
}

func (failingCreateStore) DeleteSession(context.Context, string, string) error {
	return fmt.Errorf("delete unavailable")
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

func TestServiceRecoversDurableEventHistoryWithFreshDelegate(t *testing.T) {
	store := runtimestorageinmemory.New()
	first, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "event-restart"}
	created, err := first.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.AppendEvent(context.Background(), created, &trpcevent.Event{ID: "event-restart-1", Response: &trpcmodel.Response{Choices: []trpcmodel.Choice{{Message: trpcmodel.NewUserMessage("durable")}}, Done: true}}); err != nil {
		t.Fatal(err)
	}
	second, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := second.GetSession(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.GetEventCount() != 1 || recovered.GetEvents()[0].ID != "event-restart-1" {
		t.Fatalf("recovered events = %+v", recovered.GetEvents())
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

func TestServiceDeleteSessionRemovesDurableStateBeforeRecreate(t *testing.T) {
	store := runtimestorageinmemory.New()
	service, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "recreate"}
	if _, err := service.CreateSession(context.Background(), key, session.StateMap{"value": []byte("old")}); err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteSession(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	created, err := service.CreateSession(context.Background(), key, session.StateMap{"value": []byte("new")})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(created.State["value"]); got != "new" {
		t.Fatalf("recreated state = %q, want new", got)
	}
	restarted, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := restarted.GetSession(context.Background(), key)
	if err != nil || string(recovered.State["value"]) != "new" {
		t.Fatalf("recovered recreated session = %+v err=%v", recovered, err)
	}
}

func TestServiceDeleteSessionValidationAndDurableError(t *testing.T) {
	service, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), runtimestorageinmemory.New())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.DeleteSession(context.Background(), session.Key{AppName: "app", UserID: "user"}); !errors.Is(err, session.ErrSessionIDRequired) {
		t.Fatalf("invalid delete = %v", err)
	}
	service, err = sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), failingCreateStore{})
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "delete-error"}
	if err := service.DeleteSession(context.Background(), key); err == nil || err.Error() != "delete unavailable" {
		t.Fatalf("durable delete error = %v", err)
	}
}

func TestServiceForwardsUpstreamCapabilities(t *testing.T) {
	delegate := sessioninmemory.NewSessionService()
	service, err := sessionpostgres.New("tenant-a", delegate, runtimestorageinmemory.New())
	if err != nil {
		t.Fatal(err)
	}
	key := session.Key{AppName: "app", UserID: "user", SessionID: "forward"}
	value, err := service.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = service.ListSessions(context.Background(), session.UserKey{AppName: "app", UserID: "user"})
	_ = service.UpdateAppState(context.Background(), "app", session.StateMap{"key": []byte("value")})
	_ = service.DeleteAppState(context.Background(), "app", "key")
	_, _ = service.ListAppStates(context.Background(), "app")
	_ = service.UpdateUserState(context.Background(), session.UserKey{AppName: "app", UserID: "user"}, session.StateMap{"key": []byte("value")})
	_, _ = service.ListUserStates(context.Background(), session.UserKey{AppName: "app", UserID: "user"})
	_ = service.DeleteUserState(context.Background(), session.UserKey{AppName: "app", UserID: "user"}, "key")
	_ = service.CreateSessionSummary(context.Background(), value, "", false)
	_ = service.EnqueueSummaryJob(context.Background(), value, "", false)
	_, _ = service.GetSessionSummaryText(context.Background(), value)
	_ = service.AppendEvent(context.Background(), value, &trpcevent.Event{ID: "forward-event"})
	if err := service.DeleteSession(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceUpdateAndAppendErrorEdges(t *testing.T) {
	store := runtimestorageinmemory.New()
	service, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.UpdateSessionState(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "missing"}, nil); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing update = %v", err)
	}
	if err := service.AppendEvent(context.Background(), nil, nil); !errors.Is(err, session.ErrNilSession) {
		t.Fatalf("nil append = %v", err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{UserID: "user", SessionID: "id"}); !errors.Is(err, session.ErrAppNameRequired) {
		t.Fatalf("invalid app = %v", err)
	}
	if _, err := service.GetSession(context.Background(), session.Key{AppName: "app", SessionID: "id"}); !errors.Is(err, session.ErrUserIDRequired) {
		t.Fatalf("invalid user = %v", err)
	}
	if _, err := store.CreateSession(context.Background(), "tenant-a", "delegate-missing", map[string]any{"number": 1}); err != nil {
		t.Fatal(err)
	}
	if value, err := service.GetSession(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "delegate-missing"}); err != nil || string(value.State["number"]) != "1" {
		t.Fatalf("numeric durable state = %+v, err=%v", value, err)
	}
	service2, err := sessionpostgres.New("tenant-a", sessioninmemory.NewSessionService(), store)
	if err != nil {
		t.Fatal(err)
	}
	if err := service2.UpdateSessionState(context.Background(), session.Key{AppName: "app", UserID: "user", SessionID: "delegate-missing"}, session.StateMap{"value": []byte("new")}); err == nil {
		t.Fatal("delegate update unexpectedly succeeded")
	}
}
