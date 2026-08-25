package postgres_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/postgres"
	"github.com/jackc/pgx/v5/pgconn"
)

var eventColumns = []string{"tenant_id", "event_id", "session_id", "binding_id", "external_message_id", "idempotency_key", "event_seq", "status", "fencing_token", "lease_owner", "lease_expires_at", "reply_id", "segment_count", "created_at", "updated_at"}
var replyColumns = []string{"tenant_id", "reply_id", "event_id", "segment_index", "segment_count", "payload", "status", "attempts", "fencing_token", "lease_owner", "lease_expires_at", "provider_message_id", "last_error_class", "created_at", "updated_at"}
var historyColumns = []string{"tenant_id", "session_id", "event_id", "payload", "history_seq", "created_at"}

func eventRow(when time.Time) *sqlmock.Rows {
	return sqlmock.NewRows(eventColumns).AddRow("tenant-a", "event-1", "session-1", "binding-1", "external-1", "idem-1", int64(2), "received", int64(0), "", nil, "", 1, when, when)
}

func replyRow(when time.Time) *sqlmock.Rows {
	return sqlmock.NewRows(replyColumns).AddRow("tenant-a", "reply-1", "event-1", 0, 1, "payload", "pending", 0, int64(0), "", nil, "", "", when, when)
}

func TestGetSessionUsesExplicitTenantPredicateAndDefensiveState(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery("SELECT tenant_id, session_id, status, version, state").WithArgs("tenant-a", "session-1").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "session-1", "active", 1, []byte("{\"key\":\"value\"}"), when, when))
	value, err := runtimepostgres.New(db).GetSession(context.Background(), "tenant-a", "session-1")
	if err != nil {
		t.Fatal(err)
	}
	value.State["key"] = "changed"
	if value.State["key"] != "changed" {
		t.Fatal("state mutation was not applied to returned copy")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateSessionMapsDuplicateWithoutDriverDetails(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	mock.ExpectQuery("INSERT INTO public.runtime_session").WithArgs("tenant-a", "session-1", driver.Value([]byte("{}"))).WillReturnError(errors.New("duplicate key value contains secret connection detail"))
	_, err = runtimepostgres.New(db).CreateSession(context.Background(), "tenant-a", "session-1", nil)
	if !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestMethodsRespectCanceledContextBeforeDatabaseCall(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtimepostgres.New(db).GetSession(ctx, "tenant-a", "session-1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreMethodsRespectCanceledContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.CreateSession(ctx, "tenant-a", "session", nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("CreateSession = %v", err)
	}
	if _, err := store.UpdateSessionState(ctx, "tenant-a", "session", 1, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateSessionState = %v", err)
	}
	if err := store.DeleteSession(ctx, "tenant-a", "session"); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeleteSession = %v", err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", SessionID: "session", BindingID: "binding", ExternalMessageID: "external", EventID: "event"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecordMessage = %v", err)
	}
	if _, err := store.GetMessage(ctx, "tenant-a", "event"); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetMessage = %v", err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentCount: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("EnqueueReply = %v", err)
	}
	if _, err := store.GetReply(ctx, "tenant-a", "reply", 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetReply = %v", err)
	}
	if _, err := store.ClaimReply(ctx, "tenant-a", "reply", 0, "worker", time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("ClaimReply = %v", err)
	}
	if _, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", Owner: "worker", From: runtimestorage.ReplyPending, To: runtimestorage.ReplySending}); !errors.Is(err, context.Canceled) {
		t.Fatalf("TransitionReply = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreCoversMessageAndReplyLifecycle(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	mock.ExpectQuery("SELECT tenant_id, session_id, status, version, state").WithArgs("tenant-a", "session-1").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "session-1", "active", 1, []byte("{\"x\":\"y\"}"), when, when))
	if _, err := store.GetSession(context.Background(), "tenant-a", "session-1"); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("INSERT INTO public.runtime_session").WithArgs("tenant-a", "session-2", driver.Value([]byte("{\"x\":\"y\"}"))).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "session-2", "active", 1, []byte("{\"x\":\"y\"}"), when, when))
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-2", map[string]any{"x": "y"}); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("UPDATE public.runtime_session SET version=version\\+1").WithArgs("tenant-a", "session-2", int64(1), driver.Value([]byte("{\"x\":\"z\"}"))).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "session-2", "active", 2, []byte("{\"x\":\"z\"}"), when, when))
	if _, err := store.UpdateSessionState(context.Background(), "tenant-a", "session-2", 1, map[string]any{"x": "z"}); err != nil {
		t.Fatal(err)
	}
	mock.ExpectExec("DELETE FROM public.runtime_session").WithArgs("tenant-a", "session-2").WillReturnResult(sqlmock.NewResult(0, 1))
	if err := store.DeleteSession(context.Background(), "tenant-a", "session-2"); err != nil {
		t.Fatal(err)
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "binding-1", "external-1").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("UPDATE public.runtime_session SET version=version\\+1").WithArgs("tenant-a", "session-1").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(int64(2)))
	mock.ExpectQuery("INSERT INTO public.message_event").WithArgs("tenant-a", "event-1", "session-1", "binding-1", "external-1", "idem-1", int64(2)).WillReturnRows(eventRow(when))
	mock.ExpectCommit()
	if _, duplicate, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-1", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1", IdempotencyKey: "idem-1"}); err != nil || duplicate {
		t.Fatalf("RecordMessage = duplicate=%v err=%v", duplicate, err)
	}
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "event-1").WillReturnRows(eventRow(when))
	if _, err := store.GetMessage(context.Background(), "tenant-a", "event-1"); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("INSERT INTO public.reply_outbox").WithArgs("tenant-a", "reply-1", "event-1", 0, 1, "payload").WillReturnRows(replyRow(when))
	if _, err := store.EnqueueReply(context.Background(), runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-1", EventID: "event-1", SegmentIndex: 0, SegmentCount: 1, Payload: "payload"}); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-a", "reply-1", 0).WillReturnRows(replyRow(when))
	if _, err := store.GetReply(context.Background(), "tenant-a", "reply-1", 0); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("UPDATE public.reply_outbox SET status='sending'").WithArgs("tenant-a", "reply-1", 0, "worker-a", int64(3)).WillReturnRows(sqlmock.NewRows(replyColumns).AddRow("tenant-a", "reply-1", "event-1", 0, 1, "payload", "sending", 1, int64(1), "worker-a", when.Add(time.Minute), "", "", when, when))
	if _, err := store.ClaimReply(context.Background(), "tenant-a", "reply-1", 0, "worker-a", 3*time.Second); err != nil {
		t.Fatal(err)
	}
	mock.ExpectQuery("UPDATE public.reply_outbox SET status=\\$5").WithArgs("tenant-a", "reply-1", 0, "sending", "sent", "worker-a", int64(0), "provider-1", "", int64(1)).WillReturnRows(sqlmock.NewRows(replyColumns).AddRow("tenant-a", "reply-1", "event-1", 0, 1, "payload", "sent", 2, int64(2), "worker-a", nil, "provider-1", "", when, when))
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply-1", SegmentIndex: 0, From: "sending", To: "sent", Owner: "worker-a", FencingToken: 1, ProviderID: "provider-1"}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreCoversEventHistoryAndMessageLifecycle(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	when := time.Now().UTC()
	payload := []byte("{\"ID\":\"runner-1\"}")
	mock.ExpectQuery("INSERT INTO public.runtime_event_history").WithArgs("tenant-a", "session-1", "runner-1", payload).WillReturnRows(sqlmock.NewRows(historyColumns).AddRow("tenant-a", "session-1", "runner-1", string(payload), int64(1), when))
	value, err := store.AppendEventPayload(context.Background(), runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session-1", EventID: "runner-1", Payload: payload})
	if err != nil || value.HistorySeq != 1 {
		t.Fatalf("append = %+v err=%v", value, err)
	}
	mock.ExpectQuery("SELECT tenant_id,session_id,event_id,payload::text").WithArgs("tenant-a", "session-1").WillReturnRows(sqlmock.NewRows(historyColumns).AddRow("tenant-a", "session-1", "runner-1", string(payload), int64(1), when))
	items, err := store.ListEventPayloads(context.Background(), "tenant-a", "session-1")
	if err != nil || len(items) != 1 {
		t.Fatalf("history = %+v err=%v", items, err)
	}
	mock.ExpectQuery("UPDATE public.message_event SET status=\\$4").WithArgs("tenant-a", "event-1", "received", "running", "worker-a", int64(60), int64(0)).WillReturnRows(sqlmock.NewRows(eventColumns).AddRow("tenant-a", "event-1", "session-1", "binding-1", "external-1", "idem-1", int64(2), "running", int64(1), "worker-a", when.Add(time.Minute), "", 0, when, when))
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event-1", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker-a", LeaseDuration: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionMessagePersistsReplyMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	when := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mock.ExpectQuery("UPDATE public.message_event SET status=\\$4.*reply_id=COALESCE").
		WithArgs("tenant-a", "event-1", runtimestorage.EventRunning, runtimestorage.EventCompleted, "worker-a", int64(0), int64(3), "reply-1", 2).
		WillReturnRows(sqlmock.NewRows(eventColumns).AddRow("tenant-a", "event-1", "session-1", "binding-1", "external-1", "idem-1", int64(2), runtimestorage.EventCompleted, int64(4), "", nil, "reply-1", 2, when, when))
	value, err := runtimepostgres.New(db).TransitionMessage(context.Background(), runtimestorage.MessageTransition{
		TenantID: "tenant-a", EventID: "event-1", From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "worker-a", FencingToken: 3, ReplyID: "reply-1", SegmentCount: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if value.ReplyID != "reply-1" || value.SegmentCount != 2 {
		t.Fatalf("reply metadata = %+v", value)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionMessageWithReplyMapsErrorAndConflict(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(sqlmock.Sqlmock, time.Time)
		want    error
	}{
		{
			name: "storage error",
			prepare: func(mock sqlmock.Sqlmock, _ time.Time) {
				mock.ExpectQuery("UPDATE public.message_event SET status=\\$4.*reply_id=COALESCE").WillReturnError(errors.New("database unavailable"))
			},
			want: runtimestorage.ErrStorage,
		},
		{
			name: "stale fence conflict",
			prepare: func(mock sqlmock.Sqlmock, when time.Time) {
				mock.ExpectQuery("UPDATE public.message_event SET status=\\$4.*reply_id=COALESCE").WillReturnError(sql.ErrNoRows)
				mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "event-1").WillReturnRows(eventRow(when))
			},
			want: runtimestorage.ErrConflict,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			tc.prepare(mock, time.Now().UTC())
			_, err = runtimepostgres.New(db).TransitionMessage(context.Background(), runtimestorage.MessageTransition{
				TenantID: "tenant-a", EventID: "event-1", From: runtimestorage.EventRunning, To: runtimestorage.EventCompleted, Owner: "worker-a", FencingToken: 3, ReplyID: "reply-1", SegmentCount: 2,
			})
			if !errors.Is(err, tc.want) {
				t.Fatalf("TransitionMessage error = %v, want %v", err, tc.want)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRuntimeStoreListReplyCandidates(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	when := time.Now().UTC()
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-a").WillReturnRows(sqlmock.NewRows(replyColumns).AddRow("tenant-a", "reply-1", "event-1", 0, 1, "payload", "pending", 0, int64(0), "", nil, "", "", when, when))
	values, err := store.ListReplyCandidates(context.Background(), "tenant-a")
	if err != nil || len(values) != 1 || values[0].ReplyID != "reply-1" {
		t.Fatalf("reply candidates = %+v err=%v", values, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreDeleteSessionErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	if err := store.DeleteSession(context.Background(), "", "session"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid delete = %v", err)
	}
	mock.ExpectExec("DELETE FROM public.runtime_session").WithArgs("tenant-a", "missing").WillReturnResult(sqlmock.NewResult(0, 0))
	if err := store.DeleteSession(context.Background(), "tenant-a", "missing"); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing delete = %v", err)
	}
	mock.ExpectExec("DELETE FROM public.runtime_session").WithArgs("tenant-a", "error").WillReturnError(errors.New("delete failed"))
	if err := store.DeleteSession(context.Background(), "tenant-a", "error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("delete query error = %v", err)
	}
	mock.ExpectExec("DELETE FROM public.runtime_session").WithArgs("tenant-a", "result-error").WillReturnResult(sqlmock.NewErrorResult(errors.New("rows failed")))
	if err := store.DeleteSession(context.Background(), "tenant-a", "result-error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("delete rows error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreDeleteSessionValidationAndCanceledContext(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	if err := store.DeleteSession(context.Background(), "tenant-a", ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid session delete = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.DeleteSession(canceled, "tenant-a", "session"); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled delete = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreMapsCASAndClaimConflicts(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	when := time.Now().UTC()
	mock.ExpectQuery("UPDATE public.runtime_session SET version=version\\+1").WithArgs("tenant-a", "session-1", int64(1), driver.Value([]byte("{}"))).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT tenant_id, session_id, status, version, state").WithArgs("tenant-a", "session-1").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "session-1", "active", 2, []byte("{}"), when, when))
	if _, err := store.UpdateSessionState(context.Background(), "tenant-a", "session-1", 1, nil); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("CAS error = %v", err)
	}
	mock.ExpectQuery("UPDATE public.reply_outbox SET status='sending'").WithArgs("tenant-a", "reply-1", 0, "worker-a", int64(1)).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-a", "reply-1", 0).WillReturnRows(replyRow(when))
	if _, err := store.ClaimReply(context.Background(), "tenant-a", "reply-1", 0, "worker-a", time.Second); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("claim error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreValidationAndDecodeErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	ctx := context.Background()
	if _, err := store.GetSession(ctx, "", "session"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("invalid tenant = %v", err)
	}
	if _, err := store.CreateSession(ctx, "tenant-a", "session", map[string]any{"bad": make(chan int)}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("encode error = %v", err)
	}
	mock.ExpectQuery("SELECT tenant_id, session_id, status, version, state").WithArgs("tenant-a", "bad").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "bad", "active", 1, []byte("not-json"), time.Now(), time.Now()))
	if _, err := store.GetSession(ctx, "tenant-a", "bad"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("decode error = %v", err)
	}
	if _, err := store.UpdateSessionState(ctx, "tenant-a", "bad", 1, map[string]any{"bad": make(chan int)}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("update encode error = %v", err)
	}
	if _, err := store.GetMessage(ctx, "", "event"); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("message validation = %v", err)
	}
	if _, err := store.GetReply(ctx, "tenant-a", "", 0); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("reply validation = %v", err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply", EventID: "event", SegmentIndex: 0, SegmentCount: 1, Status: runtimestorage.ReplySent}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("reply status = %v", err)
	}
	if _, err := store.ClaimReply(ctx, "tenant-a", "reply", 0, "", time.Second); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("claim validation = %v", err)
	}
	if _, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply"}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("transition validation = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreRecordMessageDuplicateRaceRecovery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	when := time.Now().UTC()
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "binding-1", "external-1").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("UPDATE public.runtime_session SET version=version\\+1").WithArgs("tenant-a", "session-1").WillReturnRows(sqlmock.NewRows([]string{"version"}).AddRow(int64(2)))
	mock.ExpectQuery("INSERT INTO public.message_event").WithArgs("tenant-a", "event-2", "session-1", "binding-1", "external-1", "idem-2", int64(2)).WillReturnError(&pgconn.PgError{Code: "23505"})
	mock.ExpectRollback()
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "binding-1", "external-1").WillReturnRows(eventRow(when))
	value, duplicate, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-2", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1", IdempotencyKey: "idem-2"})
	if err != nil || !duplicate || value.EventID != "event-1" {
		t.Fatalf("duplicate recovery = %+v duplicate=%v err=%v", value, duplicate, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStorePostgresErrorBranches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	ctx := context.Background()
	when := time.Now().UTC()

	mock.ExpectQuery("SELECT tenant_id, session_id, status, version, state").WithArgs("tenant-a", "error").WillReturnError(errors.New("query failed"))
	if _, err := store.GetSession(ctx, "tenant-a", "error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("get session error = %v", err)
	}
	mock.ExpectQuery("INSERT INTO public.runtime_session").WithArgs("tenant-a", "create-error", driver.Value([]byte("{}"))).WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "create-error", "active", 1, []byte("bad"), when, when))
	if _, err := store.CreateSession(ctx, "tenant-a", "create-error", nil); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("create decode error = %v", err)
	}
	mock.ExpectQuery("UPDATE public.runtime_session SET version=version\\+1").WithArgs("tenant-a", "update-error", int64(1), driver.Value([]byte("{}"))).WillReturnError(errors.New("update failed"))
	if _, err := store.UpdateSessionState(ctx, "tenant-a", "update-error", 1, nil); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("update query error = %v", err)
	}

	mock.ExpectBegin().WillReturnError(errors.New("begin failed"))
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event", SessionID: "session", BindingID: "binding", ExternalMessageID: "external"}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("begin error = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "binding", "external").WillReturnRows(eventRow(when))
	mock.ExpectRollback()
	if _, duplicate, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event", SessionID: "session", BindingID: "binding", ExternalMessageID: "external"}); err != nil || !duplicate {
		t.Fatalf("existing message = duplicate=%v err=%v", duplicate, err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "binding", "external-err").WillReturnError(errors.New("lookup failed"))
	mock.ExpectRollback()
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event", SessionID: "session", BindingID: "binding", ExternalMessageID: "external-err"}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("message lookup error = %v", err)
	}
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "binding", "external-update").WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("UPDATE public.runtime_session SET version=version\\+1").WithArgs("tenant-a", "session").WillReturnError(errors.New("version update failed"))
	mock.ExpectRollback()
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event", SessionID: "session", BindingID: "binding", ExternalMessageID: "external-update"}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("message update error = %v", err)
	}

	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "event-error").WillReturnError(errors.New("message read failed"))
	if _, err := store.GetMessage(ctx, "tenant-a", "event-error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("get message error = %v", err)
	}
	mock.ExpectQuery("INSERT INTO public.reply_outbox").WithArgs("tenant-a", "reply-error", "event", 0, 1, "").WillReturnError(errors.New("enqueue failed"))
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: "tenant-a", ReplyID: "reply-error", EventID: "event", SegmentIndex: 0, SegmentCount: 1}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("enqueue error = %v", err)
	}
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-a", "reply-error", 0).WillReturnError(errors.New("reply read failed"))
	if _, err := store.GetReply(ctx, "tenant-a", "reply-error", 0); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("get reply error = %v", err)
	}
	mock.ExpectQuery("UPDATE public.reply_outbox SET status='sending'").WithArgs("tenant-a", "reply-error", 0, "worker", int64(1)).WillReturnError(errors.New("claim failed"))
	if _, err := store.ClaimReply(ctx, "tenant-a", "reply-error", 0, "worker", time.Second); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("claim error = %v", err)
	}
	mock.ExpectQuery("UPDATE public.reply_outbox SET status=\\$5").WithArgs("tenant-a", "reply-error", 0, "sending", "sent", "worker", int64(0), "", "", int64(1)).WillReturnError(errors.New("transition failed"))
	if _, err := store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply-error", SegmentIndex: 0, From: "sending", To: "sent", Owner: "worker", FencingToken: 1}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("transition error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreMissingReplyBranches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	mock.ExpectQuery("UPDATE public.reply_outbox SET status='sending'").WithArgs("tenant-a", "reply-missing", 0, "worker", int64(1)).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-a", "reply-missing", 0).WillReturnError(sql.ErrNoRows)
	if _, err := store.ClaimReply(context.Background(), "tenant-a", "reply-missing", 0, "worker", time.Second); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing claim = %v", err)
	}
	mock.ExpectQuery("UPDATE public.reply_outbox SET status=\\$5").WithArgs("tenant-a", "reply-missing", 0, "sending", "sent", "worker", int64(0), "", "", int64(1)).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-a", "reply-missing", 0).WillReturnError(sql.ErrNoRows)
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply-missing", SegmentIndex: 0, From: "sending", To: "sent", Owner: "worker", FencingToken: 1}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing transition = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreTransitionValidationAndLease(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply", Owner: "worker", From: runtimestorage.ReplySent, To: runtimestorage.ReplySending}); !errors.Is(err, runtimestorage.ErrIllegalTransition) {
		t.Fatalf("illegal transition = %v", err)
	}
	when := time.Now().UTC()
	mock.ExpectQuery("UPDATE public.reply_outbox SET status=\\$5").WithArgs("tenant-a", "reply-lease", 0, "pending", "sending", "worker", int64(2), "", "", int64(0)).WillReturnRows(sqlmock.NewRows(replyColumns).AddRow("tenant-a", "reply-lease", "event", 0, 1, "payload", "sending", 1, int64(1), "worker", when.Add(time.Minute), "", "", when, when))
	if _, err := store.TransitionReply(context.Background(), runtimestorage.ReplyTransition{TenantID: "tenant-a", ReplyID: "reply-lease", SegmentIndex: 0, From: "pending", To: "sending", Owner: "worker", LeaseDuration: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreTransitionMessageExpiredLeaseReconciliation(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	when := time.Now().UTC()
	mock.ExpectQuery("UPDATE public.message_event SET status=\\$4").WithArgs("tenant-a", "event-expired", runtimestorage.EventRunning, runtimestorage.EventExecutionReconciling, "reconciler", int64(0), int64(7)).WillReturnRows(sqlmock.NewRows(eventColumns).AddRow("tenant-a", "event-expired", "session-1", "binding-1", "external-1", "idem-1", int64(2), runtimestorage.EventExecutionReconciling, int64(8), "", nil, "", 0, when, when))
	value, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event-expired", From: runtimestorage.EventRunning, To: runtimestorage.EventExecutionReconciling, Owner: "reconciler", FencingToken: 7})
	if err != nil || value.Status != runtimestorage.EventExecutionReconciling || value.FencingToken != 8 {
		t.Fatalf("expired transition = %+v err=%v", value, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreTransitionMessageNoRowsMapsConflictOrNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	transition := runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event-1", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker", LeaseDuration: time.Second}
	mock.ExpectQuery("UPDATE public.message_event SET status=\\$4").WithArgs("tenant-a", "event-1", runtimestorage.EventReceived, runtimestorage.EventRunning, "worker", int64(1), int64(0)).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "event-1").WillReturnRows(eventRow(time.Now().UTC()))
	if _, err := store.TransitionMessage(context.Background(), transition); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("existing event conflict = %v", err)
	}
	transition.EventID = "missing"
	mock.ExpectQuery("UPDATE public.message_event SET status=\\$4").WithArgs("tenant-a", "missing", runtimestorage.EventReceived, runtimestorage.EventRunning, "worker", int64(1), int64(0)).WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SELECT tenant_id,event_id,session_id,binding_id,external_message_id").WithArgs("tenant-a", "missing").WillReturnError(sql.ErrNoRows)
	if _, err := store.TransitionMessage(context.Background(), transition); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("missing event = %v", err)
	}
	mock.ExpectQuery("UPDATE public.message_event SET status=\\$4").WithArgs("tenant-a", "query-error", runtimestorage.EventReceived, runtimestorage.EventRunning, "worker", int64(1), int64(0)).WillReturnError(errors.New("transition query failed"))
	transition.EventID = "query-error"
	if _, err := store.TransitionMessage(context.Background(), transition); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("transition query error = %v", err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "", EventID: "event", From: runtimestorage.EventReceived, To: runtimestorage.EventRunning, Owner: "worker", LeaseDuration: time.Second}); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("transition validation = %v", err)
	}
	if _, err := store.TransitionMessage(context.Background(), runtimestorage.MessageTransition{TenantID: "tenant-a", EventID: "event", From: runtimestorage.EventCompleted, To: runtimestorage.EventRunning, Owner: "worker"}); !errors.Is(err, runtimestorage.ErrIllegalTransition) {
		t.Fatalf("transition illegal = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreAppendEventPayloadValidationAndErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	ctx := context.Background()
	invalid := []runtimestorage.EventPayload{
		{TenantID: "", SessionID: "session", EventID: "event", Payload: []byte("{}")},
		{TenantID: "tenant-a", SessionID: "", EventID: "event", Payload: []byte("{}")},
		{TenantID: "tenant-a", SessionID: "session", EventID: "", Payload: []byte("{}")},
		{TenantID: "tenant-a", SessionID: "session", EventID: "event", Payload: nil},
		{TenantID: "tenant-a", SessionID: "session", EventID: "event", Payload: []byte("not-json")},
	}
	for i, payload := range invalid {
		if _, err := store.AppendEventPayload(ctx, payload); !errors.Is(err, runtimestorage.ErrInvalid) {
			t.Errorf("invalid payload %d = %v", i, err)
		}
	}
	payload := []byte("{\"ok\":true}")
	mock.ExpectQuery("INSERT INTO public.runtime_event_history").WithArgs("tenant-a", "session", "event", payload).WillReturnError(sql.ErrNoRows)
	if _, err := store.AppendEventPayload(ctx, runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "event", Payload: payload}); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("duplicate payload = %v", err)
	}
	mock.ExpectQuery("INSERT INTO public.runtime_event_history").WithArgs("tenant-a", "session", "error", payload).WillReturnError(errors.New("insert failed"))
	if _, err := store.AppendEventPayload(ctx, runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "error", Payload: payload}); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("insert error = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.AppendEventPayload(canceled, runtimestorage.EventPayload{TenantID: "tenant-a", SessionID: "session", EventID: "event", Payload: payload}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled append = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreListEventPayloadsEmptyAndErrorBranches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	when := time.Now().UTC()
	mock.ExpectQuery("SELECT tenant_id,session_id,event_id,payload::text").WithArgs("tenant-a", "query-error").WillReturnError(errors.New("history query failed"))
	if _, err := store.ListEventPayloads(context.Background(), "tenant-a", "query-error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("history query error = %v", err)
	}
	mock.ExpectQuery("SELECT tenant_id,session_id,event_id,payload::text").WithArgs("tenant-a", "scan-error").WillReturnRows(sqlmock.NewRows(historyColumns).AddRow("tenant-a", "scan-error", "event", []byte("{}"), "bad-sequence", when))
	if _, err := store.ListEventPayloads(context.Background(), "tenant-a", "scan-error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("history scan error = %v", err)
	}
	mock.ExpectQuery("SELECT tenant_id,session_id,event_id,payload::text").WithArgs("tenant-a", "rows-error").WillReturnRows(sqlmock.NewRows(historyColumns).AddRow("tenant-a", "rows-error", "event", []byte("{}"), int64(1), when).AddRow("tenant-a", "rows-error", "event-2", []byte("{}"), int64(2), when).RowError(1, errors.New("rows failed")))
	if _, err := store.ListEventPayloads(context.Background(), "tenant-a", "rows-error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("history rows error = %v", err)
	}
	mock.ExpectQuery("SELECT tenant_id,session_id,event_id,payload::text").WithArgs("tenant-a", "empty").WillReturnRows(sqlmock.NewRows(historyColumns))
	mock.ExpectQuery("SELECT tenant_id, session_id, status, version, state").WithArgs("tenant-a", "empty").WillReturnRows(sqlmock.NewRows([]string{"tenant_id", "session_id", "status", "version", "state", "created_at", "updated_at"}).AddRow("tenant-a", "empty", "active", 1, []byte("{}"), when, when))
	values, err := store.ListEventPayloads(context.Background(), "tenant-a", "empty")
	if err != nil || values == nil || len(values) != 0 {
		t.Fatalf("empty history = %+v err=%v", values, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStoreListReplyCandidatesErrorBranches(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	store := runtimepostgres.New(db)
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-query-error").WillReturnError(errors.New("candidate query failed"))
	if _, err := store.ListReplyCandidates(context.Background(), "tenant-query-error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("candidate query error = %v", err)
	}
	when := time.Now().UTC()
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-scan-error").WillReturnRows(sqlmock.NewRows(replyColumns).AddRow("tenant-a", "reply", "event", 0, 1, "payload", "pending", "bad-attempts", int64(0), "", nil, "", "", when, when))
	if _, err := store.ListReplyCandidates(context.Background(), "tenant-scan-error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("candidate scan error = %v", err)
	}
	mock.ExpectQuery("SELECT tenant_id,reply_id,event_id,segment_index").WithArgs("tenant-rows-error").WillReturnRows(sqlmock.NewRows(replyColumns).AddRow("tenant-a", "reply", "event", 0, 1, "payload", "pending", 0, int64(0), "", nil, "", "", when, when).AddRow("tenant-a", "reply-2", "event", 0, 1, "payload", "pending", 0, int64(0), "", nil, "", "", when, when).RowError(1, errors.New("candidate rows failed")))
	if _, err := store.ListReplyCandidates(context.Background(), "tenant-rows-error"); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("candidate rows error = %v", err)
	}
	if _, err := store.ListReplyCandidates(context.Background(), ""); !errors.Is(err, runtimestorage.ErrInvalid) {
		t.Fatalf("candidate validation = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.ListReplyCandidates(canceled, "tenant-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("candidate canceled = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
