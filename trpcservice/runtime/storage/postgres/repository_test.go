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
