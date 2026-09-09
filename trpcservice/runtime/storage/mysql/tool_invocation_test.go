package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestPrepareToolInvocationIsAppScopedAndIdempotent(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	input := mysqlToolInvocationInput()
	stored := mysqlToolInvocationValue(input, runtimestorage.ToolInvocationPrepared)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO runtime_tool_invocation (")).WithArgs(input.TenantID, input.AppID, input.InvocationID, input.EventID, input.RequestID, input.TraceID, input.ToolCallID, input.ToolName, input.ArgsSHA256, input.Owner).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+toolInvocationColumns+" FROM runtime_tool_invocation WHERE tenant_id=? AND app_id=? AND invocation_id=?")).WithArgs(input.TenantID, input.AppID, input.InvocationID).WillReturnRows(mysqlToolInvocationRows(stored))

	got, err := New(db).PrepareToolInvocation(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if got != stored {
		t.Fatalf("prepared value = %#v, want %#v", got, stored)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareToolInvocationRejectsExistingIdentityMismatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	input := mysqlToolInvocationInput()
	stored := mysqlToolInvocationValue(input, runtimestorage.ToolInvocationPrepared)
	stored.Owner = "another-owner"
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO runtime_tool_invocation (")).WithArgs(input.TenantID, input.AppID, input.InvocationID, input.EventID, input.RequestID, input.TraceID, input.ToolCallID, input.ToolName, input.ArgsSHA256, input.Owner).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+toolInvocationColumns+" FROM runtime_tool_invocation WHERE tenant_id=? AND app_id=? AND invocation_id=?")).WithArgs(input.TenantID, input.AppID, input.InvocationID).WillReturnRows(mysqlToolInvocationRows(stored))

	if _, err := New(db).PrepareToolInvocation(context.Background(), input); !errors.Is(err, runtimestorage.ErrConflict) {
		t.Fatalf("mismatched prepare = %v, want conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionToolInvocationUsesFenceAndMapsStorageErrors(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	input := mysqlToolInvocationInput()
	transition := runtimestorage.ToolInvocationTransition{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID,
		From: runtimestorage.ToolInvocationPrepared, To: runtimestorage.ToolInvocationDispatching,
		Owner: input.Owner, FencingToken: 1,
	}
	updated := mysqlToolInvocationValue(input, transition.To)
	updated.FencingToken = 2
	mock.ExpectExec(regexp.QuoteMeta("UPDATE runtime_tool_invocation SET status=?,error_class=?,reviewer_id=?,fencing_token=fencing_token+1")).WithArgs(string(transition.To), "", "", input.TenantID, input.AppID, input.InvocationID, string(transition.From), int64(1), input.Owner, input.Owner, string(transition.To), string(transition.From)).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT "+toolInvocationColumns+" FROM runtime_tool_invocation WHERE tenant_id=? AND app_id=? AND invocation_id=?")).WithArgs(input.TenantID, input.AppID, input.InvocationID).WillReturnRows(mysqlToolInvocationRows(updated))

	got, err := New(db).TransitionToolInvocation(context.Background(), transition)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != runtimestorage.ToolInvocationDispatching || got.FencingToken != 2 {
		t.Fatalf("transition value = %#v", got)
	}

	mock.ExpectExec(regexp.QuoteMeta("UPDATE runtime_tool_invocation SET status=?,error_class=?,reviewer_id=?,fencing_token=fencing_token+1")).WithArgs(string(transition.To), "", "", input.TenantID, input.AppID, input.InvocationID, string(transition.From), int64(1), input.Owner, input.Owner, string(transition.To), string(transition.From)).WillReturnError(errors.New("driver password details"))
	if _, err := New(db).TransitionToolInvocation(context.Background(), transition); !errors.Is(err, runtimestorage.ErrStorage) {
		t.Fatalf("driver error = %v, want runtime storage error", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverStaleToolInvocationsFencesRowsAtomically(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })

	input := mysqlToolInvocationInput()
	stored := mysqlToolInvocationValue(input, runtimestorage.ToolInvocationAccepted)
	before := stored.UpdatedAt.Add(time.Second)
	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT " + toolInvocationColumns + " FROM runtime_tool_invocation WHERE status IN ('dispatching','accepted')")).WithArgs(before).WillReturnRows(mysqlToolInvocationRows(stored))
	mock.ExpectExec(regexp.QuoteMeta("UPDATE runtime_tool_invocation SET status='unknown',error_class='provider_uncertain'")).WithArgs(sqlmock.AnyArg(), input.TenantID, input.AppID, input.InvocationID, string(stored.Status), stored.FencingToken).WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	got, err := New(db).RecoverStaleToolInvocations(context.Background(), before)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Status != runtimestorage.ToolInvocationUnknown || got[0].FencingToken != stored.FencingToken+1 || got[0].ErrorClass != "provider_uncertain" {
		t.Fatalf("recovered values = %#v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func mysqlToolInvocationInput() runtimestorage.ToolInvocationInput {
	args := sha256.Sum256([]byte(`{"value":1}`))
	input := runtimestorage.ToolInvocationInput{
		TenantID: "tenant-a", AppID: "app_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		EventID: "event-a", RequestID: "request-a", TraceID: "trace-a", ToolCallID: "call-a",
		ToolName: "mcp__write", ArgsSHA256: hex.EncodeToString(args[:]), Owner: "request-a",
	}
	input.InvocationID = runtimestorage.DeriveToolInvocationIDForApp(input.TenantID, input.AppID, input.EventID, input.ToolCallID, input.ToolName, input.ArgsSHA256)
	return input
}

func mysqlToolInvocationValue(input runtimestorage.ToolInvocationInput, status runtimestorage.ToolInvocationStatus) runtimestorage.ToolInvocation {
	now := time.Now().UTC().Truncate(time.Microsecond)
	return runtimestorage.ToolInvocation{
		TenantID: input.TenantID, AppID: input.AppID, InvocationID: input.InvocationID,
		EventID: input.EventID, RequestID: input.RequestID, TraceID: input.TraceID,
		ToolCallID: input.ToolCallID, ToolName: input.ToolName, ArgsSHA256: input.ArgsSHA256,
		Status: status, Owner: input.Owner, FencingToken: 1, CreatedAt: now, UpdatedAt: now,
	}
}

func mysqlToolInvocationRows(value runtimestorage.ToolInvocation) *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"tenant_id", "app_id", "invocation_id", "event_id", "request_id", "trace_id", "tool_call_id", "tool_name", "args_sha256", "status", "owner", "fencing_token", "error_class", "reviewer_id", "created_at", "updated_at",
	}).AddRow(value.TenantID, value.AppID, value.InvocationID, value.EventID, value.RequestID, value.TraceID, value.ToolCallID, value.ToolName, value.ArgsSHA256, string(value.Status), value.Owner, value.FencingToken, value.ErrorClass, value.ReviewerID, value.CreatedAt, value.UpdatedAt)
}
