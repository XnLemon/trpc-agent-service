// Package postgres implements queue.Store on PostgreSQL.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/queue"
)

type Store struct{ db *sql.DB }

func New(db *sql.DB) (*Store, error) {
	if db == nil {
		return nil, queue.ErrInvalid
	}
	return &Store{db: db}, nil
}

const columns = "tenant_id,task_id,kind,payload,status,attempts,fencing_token,lease_owner,lease_expires_at,next_attempt_at,last_error_class,created_at,updated_at"

func (s *Store) Enqueue(ctx context.Context, input queue.TaskInput) (queue.Task, bool, error) {
	if err := check(ctx); err != nil {
		return queue.Task{}, false, err
	}
	if input.TenantID == "" || input.TaskID == "" || input.Kind == "" || len(input.Payload) == 0 {
		return queue.Task{}, false, queue.ErrInvalid
	}
	var value queue.Task
	err := s.db.QueryRowContext(ctx, "INSERT INTO public.runtime_execution_queue (tenant_id,task_id,kind,payload) VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id,task_id) DO UPDATE SET updated_at=public.runtime_execution_queue.updated_at WHERE public.runtime_execution_queue.kind=EXCLUDED.kind AND public.runtime_execution_queue.payload=EXCLUDED.payload RETURNING "+columns, input.TenantID, input.TaskID, input.Kind, input.Payload).Scan(args(&value)...)
	if err == nil {
		return value, false, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return queue.Task{}, false, err
	}
	value, lookupErr := s.Get(ctx, input.TenantID, input.TaskID)
	if lookupErr != nil {
		return queue.Task{}, false, lookupErr
	}
	if value.Kind != input.Kind || string(value.Payload) != string(input.Payload) {
		return queue.Task{}, false, queue.ErrConflict
	}
	return value, true, nil
}

func (s *Store) Get(ctx context.Context, tenantID, taskID string) (queue.Task, error) {
	if err := check(ctx); err != nil {
		return queue.Task{}, err
	}
	if tenantID == "" || taskID == "" {
		return queue.Task{}, queue.ErrInvalid
	}
	var value queue.Task
	if err := s.db.QueryRowContext(ctx, "SELECT "+columns+" FROM public.runtime_execution_queue WHERE tenant_id=$1 AND task_id=$2", tenantID, taskID).Scan(args(&value)...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return queue.Task{}, queue.ErrNotFound
		}
		return queue.Task{}, err
	}
	return value, nil
}

func (s *Store) Claim(ctx context.Context, tenantID, owner string, lease time.Duration) (queue.Task, error) {
	if err := check(ctx); err != nil {
		return queue.Task{}, err
	}
	if owner == "" || lease <= 0 {
		return queue.Task{}, queue.ErrInvalid
	}
	seconds := int64(lease / time.Second)
	if seconds == 0 {
		seconds = 1
	}
	var value queue.Task
	err := s.db.QueryRowContext(ctx, "WITH candidate AS (SELECT tenant_id,task_id FROM public.runtime_execution_queue WHERE ($1='' OR tenant_id=$1) AND ((status IN ('queued','retryable') AND next_attempt_at<=now()) OR (status='leased' AND lease_expires_at<=now())) ORDER BY created_at,task_id FOR UPDATE SKIP LOCKED LIMIT 1) UPDATE public.runtime_execution_queue q SET status='leased',attempts=q.attempts+1,fencing_token=q.fencing_token+1,lease_owner=$2,lease_expires_at=now()+($3 * interval '1 second'),updated_at=now() FROM candidate c WHERE q.tenant_id=c.tenant_id AND q.task_id=c.task_id RETURNING "+columns, tenantID, owner, seconds).Scan(args(&value)...)
	if errors.Is(err, sql.ErrNoRows) {
		return queue.Task{}, queue.ErrNotFound
	}
	return value, err
}

func (s *Store) Complete(ctx context.Context, tenantID, taskID, owner string, fence int64) (queue.Task, error) {
	return s.transition(ctx, tenantID, taskID, owner, fence, queue.StatusCompleted, "", "NULL")
}

func (s *Store) Retry(ctx context.Context, tenantID, taskID, owner string, fence int64, next time.Time, class string) (queue.Task, error) {
	return s.transition(ctx, tenantID, taskID, owner, fence, queue.StatusRetryable, class, "$6")
}

func (s *Store) Fail(ctx context.Context, tenantID, taskID, owner string, fence int64, class string) (queue.Task, error) {
	return s.transition(ctx, tenantID, taskID, owner, fence, queue.StatusFailed, class, "NULL")
}

func (s *Store) transition(ctx context.Context, tenantID, taskID, owner string, fence int64, status queue.Status, class, nextExpr string) (queue.Task, error) {
	if err := check(ctx); err != nil {
		return queue.Task{}, err
	}
	if tenantID == "" || taskID == "" || owner == "" || fence <= 0 {
		return queue.Task{}, queue.ErrInvalid
	}
	query := "UPDATE public.runtime_execution_queue SET status=$4,lease_owner='',lease_expires_at=NULL,next_attempt_at=" + nextExpr + ",last_error_class=NULLIF($5,''),updated_at=now() WHERE tenant_id=$1 AND task_id=$2 AND status='leased' AND lease_owner=$3 AND fencing_token=$6 AND lease_expires_at>now() RETURNING " + columns
	argsList := []any{tenantID, taskID, owner, string(status), class, fence}
	if nextExpr == "$6" {
		query = "UPDATE public.runtime_execution_queue SET status=$4,lease_owner='',lease_expires_at=NULL,next_attempt_at=$6,last_error_class=NULLIF($5,''),updated_at=now() WHERE tenant_id=$1 AND task_id=$2 AND status='leased' AND lease_owner=$3 AND fencing_token=$7 AND lease_expires_at>now() RETURNING " + columns
		argsList = []any{tenantID, taskID, owner, string(status), class, time.Now().UTC(), fence}
	}
	var value queue.Task
	err := s.db.QueryRowContext(ctx, query, argsList...).Scan(args(&value)...)
	if errors.Is(err, sql.ErrNoRows) {
		if _, lookupErr := s.Get(ctx, tenantID, taskID); lookupErr != nil {
			return queue.Task{}, lookupErr
		}
		return queue.Task{}, queue.ErrConflict
	}
	return value, err
}

func (s *Store) Close() error { return nil }

func check(ctx context.Context) error {
	if ctx == nil {
		return queue.ErrInvalid
	}
	return ctx.Err()
}

func args(value *queue.Task) []any {
	return []any{&value.TenantID, &value.TaskID, &value.Kind, &value.Payload, &value.Status, &value.Attempts, &value.FencingToken, &value.LeaseOwner, &value.LeaseExpiresAt, &value.NextAttemptAt, &value.LastErrorClass, &value.CreatedAt, &value.UpdatedAt}
}

var _ queue.Store = (*Store)(nil)
