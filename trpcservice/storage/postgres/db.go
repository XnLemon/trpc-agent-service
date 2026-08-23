// Package postgres contains durable PostgreSQL implementations of the
// control-plane repositories. It deliberately exposes database/sql rather
// than pgx types so callers can own pooling and lifecycle separately.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// ErrStorage is the stable error category returned for unexpected database
// failures. The underlying driver error is intentionally not exposed to the
// Gateway because it may contain connection details or provider metadata.
var ErrStorage = errors.New("postgres storage error")

// Options configures a database/sql pool opened by Open. A caller that
// already owns a pool can pass it directly to repository constructors.
type Options struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// Open creates and pings a pgx-backed database/sql pool. The ping is part of
// bootstrap readiness; it is not repeated by repository constructors.
func Open(ctx context.Context, dsn string, options Options) (*sql.DB, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dsn = normalizeDSN(dsn)
	if dsn == "" {
		return nil, ErrStorage
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, ErrStorage
	}
	if options.MaxOpenConns > 0 {
		db.SetMaxOpenConns(options.MaxOpenConns)
	}
	if options.MaxIdleConns > 0 {
		db.SetMaxIdleConns(options.MaxIdleConns)
	}
	if options.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(options.ConnMaxLifetime)
	}
	if options.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(options.ConnMaxIdleTime)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, mapDBError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return db, nil
}

// Ping is used by readiness probes and does not disclose the driver error.
func Ping(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		return mapDBError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return nil
}

func normalizeDSN(dsn string) string {
	dsn = strings.TrimSpace(dsn)
	for _, prefix := range []string{"postgresql+psycopg://", "postgres+psycopg://"} {
		if strings.HasPrefix(strings.ToLower(dsn), prefix) {
			return "postgresql://" + dsn[len(prefix):]
		}
	}
	return dsn
}

type queryer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func begin(ctx context.Context, db *sql.DB) (*sql.Tx, error) {
	if db == nil {
		return nil, ErrStorage
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, mapDBError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return tx, nil
}

func rollback(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}

func mapDBError(ctx context.Context, err error, notFound, duplicate, conflict, invalid error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, sql.ErrNoRows) {
		return notFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505":
			return duplicate
		case "23503", "23514", "22P02", "22001":
			return invalid
		case "40001", "40P01":
			return conflict
		}
	}
	return ErrStorage
}

func commit(ctx context.Context, tx *sql.Tx) error {
	if tx == nil {
		return ErrStorage
	}
	if err := ctx.Err(); err != nil {
		rollback(tx)
		return err
	}
	if err := tx.Commit(); err != nil {
		return mapDBError(ctx, err, ErrStorage, ErrStorage, ErrStorage, ErrStorage)
	}
	return nil
}

func asUTC(value time.Time) time.Time {
	return value.UTC()
}

func nullableInt(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	copy := value.Int64
	return &copy
}

func nullableString(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	copy := value.String
	return &copy
}
