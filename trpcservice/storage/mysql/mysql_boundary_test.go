package mysql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStorageRejectsCancelledContextsBeforeQueries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name string
		call func() error
	}{
		{"open", func() error { _, err := Open(ctx, "root:secret@tcp(localhost:3306)/db", Options{}); return err }},
		{"ping", func() error { return Ping(ctx, nil) }},
		{"begin", func() error { _, err := Begin(ctx, nil); return err }},
		{"begin conn", func() error { _, err := BeginConn(ctx, nil); return err }},
		{"acquire lock", func() error { return AcquireLock(ctx, nil, "name", 1) }},
		{"release lock", func() error { return ReleaseLock(ctx, nil, "name") }},
		{"current user", func() error { _, err := CurrentUser(ctx, nil); return err }},
		{"current database", func() error { _, err := CurrentDatabase(ctx, nil); return err }},
		{"privileges", func() error { return VerifyApplicationPrivileges(ctx, nil) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, context.Canceled) && !errors.Is(err, ErrStorage) {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestStorageNilContextsFailClosed(t *testing.T) {
	if err := Ping(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("Ping(nil) = %v", err)
	}
	if _, err := Begin(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("Begin(nil) = %v", err)
	}
	if _, err := BeginConn(nil, nil); !errors.Is(err, ErrStorage) {
		t.Fatalf("BeginConn(nil) = %v", err)
	}
	if err := AcquireLock(nil, nil, "name", 1); !errors.Is(err, ErrStorage) {
		t.Fatalf("AcquireLock(nil) = %v", err)
	}
	if err := ReleaseLock(nil, nil, "name"); !errors.Is(err, ErrStorage) {
		t.Fatalf("ReleaseLock(nil) = %v", err)
	}
}

func TestMonotonicNowDoesNotMoveBeforePersistedTime(t *testing.T) {
	previous := time.Now().UTC().Add(time.Hour)
	if got := MonotonicNow(previous); !got.Equal(previous) {
		t.Fatalf("MonotonicNow(%v) = %v, want persisted timestamp", previous, got)
	}
}
