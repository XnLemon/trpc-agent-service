package storage_test

import (
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestValidationContracts(t *testing.T) {
	if !errors.Is(storage.ValidateTenant(""), storage.ErrInvalid) || storage.ValidateTenant("tenant-a") != nil {
		t.Fatal("tenant validation contract changed")
	}
	if !errors.Is(storage.ValidateSession("", "session"), storage.ErrInvalid) || !errors.Is(storage.ValidateSession("tenant", ""), storage.ErrInvalid) || storage.ValidateSession("tenant", "session") != nil {
		t.Fatal("session validation contract changed")
	}
	valid := [][2]string{{storage.ReplyPending, storage.ReplySending}, {storage.ReplyPending, storage.ReplyRetryable}, {storage.ReplySending, storage.ReplySent}, {storage.ReplySending, storage.ReplyRetryable}, {storage.ReplySending, storage.ReplyDeadLetter}, {storage.ReplyRetryable, storage.ReplySending}, {storage.ReplyRetryable, storage.ReplyDeadLetter}}
	for _, edge := range valid {
		if !storage.ValidateTransition(edge[0], edge[1]) {
			t.Fatalf("valid transition %q -> %q rejected", edge[0], edge[1])
		}
	}
	invalid := [][2]string{{storage.ReplySent, storage.ReplySending}, {storage.ReplyDeadLetter, storage.ReplySending}, {"unknown", storage.ReplySending}}
	for _, edge := range invalid {
		if storage.ValidateTransition(edge[0], edge[1]) {
			t.Fatalf("invalid transition %q -> %q accepted", edge[0], edge[1])
		}
	}
}
