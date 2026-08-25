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

func TestReplyTargetValidation(t *testing.T) {
	valid := storage.ReplyTarget{BindingID: "binding-1", ConversationKind: "direct", ReceiverID: "user-1", ThreadID: "topic-1"}
	if err := storage.ValidateReplyTarget(valid); err != nil {
		t.Fatalf("valid target = %v", err)
	}
	for _, target := range []storage.ReplyTarget{
		{BindingID: "binding-1"},
		{BindingID: "binding-1", ConversationKind: "unknown", ReceiverID: "user-1"},
		{BindingID: "binding-1", ConversationKind: "group"},
		{BindingID: "binding-1", ConversationKind: "group", ReceiverID: "user-1", ThreadID: "bad\nthread"},
	} {
		if !errors.Is(storage.ValidateReplyTarget(target), storage.ErrInvalid) {
			t.Fatalf("invalid target accepted: %+v", target)
		}
	}
	if err := storage.ValidateReplyTarget(storage.ReplyTarget{}); err != nil {
		t.Fatalf("legacy zero target = %v", err)
	}
}
