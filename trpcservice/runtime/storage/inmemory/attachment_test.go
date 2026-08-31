package inmemory

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestAttachmentStoreScopesBindsAndCleansUp(t *testing.T) {
	store := New()
	t.Cleanup(func() { _ = store.Close() })
	data := []byte("document")
	digest := sha256.Sum256(data)
	stored, err := store.PutAttachment(context.Background(), "tenant-a", attachment.Upload{ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data))}, strings.NewReader(string(data)))
	if err != nil {
		t.Fatalf("PutAttachment = %v", err)
	}
	reference := attachment.Reference{ID: "attachment-1", Kind: attachment.KindDocument, MIMEType: "application/pdf", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if stored != reference {
		t.Fatalf("stored reference = %+v", stored)
	}
	if err := store.BindAttachments(context.Background(), "tenant-a", "event-1", []attachment.Reference{reference}); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("binding unknown event = %v", err)
	}
	if _, err := store.CreateSession(context.Background(), "tenant-a", "session-1", nil); err != nil {
		t.Fatalf("CreateSession = %v", err)
	}
	if _, _, err := store.RecordMessage(context.Background(), runtimestorage.MessageEventInput{TenantID: "tenant-a", EventID: "event-1", SessionID: "session-1", BindingID: "binding-1", ExternalMessageID: "external-1"}); err != nil {
		t.Fatalf("RecordMessage = %v", err)
	}
	if err := store.BindAttachments(context.Background(), "tenant-a", "event-1", []attachment.Reference{reference}); err != nil {
		t.Fatalf("BindAttachments = %v", err)
	}
	content, err := store.Load(context.Background(), "tenant-a", "event-1", reference)
	if err != nil || string(content.Data) != string(data) {
		t.Fatalf("Load = %q, %v", content.Data, err)
	}
	if _, err := store.Load(context.Background(), "tenant-b", "event-1", reference); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant Load = %v", err)
	}
	reference.Size++
	if _, err := store.Load(context.Background(), "tenant-a", "event-1", reference); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("size mismatch Load = %v", err)
	}
	if removed, err := store.CleanupAttachments(context.Background(), "tenant-a", time.Now().UTC().Add(attachment.DefaultRetention)); err != nil || removed != 0 {
		t.Fatalf("bound cleanup = %d, %v", removed, err)
	}
	if err := store.DeleteSession(context.Background(), "tenant-a", "session-1"); err != nil {
		t.Fatalf("DeleteSession = %v", err)
	}
	if removed, err := store.CleanupAttachments(context.Background(), "tenant-a", time.Now().UTC().Add(attachment.DefaultRetention+time.Second)); err != nil || removed != 1 {
		t.Fatalf("orphaned cleanup = %d, %v", removed, err)
	}
}
