package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestInboundMessageNormalizesAttachmentOnlyInput(t *testing.T) {
	reference := testAttachmentReference(t, attachment.KindImage, "image/png", []byte("image"))
	message, err := (InboundMessage{Attachments: []attachment.Reference{reference}, ExternalMessageID: "message", ExternalUserID: "user", ConversationKind: ConversationDirect, ExternalPeerID: "peer"}).Normalize()
	if err != nil {
		t.Fatalf("Normalize = %v", err)
	}
	if message.ContentType != ContentTypeMedia || len(message.Attachments) != 1 {
		t.Fatalf("normalized message = %+v", message)
	}
}

func TestBuildUserMessageUsesVerifiedContentParts(t *testing.T) {
	data := []byte("image")
	reference := testAttachmentReference(t, attachment.KindImage, "image/png", data)
	reader := attachmentReaderFunc(func(context.Context, string, attachment.Reference) (attachment.Content, error) {
		return attachment.Content{Data: data}, nil
	})
	message, err := buildUserMessage(context.Background(), reader, "tenant", InboundMessage{Content: "describe", Attachments: []attachment.Reference{reference}})
	if err != nil {
		t.Fatalf("buildUserMessage = %v", err)
	}
	if message.Content != "describe" || len(message.ContentParts) != 1 || message.ContentParts[0].Type != trpcmodel.ContentTypeImage || string(message.ContentParts[0].Image.Data) != string(data) {
		t.Fatalf("message = %+v", message)
	}
}

func TestBuildUserMessageRejectsUnavailableOrTamperedAttachment(t *testing.T) {
	reference := testAttachmentReference(t, attachment.KindDocument, "application/pdf", []byte("document"))
	message := InboundMessage{Attachments: []attachment.Reference{reference}}
	if _, err := buildUserMessage(context.Background(), nil, "tenant", message); err == nil {
		t.Fatal("buildUserMessage accepted nil reader")
	}
	reader := attachmentReaderFunc(func(context.Context, string, attachment.Reference) (attachment.Content, error) {
		return attachment.Content{Data: []byte("tampered")}, nil
	})
	if _, err := buildUserMessage(context.Background(), reader, "tenant", message); err == nil {
		t.Fatal("buildUserMessage accepted tampered content")
	}
	reader = attachmentReaderFunc(func(context.Context, string, attachment.Reference) (attachment.Content, error) {
		return attachment.Content{}, errors.New("unavailable")
	})
	if _, err := buildUserMessage(context.Background(), reader, "tenant", message); err == nil {
		t.Fatal("buildUserMessage accepted reader error")
	}
}

type attachmentReaderFunc func(context.Context, string, attachment.Reference) (attachment.Content, error)

func (function attachmentReaderFunc) Load(ctx context.Context, tenantID string, reference attachment.Reference) (attachment.Content, error) {
	return function(ctx, tenantID, reference)
}

func testAttachmentReference(t *testing.T, kind attachment.Kind, contentType string, data []byte) attachment.Reference {
	t.Helper()
	digest := sha256.Sum256(data)
	reference := attachment.Reference{ID: "attachment-1", Kind: kind, MIMEType: contentType, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if _, err := reference.Normalize(); err != nil {
		t.Fatalf("test reference = %v", err)
	}
	return reference
}
