package attachment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	runtimeinmemory "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/inmemory"
)

func TestReferenceNormalizeAndContentValidate(t *testing.T) {
	data := []byte("image")
	digest := sha256.Sum256(data)
	reference := Reference{ID: "attachment-1", Kind: KindImage, MIMEType: "image/png", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:]), Provider: "telegram", ProviderID: "file-1"}
	normalized, err := reference.Normalize()
	if err != nil || normalized.MIMEType != "image/png" {
		t.Fatalf("Normalize = %+v, %v", normalized, err)
	}
	if err := (Content{Data: data}).Validate(normalized); err != nil {
		t.Fatalf("Validate = %v", err)
	}
	if err := (Content{Data: []byte("other")}).Validate(normalized); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched content error = %v", err)
	}
}

func TestStoreReaderScopesAndVerifiesObject(t *testing.T) {
	store := runtimeinmemory.New()
	t.Cleanup(func() { _ = store.Close() })
	data := []byte("document")
	digest := sha256.Sum256(data)
	if _, err := store.PutObject(context.Background(), "tenant-a", "attachment-1", strings.NewReader(string(data)), "application/pdf"); err != nil {
		t.Fatalf("PutObject = %v", err)
	}
	reader := StoreReader{Store: store}
	reference := Reference{ID: "attachment-1", Kind: KindDocument, MIMEType: "application/pdf", Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	content, err := reader.Load(context.Background(), "tenant-a", reference)
	if err != nil || string(content.Data) != string(data) {
		t.Fatalf("Load = %q, %v", content.Data, err)
	}
	if _, err := reader.Load(context.Background(), "tenant-b", reference); !errors.Is(err, runtimestorage.ErrNotFound) {
		t.Fatalf("cross-tenant Load = %v", err)
	}
	reference.Size++
	if _, err := reader.Load(context.Background(), "tenant-a", reference); !errors.Is(err, ErrInvalid) {
		t.Fatalf("size mismatch Load = %v", err)
	}
}
