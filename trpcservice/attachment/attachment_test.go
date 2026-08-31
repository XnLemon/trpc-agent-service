package attachment

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"
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

func TestUploadNormalizeAppliesBoundedRetentionAndMediaFamily(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	normalized, err := (Upload{ID: "attachment-1", Kind: KindVideo, MIMEType: "video/mp4", Size: 1}).Normalize(now)
	if err != nil || !normalized.ExpiresAt.Equal(now.Add(DefaultRetention)) {
		t.Fatalf("Normalize = %+v, %v", normalized, err)
	}
	if _, err := (Upload{ID: "attachment-1", Kind: KindImage, MIMEType: "application/pdf", Size: 1}).Normalize(now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched media family error = %v", err)
	}
	if _, err := (Upload{ID: "attachment-1", Kind: KindDocument, MIMEType: "application/pdf", Size: 1, ExpiresAt: now.Add(DefaultRetention + time.Second)}).Normalize(now); !errors.Is(err, ErrInvalid) {
		t.Fatalf("excess retention error = %v", err)
	}
}
