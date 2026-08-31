// Package attachment defines protocol-neutral, tenant-scoped media references.
package attachment

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"strings"
	"unicode/utf8"
)

var (
	// ErrInvalid reports malformed attachment metadata or content.
	ErrInvalid = errors.New("invalid attachment")
)

const (
	maxReferenceIDRunes = 256
	maxProviderRunes    = 64
	maxProviderIDRunes  = 512
	maxNameRunes        = 512
	maxMIMETypeRunes    = 256
	maxSizeBytes        = 64 << 20
)

// Kind identifies the protocol-neutral media category of an attachment.
type Kind string

const (
	// KindImage identifies an image attachment.
	KindImage Kind = "image"
	// KindVideo identifies a video attachment.
	KindVideo Kind = "video"
	// KindAudio identifies an audio attachment.
	KindAudio Kind = "audio"
	// KindDocument identifies a document or other file attachment.
	KindDocument Kind = "document"
)

// Reference identifies one attachment owned by the authenticated tenant. It
// intentionally contains no provider URL, credential, or provider fetch token.
type Reference struct {
	ID         string
	Kind       Kind
	MIMEType   string
	Name       string
	Size       int64
	SHA256     string
	Provider   string
	ProviderID string
}

// Normalize validates a Reference and returns its canonical representation.
func (reference Reference) Normalize() (Reference, error) {
	value := reference
	value.ID = strings.TrimSpace(value.ID)
	value.Name = strings.TrimSpace(value.Name)
	value.MIMEType = strings.ToLower(strings.TrimSpace(value.MIMEType))
	value.SHA256 = strings.ToLower(strings.TrimSpace(value.SHA256))
	value.Provider = strings.ToLower(strings.TrimSpace(value.Provider))
	value.ProviderID = strings.TrimSpace(value.ProviderID)
	if !validText(value.ID, maxReferenceIDRunes, true) {
		return Reference{}, fmt.Errorf("%w: reference ID is invalid", ErrInvalid)
	}
	switch value.Kind {
	case KindImage, KindVideo, KindAudio, KindDocument:
	default:
		return Reference{}, fmt.Errorf("%w: kind is invalid", ErrInvalid)
	}
	if !validText(value.Name, maxNameRunes, false) || !validText(value.MIMEType, maxMIMETypeRunes, true) || !validText(value.Provider, maxProviderRunes, false) || !validText(value.ProviderID, maxProviderIDRunes, false) {
		return Reference{}, fmt.Errorf("%w: name or MIME type is invalid", ErrInvalid)
	}
	mediaType, params, err := mime.ParseMediaType(value.MIMEType)
	if err != nil || mediaType != value.MIMEType || len(params) != 0 || !strings.Contains(value.MIMEType, "/") {
		return Reference{}, fmt.Errorf("%w: MIME type is invalid", ErrInvalid)
	}
	if value.Size < 1 || value.Size > maxSizeBytes {
		return Reference{}, fmt.Errorf("%w: size is invalid", ErrInvalid)
	}
	if len(value.SHA256) != sha256.Size*2 || !isLowerHex(value.SHA256) {
		return Reference{}, fmt.Errorf("%w: digest is invalid", ErrInvalid)
	}
	return value, nil
}

// Content is the verified data loaded for one Reference. Callers own Data and
// must not retain it beyond the model request that consumes it.
type Content struct {
	Data []byte
}

// Validate checks that Content is the exact content described by reference.
func (content Content) Validate(reference Reference) error {
	if _, err := reference.Normalize(); err != nil {
		return err
	}
	if int64(len(content.Data)) != reference.Size {
		return fmt.Errorf("%w: content size does not match reference", ErrInvalid)
	}
	digest := sha256.Sum256(content.Data)
	if hex.EncodeToString(digest[:]) != reference.SHA256 {
		return fmt.Errorf("%w: content digest does not match reference", ErrInvalid)
	}
	return nil
}

// Clone returns an independent copy of Content.
func (content Content) Clone() Content {
	return Content{Data: append([]byte(nil), content.Data...)}
}

// Reader loads tenant-owned attachment data for a validated Reference. The
// reader must enforce tenant scope and return a defensive copy of the bytes.
type Reader interface {
	Load(context.Context, string, Reference) (Content, error)
}

func validText(value string, maximum int, required bool) bool {
	if (required && value == "") || !utf8.ValidString(value) || len([]rune(value)) > maximum {
		return false
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return false
		}
	}
	return true
}

func isLowerHex(value string) bool {
	for _, character := range value {
		if !('0' <= character && character <= '9' || 'a' <= character && character <= 'f') {
			return false
		}
	}
	return true
}
