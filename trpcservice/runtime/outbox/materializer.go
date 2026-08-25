package outbox

import (
	"context"
	"errors"
	"strings"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// ErrMaterialization wraps failures while creating durable reply segments.
var ErrMaterialization = errors.New("reply materialization failed")

const defaultSegmentRunes = 4096

// Materializer turns one completed Runner reply into durable, idempotent
// segments. It is deliberately independent of any channel SDK.
type Materializer struct {
	store       runtimestorage.RuntimeStore
	segmentSize int
}

// MaterializerConfig controls durable reply segmentation.
type MaterializerConfig struct {
	Store       runtimestorage.RuntimeStore
	SegmentSize int
}

// MaterializeInput identifies the completed reply to segment.
type MaterializeInput struct {
	TenantID string
	EventID  string
	ReplyID  string
	Payload  string
}

// NewMaterializer creates a reply materializer with a default segment size.
func NewMaterializer(config MaterializerConfig) (*Materializer, error) {
	if config.Store == nil {
		return nil, ErrInvalid
	}
	if config.SegmentSize <= 0 {
		config.SegmentSize = defaultSegmentRunes
	}
	return &Materializer{store: config.Store, segmentSize: config.SegmentSize}, nil
}

// Materialize writes all segments under the stable reply identity. A repeated
// call is idempotent when the existing rows have the same event and payload.
func (m *Materializer) Materialize(ctx context.Context, input MaterializeInput) (int, error) {
	if m == nil || ctx == nil || runtimestorage.ValidateTenant(input.TenantID) != nil || input.EventID == "" || input.ReplyID == "" {
		return 0, ErrInvalid
	}
	segments := splitRunes(input.Payload, m.segmentSize)
	if len(segments) == 0 {
		return 0, ErrInvalid
	}
	batchStore, ok := m.store.(runtimestorage.ReplyBatchEnqueuer)
	if !ok {
		return 0, errors.Join(ErrMaterialization, runtimestorage.ErrInvalid)
	}
	replies := make([]runtimestorage.ReplyOutbox, 0, len(segments))
	for index, payload := range segments {
		replies = append(replies, runtimestorage.ReplyOutbox{TenantID: input.TenantID, ReplyID: input.ReplyID, EventID: input.EventID, SegmentIndex: index, SegmentCount: len(segments), Payload: payload})
	}
	if _, err := batchStore.EnqueueReplies(ctx, replies); err != nil {
		return 0, errors.Join(ErrMaterialization, err)
	}
	return len(segments), nil
}

func splitRunes(value string, size int) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	runes := []rune(value)
	result := make([]string, 0, (len(runes)+size-1)/size)
	for start := 0; start < len(runes); start += size {
		end := start + size
		if end > len(runes) {
			end = len(runes)
		}
		result = append(result, string(runes[start:end]))
	}
	return result
}
