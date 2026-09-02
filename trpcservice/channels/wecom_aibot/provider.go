package wecom_aibot

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

// Provider delivers durable final replies through the manager's current
// connection. Partial stream events are intentionally excluded from this
// provider; only the durable final segment is retried after reconnect.
type Provider struct {
	manager      *Manager
	correlations storage.ReplyCorrelationStore
	mu           sync.Mutex
	receipts     map[string]string
}

var _ outbox.Provider = (*Provider)(nil)

// NewProvider creates the durable final-reply adapter for one manager.
func NewProvider(manager *Manager, correlations storage.ReplyCorrelationStore) (*Provider, error) {
	if manager == nil || correlations == nil {
		return nil, ErrInvalid
	}
	return &Provider{manager: manager, correlations: correlations, receipts: make(map[string]string)}, nil
}

// Deliver sends and acknowledges a durable final reply for its correlated request.
func (p *Provider) Deliver(ctx context.Context, value storage.ReplyOutbox) (string, error) {
	if p == nil || p.manager == nil || p.correlations == nil || ctx == nil || strings.TrimSpace(value.Payload) == "" || value.ReplyID == "" {
		return "", &outbox.DeliveryError{Class: "invalid", Retryable: false}
	}
	key := value.TenantID + "\x00" + value.ReplyID + "\x00" + strconv.Itoa(value.SegmentIndex)
	p.mu.Lock()
	if receipt := p.receipts[key]; receipt != "" {
		p.mu.Unlock()
		return receipt, nil
	}
	p.mu.Unlock()
	if !p.manager.Ready() {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	correlation, err := p.correlations.GetReplyCorrelation(ctx, value.TenantID, value.EventID)
	if err != nil || strings.TrimSpace(correlation.RequestID) == "" {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	reqID := correlation.RequestID
	body := StreamReply{MsgType: "stream", Stream: struct {
		ID      string `json:"id"`
		Finish  bool   `json:"finish"`
		Content string `json:"content,omitempty"`
	}{ID: reqID, Finish: value.SegmentIndex+1 == value.SegmentCount, Content: value.Payload}}
	if err := p.manager.sendReply(ctx, reqID, body); err != nil {
		return "", &outbox.DeliveryError{Class: "unavailable", Retryable: true}
	}
	receipt := reqID + ":" + strconv.Itoa(value.SegmentIndex)
	p.mu.Lock()
	p.receipts[key] = receipt
	p.mu.Unlock()
	return receipt, nil
}

// Reconcile reports locally acknowledged replies as accepted.
func (p *Provider) Reconcile(_ context.Context, value storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	if p == nil {
		return outbox.DeliveryUnknown, "", nil
	}
	key := value.TenantID + "\x00" + value.ReplyID + "\x00" + strconv.Itoa(value.SegmentIndex)
	p.mu.Lock()
	defer p.mu.Unlock()
	if receipt := p.receipts[key]; receipt != "" {
		return outbox.DeliveryAccepted, receipt, nil
	}
	return outbox.DeliveryUnknown, "", nil
}
