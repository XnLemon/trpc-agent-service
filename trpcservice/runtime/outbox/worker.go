// Package outbox delivers durable replies with lease fencing and reconciliation.
package outbox

import (
	"context"
	"errors"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

var (
	ErrInvalid  = errors.New("invalid outbox worker")
	ErrProvider = errors.New("provider delivery failed")
)

type DeliveryStatus string

const (
	DeliveryAccepted DeliveryStatus = "accepted"
	DeliveryRejected DeliveryStatus = "rejected"
	DeliveryUnknown  DeliveryStatus = "unknown"
)

// Provider is intentionally protocol-neutral. Implementations must use the
// stable ReplyID/SegmentIndex as their external idempotency key.
type Provider interface {
	Deliver(context.Context, runtimestorage.ReplyOutbox) (providerMessageID string, err error)
	Reconcile(context.Context, runtimestorage.ReplyOutbox) (DeliveryStatus, string, error)
}

type DeliveryError struct {
	Class     string
	Retryable bool
}

func (e *DeliveryError) Error() string { return ErrProvider.Error() }

type Worker struct {
	store         runtimestorage.RuntimeStore
	provider      Provider
	tenantID      string
	owner         string
	leaseDuration time.Duration
	maxAttempts   int
}

type Config struct {
	Store         runtimestorage.RuntimeStore
	Provider      Provider
	TenantID      string
	Owner         string
	LeaseDuration time.Duration
	MaxAttempts   int
}

func New(config Config) (*Worker, error) {
	if config.Store == nil || config.Provider == nil || runtimestorage.ValidateTenant(config.TenantID) != nil || config.Owner == "" || config.LeaseDuration <= 0 {
		return nil, ErrInvalid
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	return &Worker{store: config.Store, provider: config.Provider, tenantID: config.TenantID, owner: config.Owner, leaseDuration: config.LeaseDuration, maxAttempts: config.MaxAttempts}, nil
}

// RunOnce claims and processes every currently eligible reply. Conflicts are
// expected under competing workers and are skipped; provider errors are stored
// only as stable classes, never as raw error text.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, ErrInvalid
	}
	candidates, err := w.store.ListReplyCandidates(ctx, w.tenantID)
	if err != nil {
		return 0, err
	}
	processed := 0
	for _, candidate := range candidates {
		if !eligible(candidate) {
			continue
		}
		claimed, claimErr := w.store.ClaimReply(ctx, candidate.TenantID, candidate.ReplyID, candidate.SegmentIndex, w.owner, w.leaseDuration)
		if errors.Is(claimErr, runtimestorage.ErrConflict) || errors.Is(claimErr, runtimestorage.ErrNotFound) {
			continue
		}
		if claimErr != nil {
			return processed, claimErr
		}
		processed++
		if candidate.Status == runtimestorage.ReplySending {
			// A sending lease means the previous worker may have reached the
			// provider before losing its lease. Reconcile is the only safe
			// resolution path; an unknown/error result must not redeliver.
			if w.reconcile(ctx, claimed) {
				w.advanceEvent(ctx, claimed.EventID)
			}
			continue
		}
		providerID, deliveryErr := w.provider.Deliver(ctx, claimed)
		if deliveryErr == nil {
			_, err = w.store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: w.owner, FencingToken: claimed.FencingToken, ProviderID: providerID})
			if err == nil {
				w.advanceEvent(ctx, claimed.EventID)
			}
		} else {
			class, retryable := classify(deliveryErr)
			to := runtimestorage.ReplyRetryable
			if !retryable || claimed.Attempts >= w.maxAttempts {
				to = runtimestorage.ReplyDeadLetter
			}
			_, err = w.store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: to, Owner: w.owner, FencingToken: claimed.FencingToken, ErrorClass: class})
		}
		if err != nil && !errors.Is(err, runtimestorage.ErrConflict) {
			return processed, err
		}
	}
	return processed, nil
}

func eligible(value runtimestorage.ReplyOutbox) bool {
	if value.Status == runtimestorage.ReplyPending || value.Status == runtimestorage.ReplyRetryable {
		return true
	}
	return value.Status == runtimestorage.ReplySending && value.LeaseExpiresAt != nil && !value.LeaseExpiresAt.After(time.Now().UTC())
}

func (w *Worker) advanceEvent(ctx context.Context, eventID string) {
	if eventID == "" {
		return
	}
	candidates, err := w.store.ListReplyCandidates(ctx, w.tenantID)
	if err != nil {
		return
	}
	hasEvent := false
	for _, value := range candidates {
		if value.EventID != eventID {
			continue
		}
		hasEvent = true
		if value.Status != runtimestorage.ReplySent {
			return
		}
	}
	if !hasEvent {
		return
	}
	event, err := w.store.GetMessage(ctx, w.tenantID, eventID)
	if err != nil {
		return
	}
	if event.Status == runtimestorage.EventCompleted {
		if _, err := w.store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: w.tenantID, EventID: eventID, From: runtimestorage.EventCompleted, To: runtimestorage.EventReplyPending, Owner: w.owner}); err != nil {
			return
		}
		event.Status = runtimestorage.EventReplyPending
	}
	if event.Status == runtimestorage.EventReplyPending {
		_, _ = w.store.TransitionMessage(ctx, runtimestorage.MessageTransition{TenantID: w.tenantID, EventID: eventID, From: runtimestorage.EventReplyPending, To: runtimestorage.EventReplied, Owner: w.owner})
	}
}

func (w *Worker) reconcile(ctx context.Context, claimed runtimestorage.ReplyOutbox) bool {
	status, providerID, err := w.provider.Reconcile(ctx, claimed)
	if err != nil || status == DeliveryUnknown {
		return false
	}
	to := runtimestorage.ReplySent
	class := ""
	if status == DeliveryRejected {
		to = runtimestorage.ReplyRetryable
		class = "provider_rejected"
	}
	_, transitionErr := w.store.TransitionReply(ctx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: to, Owner: w.owner, FencingToken: claimed.FencingToken, ProviderID: providerID, ErrorClass: class})
	return transitionErr == nil
}

func classify(err error) (string, bool) {
	var deliveryErr *DeliveryError
	if errors.As(err, &deliveryErr) && deliveryErr.Class != "" {
		return deliveryErr.Class, deliveryErr.Retryable
	}
	return "provider_error", true
}
