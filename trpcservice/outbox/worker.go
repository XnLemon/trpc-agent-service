// Package outbox delivers durable replies with lease fencing and reconciliation.
package outbox

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

var (
	// ErrInvalid reports an invalid worker configuration or request.
	ErrInvalid = errors.New("invalid outbox worker")
	// ErrProvider reports a provider delivery failure.
	ErrProvider = errors.New("provider delivery failed")
	// ErrAlreadyRunning reports an attempt to start a second worker loop.
	ErrAlreadyRunning = errors.New("outbox worker is already running")
)

// DeliveryStatus describes the provider's reconciliation result.
type DeliveryStatus string

const (
	// DeliveryAccepted confirms that the provider accepted the reply.
	DeliveryAccepted DeliveryStatus = "accepted"
	// DeliveryRejected confirms that the provider rejected the reply.
	DeliveryRejected DeliveryStatus = "rejected"
	// DeliveryUnknown means the provider could not confirm delivery.
	DeliveryUnknown DeliveryStatus = "unknown"
)

// Provider is intentionally protocol-neutral. Implementations must use the
// stable ReplyID/SegmentIndex as their external idempotency key.
type Provider interface {
	Deliver(context.Context, runtimestorage.ReplyOutbox) (providerMessageID string, err error)
	Reconcile(context.Context, runtimestorage.ReplyOutbox) (DeliveryStatus, string, error)
}

// DeliveryError classifies a provider delivery failure for retry decisions.
type DeliveryError struct {
	Class     string
	Retryable bool
}

func (e *DeliveryError) Error() string { return ErrProvider.Error() }

// Worker delivers durable reply segments with lease fencing.
type Worker struct {
	store         runtimestorage.ReplyStore
	messageStore  runtimestorage.MessageStore
	provider      Provider
	channel       string
	providerName  string
	tenantID      string
	owner         string
	leaseDuration time.Duration
	retry         retryPolicy
	telemetry     observability.Provider
	metrics       metrics.Catalog
	audit         audit.Recorder
	mu            sync.Mutex
	runCancel     context.CancelFunc
	runDone       chan struct{}
}

type precedingSegmentState uint8

const (
	precedingSegmentsSent precedingSegmentState = iota
	precedingSegmentsPending
	precedingSegmentsDeadLettered
)

// Config controls a durable reply worker.
type Config struct {
	// Store is the durable reply-segment capability used for claiming and
	// transitioning deliveries.
	Store runtimestorage.ReplyStore
	// MessageStore is the inbound message lifecycle capability used to advance
	// an event after all of its reply segments are delivered.
	MessageStore runtimestorage.MessageStore
	Provider     Provider
	// Channel and ProviderName identify the real delivery route for telemetry.
	// Empty values retain the legacy outbox/other defaults.
	Channel       string
	ProviderName  string
	TenantID      string
	Owner         string
	LeaseDuration time.Duration
	MaxAttempts   int
	BackoffBase   time.Duration
	BackoffMax    time.Duration
	Jitter        float64
	Observability observability.Provider
	// AuditWriter receives durable delivery, retry, and dead-letter facts.
	AuditWriter audit.Writer
}

// New creates a reply worker after validating delivery and lease settings.
func New(config Config) (*Worker, error) {
	if nilvalue.Is(config.Store) || nilvalue.Is(config.MessageStore) || nilvalue.Is(config.Provider) || runtimestorage.ValidateTenant(config.TenantID) != nil || !validWorkerText(config.Owner, 256, true) || config.LeaseDuration <= 0 {
		return nil, ErrInvalid
	}
	if config.Channel == "" {
		config.Channel = "outbox"
	}
	if config.ProviderName == "" {
		config.ProviderName = "other"
	}
	if !validWorkerText(config.Channel, 128, true) || !validWorkerText(config.ProviderName, 128, true) {
		return nil, ErrInvalid
	}
	retry, err := newRetryPolicy(config)
	if err != nil {
		return nil, err
	}
	config.Observability = observability.ProtectProvider(config.Observability)
	return &Worker{
		store: config.Store, messageStore: config.MessageStore, provider: config.Provider,
		channel: config.Channel, providerName: config.ProviderName,
		tenantID: config.TenantID, owner: config.Owner, leaseDuration: config.LeaseDuration,
		retry: retry, telemetry: config.Observability, metrics: metrics.New(config.Observability),
		audit: audit.NewRecorder(config.AuditWriter, config.TenantID),
	}, nil
}

// Run polls until ctx is canceled. It owns no goroutine after returning.
func (w *Worker) Run(ctx context.Context, pollInterval time.Duration) error {
	runCtx, err := w.beginRun(ctx)
	if err != nil {
		return err
	}
	return w.runLoop(runCtx, pollInterval)
}

// Start reserves the worker lifecycle before launching its polling goroutine.
// It is intended for process owners that must ensure Close can join it.
func (w *Worker) Start(ctx context.Context, pollInterval time.Duration) error {
	runCtx, err := w.beginRun(ctx)
	if err != nil {
		return err
	}
	go func() {
		if err := w.runLoop(runCtx, pollInterval); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			logWorkerStopped(w, err)
		}
	}()
	return nil
}

func (w *Worker) beginRun(ctx context.Context) (context.Context, error) {
	if w == nil || nilvalue.Is(ctx) {
		return nil, ErrInvalid
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.runCancel != nil {
		return nil, ErrAlreadyRunning
	}
	runCtx, cancel, err := withCancelSafely(ctx)
	if err != nil {
		return nil, err
	}
	w.runCancel = cancel
	w.runDone = make(chan struct{})
	return runCtx, nil
}

func (w *Worker) runLoop(runCtx context.Context, pollInterval time.Duration) error {
	if w == nil || nilvalue.Is(runCtx) {
		return ErrInvalid
	}
	if pollInterval <= 0 {
		pollInterval = time.Second
	}
	defer func() {
		w.mu.Lock()
		cancel := w.runCancel
		done := w.runDone
		w.runCancel = nil
		w.runDone = nil
		w.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if done != nil {
			close(done)
		}
	}()
	done, doneErr := nilvalue.ContextDone(runCtx)
	if doneErr != nil {
		return ErrInvalid
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if _, err := w.RunOnce(runCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		select {
		case <-done:
			return nilvalue.ContextErr(runCtx)
		case <-ticker.C:
		}
	}
}

// Close cancels a running poll loop and waits for it to release its lease.
func (w *Worker) Close() error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	cancel, done := w.runCancel, w.runDone
	w.mu.Unlock()
	if cancel != nil && done != nil {
		cancel()
		<-done
	} else if cancel != nil {
		cancel()
	}
	return nil
}

// RunOnce claims and processes every currently eligible reply. Conflicts are
// expected under competing workers and are skipped; provider errors are stored
// only as stable classes, never as raw error text.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	if w == nil || nilvalue.Is(ctx) {
		return 0, ErrInvalid
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return 0, err
	}
	candidates, err := observeStorage(w, ctx, func(operationCtx context.Context) ([]runtimestorage.ReplyOutbox, error) {
		return w.store.ListReplyCandidates(operationCtx, w.tenantID)
	})
	if err != nil {
		return 0, err
	}
	// Storage adapters need not provide a candidate order, but stream segments do.
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].ReplyID == candidates[j].ReplyID {
			return candidates[i].SegmentIndex < candidates[j].SegmentIndex
		}
		return candidates[i].ReplyID < candidates[j].ReplyID
	})
	if !validReplyCandidateBatch(w, candidates) {
		return 0, ErrInvalid
	}
	processed := 0
	for _, candidate := range candidates {
		// Candidate stores are durable trust boundaries. Never let a malformed
		// or cross-tenant snapshot reach ClaimReply or a provider, even when a
		// custom adapter returns rows outside its documented query scope.
		if !validReplyCandidate(w, candidate) {
			return processed, ErrInvalid
		}
		// A later segment must not overtake a retrying or leased predecessor.
		state, readyErr := w.precedingSegmentsState(ctx, candidate)
		if readyErr != nil {
			return processed, readyErr
		}
		if state == precedingSegmentsPending {
			continue
		}
		claimed, claimedOK, claimErr := w.claimCandidate(ctx, candidate)
		if claimErr != nil {
			return processed, claimErr
		}
		if !claimedOK {
			continue
		}
		if !validClaimedReply(w, candidate, claimed) {
			return processed, ErrInvalid
		}
		processed++
		if err := w.processClaimed(ctx, candidate, claimed, state == precedingSegmentsDeadLettered); err != nil && !errors.Is(err, runtimestorage.ErrConflict) {
			return processed, err
		}
	}
	return processed, nil
}

func (w *Worker) precedingSegmentsState(ctx context.Context, candidate runtimestorage.ReplyOutbox) (precedingSegmentState, error) {
	for index := 0; index < candidate.SegmentIndex; index++ {
		previous, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyOutbox, error) {
			return w.store.GetReply(operationCtx, candidate.TenantID, candidate.ReplyID, index)
		})
		if errors.Is(err, runtimestorage.ErrNotFound) {
			return precedingSegmentsPending, nil
		}
		if err != nil {
			return precedingSegmentsPending, err
		}
		if !validReplyCandidate(w, previous) || previous.TenantID != candidate.TenantID || previous.ReplyID != candidate.ReplyID || previous.EventID != candidate.EventID || previous.SegmentIndex != index {
			return precedingSegmentsPending, ErrInvalid
		}
		switch previous.Status {
		case runtimestorage.ReplySent:
		case runtimestorage.ReplyDeadLetter:
			return precedingSegmentsDeadLettered, nil
		default:
			return precedingSegmentsPending, nil
		}
	}
	return precedingSegmentsSent, nil
}

func (w *Worker) claimCandidate(ctx context.Context, candidate runtimestorage.ReplyOutbox) (runtimestorage.ReplyOutbox, bool, error) {
	if !eligible(candidate) || !w.retryDue(candidate, time.Now().UTC()) {
		return runtimestorage.ReplyOutbox{}, false, nil
	}
	claimed, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyOutbox, error) {
		return w.store.ClaimReply(operationCtx, candidate.TenantID, candidate.ReplyID, candidate.SegmentIndex, w.owner, w.leaseDuration)
	})
	if errors.Is(err, runtimestorage.ErrConflict) || errors.Is(err, runtimestorage.ErrNotFound) {
		return runtimestorage.ReplyOutbox{}, false, nil
	}
	if err != nil {
		return runtimestorage.ReplyOutbox{}, false, err
	}
	return claimed, true, nil
}

func (w *Worker) processClaimed(ctx context.Context, candidate, claimed runtimestorage.ReplyOutbox, predecessorDeadLettered bool) (err error) {
	ctx = restoreCorrelationContext(ctx, w.store, claimed)
	started := time.Now()
	operationCtx, _, finishOperation := observability.StartOperation(ctx, w.telemetry, observability.OperationChannelSend, "channel")
	labels := map[string]string{"component": "channel", "operation": observability.OperationChannelSend, "channel": w.channel, "provider": w.providerName}
	_ = w.metrics.Request(operationCtx, map[string]string{"component": "channel", "operation": observability.OperationChannelSend, "channel": w.channel, "provider": w.providerName, "status": "started"})
	var operationErr error
	defer func() {
		if err != nil {
			operationErr = err
		}
		finishOperation(operationErr)
		_ = w.metrics.Operation(operationCtx, started, labels, operationErr)
	}()
	if predecessorDeadLettered {
		deliveryErr := &DeliveryError{Class: "preceding_segment_dead_lettered", Retryable: false}
		operationErr = deliveryErr
		return w.rejectDelivery(ctx, operationCtx, claimed, deliveryErr)
	}
	if candidate.Status == runtimestorage.ReplySending {
		// A sending lease means the previous worker may have reached the
		// provider before losing its lease. Reconcile is the only safe
		// resolution path; an unknown/error result must not redeliver.
		status, reconciled := w.reconcileDelivery(operationCtx, claimed)
		if reconciled && status == DeliveryAccepted {
			w.advanceEvent(ctx, claimed.EventID)
			_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "success", "error_class": ""})
		} else if reconciled && status == DeliveryRejected {
			_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "retry", "error_class": "provider_rejected"})
		} else {
			operationErr = ErrProvider
			_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "retry", "error_class": "error"})
		}
		return nil
	}
	providerID, deliveryErr := deliverSafely(w.provider, operationCtx, claimed)
	operationErr = deliveryErr
	if deliveryErr == nil {
		return w.acceptDelivery(ctx, operationCtx, claimed, providerID)
	}
	return w.rejectDelivery(ctx, operationCtx, claimed, deliveryErr)
}

func restoreCorrelationContext(ctx context.Context, store runtimestorage.ReplyStore, value runtimestorage.ReplyOutbox) context.Context {
	ctx = observability.ContextWithoutTraceParent(ctx)
	if nilvalue.Is(store) {
		return ctx
	}
	correlations, ok := store.(runtimestorage.ReplyCorrelationStore)
	if !ok || nilvalue.Is(correlations) {
		return ctx
	}
	correlation, err := getReplyCorrelationSafely(correlations, ctx, value.TenantID, value.EventID)
	if err != nil {
		return ctx
	}
	if correlation.TraceParent == "" {
		return observability.WithCorrelation(ctx, correlation.RequestID, correlation.TraceID)
	}
	ctx = observability.ContextWithTraceParent(ctx, correlation.TraceParent)
	return observability.WithCorrelation(ctx, correlation.RequestID, correlation.TraceID)
}

func getReplyCorrelationSafely(store runtimestorage.ReplyCorrelationStore, ctx context.Context, tenantID, eventID string) (correlation runtimestorage.ReplyCorrelation, err error) {
	if nilvalue.Is(store) || nilvalue.Is(ctx) {
		return runtimestorage.ReplyCorrelation{}, ErrProvider
	}
	defer func() {
		if recover() != nil {
			correlation = runtimestorage.ReplyCorrelation{}
			err = ErrProvider
		}
	}()
	return store.GetReplyCorrelation(ctx, tenantID, eventID)
}

func (w *Worker) acceptDelivery(ctx, operationCtx context.Context, claimed runtimestorage.ReplyOutbox, providerID string) error {
	if !validProviderMessageID(providerID) {
		// A nil provider error without a durable receipt is an uncertain
		// hand-off. Leave the row sending so reconciliation can resolve it;
		// never convert it into a fresh delivery attempt here.
		return ErrProvider
	}
	// Record the durable audit fact before committing the terminal row. If
	// audit is unavailable, leave the row sending so recovery can reconcile
	// without replaying the provider side effect and retry the audit fact.
	if err := w.recordDelivery(operationCtx, audit.EventIMDeliverySent, claimed, ""); err != nil {
		return err
	}
	_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "success", "error_class": ""})
	_, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyOutbox, error) {
		return w.store.TransitionReply(operationCtx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: w.owner, FencingToken: claimed.FencingToken, ProviderID: providerID})
	})
	if err == nil {
		w.advanceEvent(ctx, claimed.EventID)
	}
	return err
}

func (w *Worker) rejectDelivery(ctx, operationCtx context.Context, claimed runtimestorage.ReplyOutbox, deliveryErr error) error {
	class, retryable := classify(deliveryErr)
	to := runtimestorage.ReplyRetryable
	if !retryable || claimed.Attempts >= w.retry.maxAttempts {
		to = runtimestorage.ReplyDeadLetter
	}
	eventType := audit.EventIMDeliveryRetryScheduled
	if to == runtimestorage.ReplyDeadLetter {
		eventType = audit.EventIMDeliveryDeadLettered
	}
	// As with acceptance, audit first. A failed audit leaves the lease in an
	// uncertain state and is intentionally reconciled rather than silently
	// committing a lifecycle decision without its durable fact.
	if err := w.recordDelivery(operationCtx, eventType, claimed, class); err != nil {
		return err
	}
	_, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyOutbox, error) {
		return w.store.TransitionReply(operationCtx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: to, Owner: w.owner, FencingToken: claimed.FencingToken, ErrorClass: class})
	})
	if retryable && to == runtimestorage.ReplyRetryable {
		_ = w.metrics.Retry(operationCtx, map[string]string{"component": "channel", "operation": observability.OperationChannelSend, "channel": w.channel, "provider": w.providerName, "status": "retry", "error_class": metricErrorClass(class)})
	}
	if to == runtimestorage.ReplyDeadLetter {
		_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "dead_letter", "error_class": metricErrorClass(class)})
	} else if to == runtimestorage.ReplyRetryable {
		_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "retry", "error_class": metricErrorClass(class)})
	}
	return err
}

func (w *Worker) recordDelivery(ctx context.Context, eventType audit.EventType, value runtimestorage.ReplyOutbox, class string) (err error) {
	if w == nil || nilvalue.Is(ctx) {
		return audit.ErrWriteFailed
	}
	defer func() {
		if recover() != nil {
			err = audit.ErrWriteFailed
		}
	}()
	decision := audit.DecisionAccepted
	if eventType != audit.EventIMDeliverySent {
		decision = audit.DecisionRejected
	}
	requestID, traceID := value.ReplyID, ""
	if correlations, ok := w.store.(runtimestorage.ReplyCorrelationStore); ok && !nilvalue.Is(correlations) {
		if correlation, correlationErr := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyCorrelation, error) {
			return getReplyCorrelationSafely(correlations, operationCtx, value.TenantID, value.EventID)
		}); correlationErr == nil {
			requestID, traceID = correlation.RequestID, correlation.TraceID
		}
	}
	return w.audit.Record(ctx, audit.Event{
		EventID:   audit.NewEventID(string(eventType), requestID, traceID, value.ReplyID, strconv.Itoa(value.SegmentIndex)),
		EventType: eventType, RequestID: requestID, TraceID: traceID,
		Decision: decision, ErrorType: class,
	})
}

func (w *Worker) retryDue(value runtimestorage.ReplyOutbox, now time.Time) bool {
	if value.Status != runtimestorage.ReplyRetryable || w.retry.base <= 0 || value.UpdatedAt.IsZero() {
		return true
	}
	return !now.Before(value.UpdatedAt.Add(w.retry.delay(value.ReplyID, value.Attempts)))
}

func eligible(value runtimestorage.ReplyOutbox) bool {
	if value.Status == runtimestorage.ReplyPending || value.Status == runtimestorage.ReplyRetryable {
		return true
	}
	return value.Status == runtimestorage.ReplySending && value.LeaseExpiresAt != nil && !value.LeaseExpiresAt.After(time.Now().UTC())
}

func validReplyCandidate(worker *Worker, value runtimestorage.ReplyOutbox) bool {
	if worker == nil || runtimestorage.ValidateTenant(value.TenantID) != nil || value.TenantID != worker.tenantID ||
		!validOutboxIdentity(value.ReplyID) || !validOutboxIdentity(value.EventID) || value.SegmentIndex < 0 || value.SegmentCount < 0 || value.SegmentCount == 0 && value.SegmentIndex != 0 || value.SegmentCount > maxWorkerSegments || value.Attempts < 0 || value.FencingToken < 0 {
		return false
	}
	if value.SegmentCount > 0 && value.SegmentIndex >= value.SegmentCount {
		return false
	}
	if !validOutboxPayload(value.Payload) || runtimestorage.ValidateReplyTarget(value.ReplyTarget) != nil {
		return false
	}
	normalized, err := runtimestorage.NormalizeReplyOutbox(value)
	if err != nil || value.Kind != "" && normalized.Kind != value.Kind || normalized.Payload != value.Payload || normalized.Fallback != value.Fallback || normalized.Attachment != value.Attachment {
		return false
	}
	if value.ProviderMessageID != "" && !validProviderMessageID(value.ProviderMessageID) {
		return false
	}
	if value.Status == runtimestorage.ReplySent && value.SegmentCount > 0 && !validProviderMessageID(value.ProviderMessageID) {
		return false
	}
	if value.LeaseOwner != "" && !validOutboxIdentity(value.LeaseOwner) {
		return false
	}
	if value.CreatedAt.IsZero() != value.UpdatedAt.IsZero() {
		return false
	}
	if !value.CreatedAt.IsZero() && (value.CreatedAt.Location() != time.UTC || value.UpdatedAt.Location() != time.UTC || value.UpdatedAt.Before(value.CreatedAt)) {
		return false
	}
	if value.LeaseExpiresAt != nil && value.LeaseExpiresAt.IsZero() {
		return false
	}
	if !validOutboxErrorClass(value.LastErrorClass) {
		return false
	}
	switch value.Status {
	case runtimestorage.ReplyPending:
		return value.LeaseOwner == "" && value.LeaseExpiresAt == nil && value.ProviderMessageID == ""
	case runtimestorage.ReplyRetryable:
		// Some durable stores retain the historical owner as an audit field;
		// the absence of an expiry is what makes the row unleased.
		return value.LeaseExpiresAt == nil && value.ProviderMessageID == ""
	case runtimestorage.ReplySending:
		// SegmentCount==0 is the legacy shape used by pre-segmentation rows.
		// New segmented rows must carry the complete lease envelope; otherwise
		// a worker cannot prove that its delivery attempt is fenced.
		if value.SegmentCount > 0 {
			return value.LeaseOwner != "" && value.LeaseExpiresAt != nil
		}
		return true
	case runtimestorage.ReplySent, runtimestorage.ReplyDeadLetter:
		return value.LeaseExpiresAt == nil
	default:
		return false
	}
}

const maxWorkerSegments = 1 << 20

func validOutboxPayload(value string) bool {
	return len(value) <= 4<<20 && runtimestorage.ValidateText(value, 4<<20, false)
}

func validOutboxErrorClass(value string) bool {
	switch value {
	case "", "rate_limited", "timeout", "canceled", "invalid", "unauthenticated", "not_ready", "unavailable", "provider_rejected", "provider_error", "permanent", "preceding_segment_dead_lettered":
		return true
	default:
		return false
	}
}

func validReplyCandidateBatch(worker *Worker, values []runtimestorage.ReplyOutbox) bool {
	// Candidate stores are untrusted, so validate every row before any claim.
	// A count of one is the legacy shape emitted for the first row while a
	// store is upgraded to segmented delivery; other conflicting counts are
	// not compatible with one reply identity.
	type replyGroup struct {
		eventID      string
		target       runtimestorage.ReplyTarget
		segmentCount int
		segments     map[int]struct{}
	}
	groups := make(map[string]replyGroup)
	for _, value := range values {
		if !validReplyCandidate(worker, value) {
			return false
		}
		group, exists := groups[value.ReplyID]
		if !exists {
			group = replyGroup{eventID: value.EventID, target: value.ReplyTarget, segmentCount: value.SegmentCount, segments: make(map[int]struct{})}
		} else if group.eventID != value.EventID || group.target != value.ReplyTarget || group.segmentCount != value.SegmentCount && group.segmentCount != 1 && value.SegmentCount != 1 {
			return false
		}
		if _, duplicate := group.segments[value.SegmentIndex]; duplicate {
			return false
		}
		group.segments[value.SegmentIndex] = struct{}{}
		if group.segmentCount == 1 && value.SegmentCount > 1 {
			group.segmentCount = value.SegmentCount
		}
		groups[value.ReplyID] = group
	}
	for _, group := range groups {
		// Zero-count rows are the pre-segmentation compatibility shape. A
		// count of one is also accepted while stores upgrade their first row.
		// Once a reply advertises multiple segments, the worker must observe
		// the complete contiguous set before it can deliver or advance the
		// inbound event; otherwise a truncated candidate list could mark an
		// event replied while a segment is missing.
		if group.segmentCount <= 1 {
			continue
		}
		if len(group.segments) != group.segmentCount {
			return false
		}
		for index := 0; index < group.segmentCount; index++ {
			if _, present := group.segments[index]; !present {
				return false
			}
		}
	}
	return true
}

func validClaimedReply(worker *Worker, candidate, claimed runtimestorage.ReplyOutbox) bool {
	if !validReplyCandidate(worker, claimed) || claimed.TenantID != candidate.TenantID || claimed.ReplyID != candidate.ReplyID || claimed.EventID != candidate.EventID || claimed.SegmentIndex != candidate.SegmentIndex || claimed.Status != runtimestorage.ReplySending {
		return false
	}
	candidateKind := candidate.Kind
	if candidateKind == "" {
		candidateKind = runtimestorage.ReplyKindText
	}
	claimedKind := claimed.Kind
	if claimedKind == "" {
		claimedKind = runtimestorage.ReplyKindText
	}
	if claimed.SegmentCount != candidate.SegmentCount || claimedKind != candidateKind || claimed.Payload != candidate.Payload || claimed.Attachment != candidate.Attachment || claimed.Fallback != candidate.Fallback || claimed.ReplyTarget != candidate.ReplyTarget {
		return false
	}
	// Legacy test/compatibility stores did not return lease metadata for their
	// zero-count rows. Every real segmented row must prove the incremented
	// attempt and fencing token and must be leased to this worker.
	if candidate.SegmentCount > 0 {
		if candidate.Attempts >= int(^uint(0)>>1) || candidate.FencingToken >= int64(^uint64(0)>>1) || claimed.Attempts != candidate.Attempts+1 || claimed.FencingToken != candidate.FencingToken+1 || claimed.LeaseOwner != worker.owner || claimed.LeaseExpiresAt == nil || !claimed.LeaseExpiresAt.After(time.Now().UTC()) {
			return false
		}
	}
	return true
}

func validWorkerText(value string, max int, required bool) bool {
	return value == strings.TrimSpace(value) && !strings.Contains(value, "://") && runtimestorage.ValidateText(value, max, required)
}

func validOutboxIdentity(value string) bool {
	return validWorkerText(value, 256, true)
}

func validProviderMessageID(value string) bool {
	return validWorkerText(value, 1024, true)
}

func (w *Worker) advanceEvent(ctx context.Context, eventID string) {
	if w == nil || nilvalue.Is(w.messageStore) || eventID == "" {
		return
	}
	candidates, err := observeStorage(w, ctx, func(operationCtx context.Context) ([]runtimestorage.ReplyOutbox, error) {
		return w.store.ListReplyCandidates(operationCtx, w.tenantID)
	})
	if err != nil || !validReplyCandidateBatch(w, candidates) {
		return
	}
	hasEvent := false
	for _, value := range candidates {
		if value.EventID != eventID {
			continue
		}
		if !validReplyCandidate(w, value) {
			return
		}
		hasEvent = true
		if value.Status != runtimestorage.ReplySent {
			return
		}
	}
	if !hasEvent {
		return
	}
	event, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.MessageEvent, error) {
		return w.messageStore.GetMessage(operationCtx, w.tenantID, eventID)
	})
	if err != nil {
		return
	}
	// Legacy adapters may omit identity fields, but a populated identity must
	// still agree with the worker's tenant/event lookup. Never use a fetched
	// cross-tenant snapshot to decide a lifecycle transition.
	if event.TenantID != "" && event.TenantID != w.tenantID || event.EventID != "" && event.EventID != eventID {
		return
	}
	if event.Status == runtimestorage.EventCompleted {
		if _, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.MessageEvent, error) {
			return w.messageStore.TransitionMessage(operationCtx, runtimestorage.MessageTransition{TenantID: w.tenantID, EventID: eventID, From: runtimestorage.EventCompleted, To: runtimestorage.EventReplyPending, Owner: w.owner})
		}); err != nil {
			return
		}
		event.Status = runtimestorage.EventReplyPending
	}
	if event.Status == runtimestorage.EventReplyPending {
		_, _ = observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.MessageEvent, error) {
			return w.messageStore.TransitionMessage(operationCtx, runtimestorage.MessageTransition{TenantID: w.tenantID, EventID: eventID, From: runtimestorage.EventReplyPending, To: runtimestorage.EventReplied, Owner: w.owner})
		})
	}
}

func deliverSafely(provider Provider, ctx context.Context, value runtimestorage.ReplyOutbox) (providerID string, err error) {
	if nilvalue.Is(provider) || nilvalue.Is(ctx) {
		return "", &DeliveryError{Class: "provider_error", Retryable: true}
	}
	defer func() {
		if recover() != nil {
			providerID = ""
			err = &DeliveryError{Class: "provider_error", Retryable: true}
		} else if nilvalue.Is(err) {
			err = nil
		}
	}()
	return provider.Deliver(ctx, value)
}

func reconcileSafely(provider Provider, ctx context.Context, value runtimestorage.ReplyOutbox) (status DeliveryStatus, providerID string, err error) {
	if nilvalue.Is(provider) || nilvalue.Is(ctx) {
		return DeliveryUnknown, "", ErrProvider
	}
	defer func() {
		if recover() != nil {
			status, providerID, err = DeliveryUnknown, "", ErrProvider
		} else if nilvalue.Is(err) {
			err = nil
		}
	}()
	return provider.Reconcile(ctx, value)
}

func (w *Worker) reconcile(ctx context.Context, claimed runtimestorage.ReplyOutbox) bool {
	_, ok := w.reconcileDelivery(ctx, claimed)
	return ok
}

func (w *Worker) reconcileDelivery(ctx context.Context, claimed runtimestorage.ReplyOutbox) (DeliveryStatus, bool) {
	if w == nil || nilvalue.Is(ctx) || nilvalue.Is(w.provider) {
		return DeliveryUnknown, false
	}
	status, providerID, err := reconcileSafely(w.provider, ctx, claimed)
	if err != nil || (status != DeliveryAccepted && status != DeliveryRejected) || (status == DeliveryAccepted && !validProviderMessageID(providerID)) || (status == DeliveryRejected && providerID != "" && !validProviderMessageID(providerID)) {
		return DeliveryUnknown, false
	}
	to := runtimestorage.ReplySent
	class := ""
	eventType := audit.EventIMDeliverySent
	if status == DeliveryRejected {
		to = runtimestorage.ReplyRetryable
		class = "provider_rejected"
		eventType = audit.EventIMDeliveryRetryScheduled
		if w.retry.maxAttempts > 0 && claimed.Attempts >= w.retry.maxAttempts {
			to = runtimestorage.ReplyDeadLetter
			eventType = audit.EventIMDeliveryDeadLettered
		}
	}
	// The row remains sending until the audit fact is present. This makes an
	// audit outage recoverable while preserving the no-replay guarantee.
	if err := w.recordDelivery(ctx, eventType, claimed, class); err != nil {
		return DeliveryUnknown, false
	}
	_, transitionErr := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyOutbox, error) {
		return w.store.TransitionReply(operationCtx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: to, Owner: w.owner, FencingToken: claimed.FencingToken, ProviderID: providerID, ErrorClass: class})
	})
	return status, transitionErr == nil
}

func withCancelSafely(ctx context.Context) (runCtx context.Context, cancel context.CancelFunc, err error) {
	if nilvalue.Is(ctx) {
		return nil, func() {}, ErrInvalid
	}
	defer func() {
		if recover() != nil {
			runCtx = nil
			cancel = func() {}
			err = ErrInvalid
		}
	}()
	runCtx, cancel = context.WithCancel(ctx)
	return runCtx, cancel, nil
}

func observeStorage[T any](worker *Worker, ctx context.Context, operation func(context.Context) (T, error)) (value T, err error) {
	var zero T
	if worker == nil || nilvalue.Is(ctx) || operation == nil || nilvalue.Is(worker.store) {
		return zero, ErrInvalid
	}
	defer func() {
		if recover() != nil {
			value = zero
			err = ErrProvider
		}
	}()
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, worker.telemetry, observability.OperationStorageOperation, "storage")
	provider := worker.providerName
	_ = worker.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": provider, "status": "started"})
	value, err = operation(operationCtx)
	if nilvalue.Is(err) {
		err = nil
	}
	finish(err)
	_ = worker.metrics.Operation(operationCtx, started, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": provider}, err)
	status := "success"
	if err != nil {
		status = observability.ErrorClass(err)
		if status == "" {
			status = "error"
		}
	}
	_ = worker.metrics.BackendDuration(operationCtx, observability.DurationMilliseconds(started), map[string]string{"component": "storage", "provider": provider, "status": status, "error_class": observability.ErrorClass(err)})
	return value, err
}

func classify(err error) (string, bool) {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", true
	}
	if errors.Is(err, context.Canceled) {
		return "canceled", true
	}
	var deliveryErr *DeliveryError
	if errors.As(err, &deliveryErr) && deliveryErr.Class != "" {
		class := normalizeErrorClass(deliveryErr.Class)
		return class, deliveryErr.Retryable
	}
	return "provider_error", true
}

func normalizeErrorClass(class string) string {
	switch class {
	case "rate_limited", "timeout", "canceled", "invalid", "unauthenticated", "not_ready", "unavailable", "provider_rejected", "provider_error":
		return class
	default:
		return "provider_error"
	}
}

func metricErrorClass(class string) string {
	switch normalizeErrorClass(class) {
	case "rate_limited", "timeout", "canceled", "invalid", "unauthenticated", "not_ready", "unavailable":
		return normalizeErrorClass(class)
	default:
		return "error"
	}
}
