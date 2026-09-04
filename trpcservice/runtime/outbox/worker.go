// Package outbox delivers durable replies with lease fencing and reconciliation.
package outbox

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/metrics"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
	"github.com/XnLemon/trpc-agent-service/trpcservice/resilience"
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
	store         runtimestorage.RuntimeStore
	provider      Provider
	channel       string
	providerName  string
	tenantID      string
	owner         string
	leaseDuration time.Duration
	maxAttempts   int
	backoffBase   time.Duration
	backoffMax    time.Duration
	jitter        float64
	telemetry     observability.Provider
	metrics       metrics.Catalog
	audit         audit.Recorder
	resilience    *resilience.Policy
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
	Store    runtimestorage.RuntimeStore
	Provider Provider
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
	// Resilience optionally bounds provider Deliver/Reconcile calls. The
	// provider contract must support the stable ReplyID/SegmentIndex idempotency
	// key before enabling retries around Deliver.
	Resilience *resilience.Policy
}

// New creates a reply worker after validating delivery and lease settings.
func New(config Config) (*Worker, error) {
	if config.Store == nil || config.Provider == nil || runtimestorage.ValidateTenant(config.TenantID) != nil || config.Owner == "" || config.LeaseDuration <= 0 || config.Resilience != nil && config.Resilience.Validate() != nil {
		return nil, ErrInvalid
	}
	if config.MaxAttempts <= 0 {
		config.MaxAttempts = 3
	}
	if config.BackoffBase < 0 || config.BackoffMax < 0 || config.Jitter < 0 || config.Jitter > 1 {
		return nil, ErrInvalid
	}
	if config.BackoffBase == 0 {
		config.BackoffBase = 100 * time.Millisecond
	}
	if config.BackoffMax == 0 {
		config.BackoffMax = 30 * time.Second
	}
	if config.BackoffMax < config.BackoffBase {
		return nil, ErrInvalid
	}
	if config.Observability == nil {
		config.Observability = observability.NewNoopProvider()
	}
	if config.Channel == "" {
		config.Channel = "outbox"
	}
	if config.ProviderName == "" {
		config.ProviderName = "other"
	}
	return &Worker{store: config.Store, provider: config.Provider, channel: config.Channel, providerName: config.ProviderName, tenantID: config.TenantID, owner: config.Owner, leaseDuration: config.LeaseDuration, maxAttempts: config.MaxAttempts, backoffBase: config.BackoffBase, backoffMax: config.BackoffMax, jitter: config.Jitter, telemetry: config.Observability, metrics: metrics.New(config.Observability), audit: audit.Recorder{Writer: config.AuditWriter, TenantID: config.TenantID}, resilience: config.Resilience}, nil
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
	go func() { _ = w.runLoop(runCtx, pollInterval) }()
	return nil
}

func (w *Worker) beginRun(ctx context.Context) (context.Context, error) {
	if w == nil || ctx == nil {
		return nil, ErrInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.runCancel != nil {
		return nil, ErrAlreadyRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.runCancel = cancel
	w.runDone = make(chan struct{})
	return runCtx, nil
}

func (w *Worker) runLoop(runCtx context.Context, pollInterval time.Duration) error {
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
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		if _, err := w.RunOnce(runCtx); err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		select {
		case <-runCtx.Done():
			return runCtx.Err()
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
	if cancel != nil {
		cancel()
		<-done
	}
	return nil
}

// RunOnce claims and processes every currently eligible reply. Conflicts are
// expected under competing workers and are skipped; provider errors are stored
// only as stable classes, never as raw error text.
func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	if ctx == nil {
		return 0, ErrInvalid
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
	processed := 0
	for _, candidate := range candidates {
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
		if w.reconcile(operationCtx, claimed) {
			w.advanceEvent(ctx, claimed.EventID)
			_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "success", "error_class": ""})
		} else {
			operationErr = ErrProvider
			_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "retry", "error_class": "error"})
		}
		return nil
	}
	providerID, deliveryErr := w.deliver(operationCtx, claimed)
	operationErr = deliveryErr
	if deliveryErr == nil {
		return w.acceptDelivery(ctx, operationCtx, claimed, providerID)
	}
	return w.rejectDelivery(ctx, operationCtx, claimed, deliveryErr)
}

func restoreCorrelationContext(ctx context.Context, store runtimestorage.RuntimeStore, value runtimestorage.ReplyOutbox) context.Context {
	ctx = observability.ContextWithoutTraceParent(ctx)
	correlations, ok := store.(runtimestorage.ReplyCorrelationStore)
	if !ok {
		return ctx
	}
	correlation, err := correlations.GetReplyCorrelation(ctx, value.TenantID, value.EventID)
	if err != nil {
		return ctx
	}
	if correlation.TraceParent == "" {
		return observability.WithCorrelation(ctx, correlation.RequestID, correlation.TraceID)
	}
	ctx = observability.ContextWithTraceParent(ctx, correlation.TraceParent)
	return observability.WithCorrelation(ctx, correlation.RequestID, correlation.TraceID)
}

func (w *Worker) acceptDelivery(ctx, operationCtx context.Context, claimed runtimestorage.ReplyOutbox, providerID string) error {
	_ = w.metrics.Delivery(operationCtx, map[string]string{"component": "channel", "channel": w.channel, "provider": w.providerName, "status": "success", "error_class": ""})
	_, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyOutbox, error) {
		return w.store.TransitionReply(operationCtx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: runtimestorage.ReplySent, Owner: w.owner, FencingToken: claimed.FencingToken, ProviderID: providerID})
	})
	if err == nil {
		err = w.recordDelivery(operationCtx, audit.EventIMDeliverySent, claimed, "")
	}
	if err == nil {
		w.advanceEvent(ctx, claimed.EventID)
	}
	return err
}

func (w *Worker) rejectDelivery(ctx, operationCtx context.Context, claimed runtimestorage.ReplyOutbox, deliveryErr error) error {
	class, retryable := classify(deliveryErr)
	to := runtimestorage.ReplyRetryable
	if !retryable || claimed.Attempts >= w.maxAttempts {
		to = runtimestorage.ReplyDeadLetter
	}
	_, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyOutbox, error) {
		return w.store.TransitionReply(operationCtx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: to, Owner: w.owner, FencingToken: claimed.FencingToken, ErrorClass: class})
	})
	if err == nil {
		eventType := audit.EventIMDeliveryRetryScheduled
		if to == runtimestorage.ReplyDeadLetter {
			eventType = audit.EventIMDeliveryDeadLettered
		}
		err = w.recordDelivery(operationCtx, eventType, claimed, class)
	}
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

func (w *Worker) recordDelivery(ctx context.Context, eventType audit.EventType, value runtimestorage.ReplyOutbox, class string) error {
	decision := audit.DecisionAccepted
	if eventType != audit.EventIMDeliverySent {
		decision = audit.DecisionRejected
	}
	requestID, traceID := value.ReplyID, ""
	if correlations, ok := w.store.(runtimestorage.ReplyCorrelationStore); ok {
		if correlation, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyCorrelation, error) {
			return correlations.GetReplyCorrelation(operationCtx, value.TenantID, value.EventID)
		}); err == nil {
			requestID, traceID = correlation.RequestID, correlation.TraceID
		}
	}
	return w.audit.IM(ctx, eventType, requestID, traceID, "", "", decision, class)
}

func (w *Worker) retryDue(value runtimestorage.ReplyOutbox, now time.Time) bool {
	if value.Status != runtimestorage.ReplyRetryable || w.backoffBase <= 0 || value.UpdatedAt.IsZero() {
		return true
	}
	attempt := value.Attempts
	if attempt < 1 {
		attempt = 1
	}
	delay := float64(w.backoffBase) * math.Pow(2, float64(attempt-1))
	if delay > float64(w.backoffMax) {
		delay = float64(w.backoffMax)
	}
	if w.jitter > 0 {
		h := fnv.New32a()
		_, _ = h.Write([]byte(value.ReplyID))
		factor := 1 + ((float64(h.Sum32()%1000)/999)-0.5)*2*w.jitter
		delay *= factor
	}
	return !now.Before(value.UpdatedAt.Add(time.Duration(delay)))
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
	candidates, err := observeStorage(w, ctx, func(operationCtx context.Context) ([]runtimestorage.ReplyOutbox, error) {
		return w.store.ListReplyCandidates(operationCtx, w.tenantID)
	})
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
	event, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.MessageEvent, error) {
		return w.store.GetMessage(operationCtx, w.tenantID, eventID)
	})
	if err != nil {
		return
	}
	if event.Status == runtimestorage.EventCompleted {
		if _, err := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.MessageEvent, error) {
			return w.store.TransitionMessage(operationCtx, runtimestorage.MessageTransition{TenantID: w.tenantID, EventID: eventID, From: runtimestorage.EventCompleted, To: runtimestorage.EventReplyPending, Owner: w.owner})
		}); err != nil {
			return
		}
		event.Status = runtimestorage.EventReplyPending
	}
	if event.Status == runtimestorage.EventReplyPending {
		_, _ = observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.MessageEvent, error) {
			return w.store.TransitionMessage(operationCtx, runtimestorage.MessageTransition{TenantID: w.tenantID, EventID: eventID, From: runtimestorage.EventReplyPending, To: runtimestorage.EventReplied, Owner: w.owner})
		})
	}
}

func (w *Worker) reconcile(ctx context.Context, claimed runtimestorage.ReplyOutbox) bool {
	status, providerID, err := w.reconcileProvider(ctx, claimed)
	if err != nil {
		return false
	}
	if status == DeliveryAccepted && providerID == "" {
		return false
	}
	switch status {
	case DeliveryAccepted, DeliveryRejected:
	case DeliveryUnknown, "":
		return false
	default:
		return false
	}
	to := runtimestorage.ReplySent
	class := ""
	if status == DeliveryRejected {
		to = runtimestorage.ReplyRetryable
		class = "provider_rejected"
	}
	_, transitionErr := observeStorage(w, ctx, func(operationCtx context.Context) (runtimestorage.ReplyOutbox, error) {
		return w.store.TransitionReply(operationCtx, runtimestorage.ReplyTransition{TenantID: claimed.TenantID, ReplyID: claimed.ReplyID, SegmentIndex: claimed.SegmentIndex, From: runtimestorage.ReplySending, To: to, Owner: w.owner, FencingToken: claimed.FencingToken, ProviderID: providerID, ErrorClass: class})
	})
	return transitionErr == nil
}

func (w *Worker) deliver(ctx context.Context, value runtimestorage.ReplyOutbox) (string, error) {
	if w == nil || w.provider == nil {
		return "", ErrProvider
	}
	if w.resilience == nil {
		providerID, err := w.provider.Deliver(ctx, value)
		if err != nil {
			return "", err
		}
		if providerID == "" {
			return "", &DeliveryError{Class: "provider_error", Retryable: false}
		}
		return providerID, nil
	}
	var providerID string
	err := w.resilience.Execute(ctx, func(callCtx context.Context) error {
		candidate, err := w.provider.Deliver(callCtx, value)
		if err != nil {
			return err
		}
		if candidate == "" {
			return &DeliveryError{Class: "provider_error", Retryable: false}
		}
		providerID = candidate
		return nil
	})
	if err == nil && providerID == "" {
		// A fallback can report success without having contacted the provider.
		// Without a receipt, accepting the outbox row would silently lose it.
		return "", &DeliveryError{Class: "provider_error", Retryable: false}
	}
	return providerID, err
}

func (w *Worker) reconcileProvider(ctx context.Context, value runtimestorage.ReplyOutbox) (DeliveryStatus, string, error) {
	if w == nil || w.provider == nil {
		return DeliveryUnknown, "", ErrProvider
	}
	if w.resilience == nil {
		return w.provider.Reconcile(ctx, value)
	}
	var status DeliveryStatus
	var providerID string
	err := w.resilience.Execute(ctx, func(callCtx context.Context) error {
		candidateStatus, candidateProviderID, err := w.provider.Reconcile(callCtx, value)
		if err != nil {
			return err
		}
		if candidateStatus == DeliveryAccepted && candidateProviderID == "" {
			return &DeliveryError{Class: "provider_error", Retryable: false}
		}
		status, providerID = candidateStatus, candidateProviderID
		return nil
	})
	return status, providerID, err
}

func observeStorage[T any](worker *Worker, ctx context.Context, operation func(context.Context) (T, error)) (T, error) {
	var zero T
	if worker == nil || operation == nil {
		return zero, ErrInvalid
	}
	started := time.Now()
	operationCtx, _, finish := observability.StartOperation(ctx, worker.telemetry, observability.OperationStorageOperation, "storage")
	provider := worker.providerName
	_ = worker.metrics.Request(operationCtx, map[string]string{"component": "storage", "operation": observability.OperationStorageOperation, "provider": provider, "status": "started"})
	value, err := operation(operationCtx)
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
