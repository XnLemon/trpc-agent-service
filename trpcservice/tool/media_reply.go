package tool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/attachment"
	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

const (
	// ExportArtifactID authorizes explicit export from an Agent artifact into a
	// platform-owned attachment and reply.
	ExportArtifactID = "export_artifact"
	// SendTestImageID is the revision allowlist ID for the controlled media
	// reply smoke-test tool.
	SendTestImageID = "send_test_image"

	testImageName     = "trpc-agent-test.png"
	testImageFallback = "[image attachment: trpc-agent-test.png]"
	testImagePayload  = "Here is the requested test image."
)

var (
	// ErrUnavailable reports a media tool that cannot use the current durable
	// execution boundary. Its detail is deliberately stable and safe for a model.
	ErrUnavailable = errors.New("media reply tool is unavailable")
	// ErrRequiredUnavailable reports an explicitly required revision tool that
	// is not installed in this service process.
	ErrRequiredUnavailable = errors.New("required tool is unavailable")
)

// ExecutionContext contains the server-owned state available while a tool is
// executing. It is deliberately absent from model-visible tool results.
type ExecutionContext struct {
	TenantID    string
	AppID       string
	UserID      string
	SessionID   string
	EventID     string
	RequestID   string
	TraceID     string
	Attachments runtimestorage.AttachmentStore
	Replies     *ReplyCollector
	Audit       audit.Recorder
	ToolBudget  *ToolCallBudget
}

// ToolCallBudget is request-local admission state for tool calls. It is never
// stored on a cached Runner, so concurrent tenants and executions cannot share
// a counter.
type ToolCallBudget struct {
	limit int64
	used  atomic.Int64
}

// NewToolCallBudget returns a bounded tool-call budget. A non-positive limit
// is rejected so an omitted budget remains distinguishable from a zero budget.
func NewToolCallBudget(limit int) (*ToolCallBudget, error) {
	if limit < 1 {
		return nil, errors.New("tool call budget must be positive")
	}
	return &ToolCallBudget{limit: int64(limit)}, nil
}

// Consume admits one call or returns ErrToolBudgetExceeded.
func (budget *ToolCallBudget) Consume() error {
	if budget == nil {
		return nil
	}
	for {
		used := budget.used.Load()
		if used >= budget.limit {
			return ErrToolBudgetExceeded
		}
		if budget.used.CompareAndSwap(used, used+1) {
			return nil
		}
	}
}

type executionContextKey struct{}

// WithExecutionContext attaches one durable execution boundary to ctx.
func WithExecutionContext(ctx context.Context, execution ExecutionContext) context.Context {
	if ctx == nil {
		return nil
	}
	return context.WithValue(ctx, executionContextKey{}, execution)
}

func executionContextFromContext(ctx context.Context) (ExecutionContext, error) {
	if ctx == nil {
		return ExecutionContext{}, ErrUnavailable
	}
	execution, ok := ctx.Value(executionContextKey{}).(ExecutionContext)
	if !ok || runtimestorage.ValidateTenant(execution.TenantID) != nil || execution.EventID == "" || execution.Attachments == nil || execution.Replies == nil {
		return ExecutionContext{}, ErrUnavailable
	}
	return execution, nil
}

// ReplyIntent is a protocol-neutral media reply selected by a tool. It never
// includes provider URLs, credentials, temporary media IDs, or object keys.
type ReplyIntent struct {
	Kind       runtimestorage.ReplyKind
	Attachment attachment.Reference
	Payload    string
	Fallback   string
}

// ReplyCollector collects media reply intents for one Runner execution. It is
// concurrency-safe because a revision may enable parallel tool calls.
type ReplyCollector struct {
	mu            sync.Mutex
	intents       []ReplyIntent
	seen          map[string]struct{}
	auditRecorder *audit.Recorder
}

// NewReplyCollector returns an empty collector for one execution.
func NewReplyCollector() *ReplyCollector {
	return &ReplyCollector{seen: make(map[string]struct{})}
}

// Add validates and records one media intent. Exact duplicate intents are
// ignored so a retried or parallel tool call cannot duplicate delivery.
func (collector *ReplyCollector) Add(intent ReplyIntent) error {
	if collector == nil {
		return ErrUnavailable
	}
	normalized, err := runtimestorage.NormalizeReplyOutbox(runtimestorage.ReplyOutbox{
		Kind:       intent.Kind,
		Attachment: intent.Attachment,
		Fallback:   intent.Fallback,
	})
	if err != nil {
		return err
	}
	intent.Kind = normalized.Kind
	intent.Attachment = normalized.Attachment
	intent.Fallback = normalized.Fallback
	key := string(intent.Kind) + "\x00" + intent.Attachment.ID
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.seen[key]; ok {
		return nil
	}
	collector.seen[key] = struct{}{}
	collector.intents = append(collector.intents, intent)
	return nil
}

// Intents returns a stable snapshot of the collected media replies.
func (collector *ReplyCollector) Intents() []ReplyIntent {
	if collector == nil {
		return nil
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	return append([]ReplyIntent(nil), collector.intents...)
}

func (collector *ReplyCollector) stableAuditRecorder(recorder audit.Recorder) audit.Recorder {
	if collector == nil {
		return recorder
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.auditRecorder == nil {
		fixed := recorder.WithFixedTime()
		collector.auditRecorder = &fixed
	}
	return *collector.auditRecorder
}

// Factory constructs one stateless, context-bound platform tool.
type Factory interface {
	ID() string
	New() trpctool.Tool
}

// Registry resolves a published revision's deny-by-default authorization list
// to the platform tools installed by this service process.
type Registry struct {
	factories map[string]Factory
}

// NewRegistry creates an immutable tool registry from the supplied factories.
func NewRegistry(factories ...Factory) (*Registry, error) {
	registry := &Registry{factories: make(map[string]Factory, len(factories))}
	for _, factory := range factories {
		if factory == nil || strings.TrimSpace(factory.ID()) == "" {
			return nil, ErrUnavailable
		}
		id := strings.TrimSpace(factory.ID())
		if _, ok := registry.factories[id]; ok {
			return nil, ErrUnavailable
		}
		registry.factories[id] = factory
	}
	return registry, nil
}

// DefaultRegistry contains the built-in platform tools. Future special-agent
// services can add a Factory without widening the Runner or channel contracts.
func DefaultRegistry() *Registry {
	registry, err := NewRegistry(sendTestImageFactory{}, exportArtifactFactory{})
	if err != nil {
		panic(err)
	}
	return registry
}

// ResolveWith returns installed platform and upstream tools explicitly
// authorized by the published revision. Upstream candidates use their
// declaration name as the stable authorization ID.
func (registry *Registry) ResolveWith(authorizations []appmodel.ToolAuthorization, candidates ...trpctool.Tool) ([]trpctool.Tool, error) {
	if registry == nil || len(authorizations) == 0 {
		return nil, nil
	}
	available := make(map[string]trpctool.Tool, len(registry.factories)+len(candidates))
	for id, factory := range registry.factories {
		tool := factory.New()
		if tool == nil || tool.Declaration() == nil || tool.Declaration().Name != id {
			return nil, ErrUnavailable
		}
		available[id] = tool
	}
	for _, candidate := range candidates {
		if candidate == nil || candidate.Declaration() == nil {
			return nil, ErrUnavailable
		}
		id := strings.TrimSpace(candidate.Declaration().Name)
		if id == "" {
			return nil, ErrUnavailable
		}
		if _, duplicate := available[id]; duplicate {
			return nil, ErrUnavailable
		}
		available[id] = candidate
	}
	tools := make([]trpctool.Tool, 0, len(authorizations))
	for _, authorization := range authorizations {
		id := strings.TrimSpace(authorization.ToolID)
		tool, ok := available[id]
		if !ok {
			if authorization.Required {
				return nil, fmt.Errorf("%w: %s", ErrRequiredUnavailable, id)
			}
			continue
		}
		tools = append(tools, tool)
	}
	return tools, nil
}

// Resolve retains the platform-only convenience API.
func (registry *Registry) Resolve(authorizations []appmodel.ToolAuthorization) ([]trpctool.Tool, error) {
	return registry.ResolveWith(authorizations)
}

type exportArtifactFactory struct{}

func (exportArtifactFactory) ID() string { return ExportArtifactID }

func (exportArtifactFactory) New() trpctool.Tool {
	return function.NewFunctionTool(
		exportArtifact,
		function.WithName(ExportArtifactID),
		function.WithDescription("Export a named Agent artifact version as a native media or document reply."),
		function.WithConcurrencySafe(true),
	)
}

type exportArtifactInput struct {
	Filename string `json:"filename" jsonschema:"description=Artifact filename to export"`
	Version  *int   `json:"version,omitempty" jsonschema:"description=Optional artifact version; omit for latest"`
}

type exportArtifactResult struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func exportArtifact(ctx context.Context, input exportArtifactInput) (exportArtifactResult, error) {
	execution, err := executionContextFromContext(ctx)
	if err != nil || strings.TrimSpace(execution.AppID) == "" || strings.TrimSpace(execution.UserID) == "" || strings.TrimSpace(execution.SessionID) == "" || strings.TrimSpace(input.Filename) == "" || input.Version != nil && *input.Version < 0 {
		return exportArtifactResult{}, ErrUnavailable
	}
	invocation, ok := agent.InvocationFromContext(ctx)
	if !ok || invocation == nil || invocation.ArtifactService == nil {
		return exportArtifactResult{}, ErrUnavailable
	}
	recorder := execution.Replies.stableAuditRecorder(execution.Audit)
	policy := Policy{Recorder: recorder, Allowed: map[string]Decision{ExportArtifactID: Allow}}
	if _, err := policy.Decide(ctx, execution.RequestID, execution.TraceID, ExportArtifactID); err != nil {
		return exportArtifactResult{}, redactedToolError(err)
	}
	filename := strings.TrimSpace(input.Filename)
	value, err := invocation.ArtifactService.LoadArtifact(ctx, artifact.SessionInfo{
		AppName: execution.AppID, UserID: execution.UserID, SessionID: execution.SessionID,
	}, filename, input.Version)
	if err != nil {
		return exportArtifactResult{}, redactedToolError(err)
	}
	if value == nil || len(value.Data) == 0 {
		return exportArtifactResult{}, ErrUnavailable
	}
	name := strings.TrimSpace(value.Name)
	if name == "" {
		name = filename
	}
	kind, replyKind := attachmentKinds(value.MimeType)
	reference, err := execution.Attachments.PutAttachment(ctx, execution.TenantID, attachment.Upload{
		ID:   exportArtifactAttachmentID(execution.EventID, filename, input.Version, value.Data),
		Kind: kind, MIMEType: strings.ToLower(strings.TrimSpace(value.MimeType)), Name: name,
		Size: int64(len(value.Data)), Provider: "artifact", ProviderID: ExportArtifactID,
	}, bytes.NewReader(value.Data))
	if err != nil {
		return exportArtifactResult{}, redactedToolError(err)
	}
	if err := execution.Attachments.BindAttachments(ctx, execution.TenantID, execution.EventID, []attachment.Reference{reference}); err != nil {
		return exportArtifactResult{}, redactedToolError(err)
	}
	if err := execution.Replies.Add(ReplyIntent{Kind: replyKind, Attachment: reference, Fallback: "[artifact attachment: " + name + "]"}); err != nil {
		return exportArtifactResult{}, redactedToolError(err)
	}
	if err := recorder.ToolExecuted(ctx, execution.RequestID, execution.TraceID, ExportArtifactID); err != nil {
		return exportArtifactResult{}, redactedToolError(err)
	}
	return exportArtifactResult{Status: "queued", Message: "The artifact is queued for native delivery."}, nil
}

func attachmentKinds(mimeType string) (attachment.Kind, runtimestorage.ReplyKind) {
	value := strings.ToLower(strings.TrimSpace(mimeType))
	switch {
	case strings.HasPrefix(value, "image/"):
		return attachment.KindImage, runtimestorage.ReplyKindImage
	case strings.HasPrefix(value, "video/"):
		return attachment.KindVideo, runtimestorage.ReplyKindVideo
	case strings.HasPrefix(value, "audio/"):
		return attachment.KindAudio, runtimestorage.ReplyKindAudio
	default:
		return attachment.KindDocument, runtimestorage.ReplyKindDocument
	}
}

func exportArtifactAttachmentID(eventID, filename string, version *int, data []byte) string {
	versionValue := "latest"
	if version != nil {
		versionValue = fmt.Sprintf("%d", *version)
	}
	digest := sha256.Sum256(data)
	sum := sha256.Sum256([]byte(eventID + "\x00" + filename + "\x00" + versionValue + "\x00" + hex.EncodeToString(digest[:])))
	return "artifact_" + hex.EncodeToString(sum[:16])
}

type sendTestImageFactory struct{}

func (sendTestImageFactory) ID() string { return SendTestImageID }

func (sendTestImageFactory) New() trpctool.Tool {
	return function.NewFunctionTool(
		func(ctx context.Context, _ struct{}) (sendTestImageResult, error) {
			return sendTestImage(ctx)
		},
		function.WithName(SendTestImageID),
		function.WithDescription("Queue a controlled test image as a native media reply when the user asks to receive a test image."),
		function.WithSkipSummarization(true),
		function.WithConcurrencySafe(true),
	)
}

type sendTestImageResult struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func sendTestImage(ctx context.Context) (sendTestImageResult, error) {
	execution, err := executionContextFromContext(ctx)
	if err != nil {
		return sendTestImageResult{}, err
	}
	recorder := execution.Replies.stableAuditRecorder(execution.Audit)
	policy := Policy{
		Recorder: recorder,
		Allowed:  map[string]Decision{SendTestImageID: Allow},
	}
	if _, err := policy.Decide(ctx, execution.RequestID, execution.TraceID, SendTestImageID); err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	reference, err := execution.Attachments.PutAttachment(ctx, execution.TenantID, attachment.Upload{
		ID:         testImageAttachmentID(execution.EventID),
		Kind:       attachment.KindImage,
		MIMEType:   "image/png",
		Name:       testImageName,
		Size:       int64(len(testImagePNG)),
		Provider:   "tool",
		ProviderID: SendTestImageID,
	}, bytes.NewReader(testImagePNG))
	if err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	if err := execution.Attachments.BindAttachments(ctx, execution.TenantID, execution.EventID, []attachment.Reference{reference}); err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	if err := execution.Replies.Add(ReplyIntent{Kind: runtimestorage.ReplyKindImage, Attachment: reference, Payload: testImagePayload, Fallback: testImageFallback}); err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	if err := recorder.ToolExecuted(ctx, execution.RequestID, execution.TraceID, SendTestImageID); err != nil {
		return sendTestImageResult{}, redactedToolError(err)
	}
	return sendTestImageResult{Status: "queued", Message: "The requested test image is queued for native delivery."}, nil
}

func testImageAttachmentID(eventID string) string {
	sum := sha256.Sum256([]byte(eventID + "\x00" + SendTestImageID))
	return "tool_" + hex.EncodeToString(sum[:16])
}

func redactedToolError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrUnavailable
}

// A fixed, valid 1x1 PNG keeps the first end-to-end slice deterministic and
// avoids introducing an image-generation provider into this transport issue.
var testImagePNG = []byte{
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d,
	0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
	0x0d, 0x49, 0x44, 0x41, 0x54, 0x08, 0xd7, 0x63, 0xf8, 0xcf, 0xc0, 0xf0,
	0x1f, 0x00, 0x05, 0x00, 0x01, 0xff, 0x89, 0x99, 0x3d, 0x1d, 0x00, 0x00,
	0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82,
}
