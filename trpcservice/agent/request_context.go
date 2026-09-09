package agent

import (
	"context"
	"strings"
	"unicode"
	"unicode/utf8"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
)

// ExecutionMetadata is the trusted platform identity carried alongside an
// upstream Runner invocation. It lets context-aware upstream capabilities
// (for example Skill RepositoryProvider and reviewers) re-check the sealed
// tenant/app/revision boundary without parsing user input or provider state.
type ExecutionMetadata struct {
	TenantID      string
	AppID         string
	Revision      int64
	PrincipalKind string
	PrincipalID   string
	UserID        string
	SessionID     string
	EventID       string
	RequestID     string
	TraceID       string
}

type executionMetadataContextKey struct{}

// WithExecutionMetadata attaches immutable request metadata to ctx.
func WithExecutionMetadata(ctx context.Context, metadata ExecutionMetadata) context.Context {
	if nilvalue.Is(ctx) {
		return nil
	}
	return context.WithValue(ctx, executionMetadataContextKey{}, metadata)
}

// ExecutionMetadataFromContext returns trusted request metadata when present.
// Invalid metadata is deliberately reported as absent to capability wrappers,
// which then fail closed instead of using an incomplete scope.
func ExecutionMetadataFromContext(ctx context.Context) (ExecutionMetadata, bool) {
	metadata, present := executionMetadataValue(ctx)
	if !present || !validExecutionMetadata(metadata) {
		return ExecutionMetadata{}, false
	}
	return metadata, true
}

// executionMetadataValue distinguishes a missing metadata boundary from a
// caller-provided but malformed one. Runner is the final direct-call boundary
// and must reject both; capability wrappers only need the validated result.
func executionMetadataValue(ctx context.Context) (ExecutionMetadata, bool) {
	if nilvalue.Is(ctx) {
		return ExecutionMetadata{}, false
	}
	metadata, present := ctx.Value(executionMetadataContextKey{}).(ExecutionMetadata)
	return metadata, present
}

func validExecutionMetadata(metadata ExecutionMetadata) bool {
	if metadata.TenantID == "" || metadata.AppID == "" || appmodel.ValidateAppID(metadata.AppID) != nil || metadata.Revision < 1 || metadata.UserID == "" || metadata.SessionID == "" {
		return false
	}
	for _, value := range []string{metadata.TenantID, metadata.AppID, metadata.PrincipalKind, metadata.PrincipalID, metadata.UserID, metadata.SessionID, metadata.EventID, metadata.RequestID, metadata.TraceID} {
		if value != strings.TrimSpace(value) || strings.Contains(value, "://") || strings.IndexFunc(value, unicode.IsControl) >= 0 || !utf8.ValidString(value) || len([]rune(value)) > 256 {
			return false
		}
	}
	return true
}
