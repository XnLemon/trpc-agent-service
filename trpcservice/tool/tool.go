// Package tool registers platform tools and tenant-scoped MCP / function tools.
package tool

import (
	"context"
	"errors"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
)

var (
	ErrDenied           = errors.New("tool denied")
	ErrApprovalRequired = errors.New("tool approval required")
)

type Decision string

const (
	Allow            Decision = "allow"
	Deny             Decision = "deny"
	ApprovalRequired Decision = "approval_required"
)

// Policy is the provider-neutral tool admission boundary. It records only the
// tool name and decision; arguments and results never enter the audit event.
type Policy struct {
	Recorder audit.Recorder
	Allowed  map[string]Decision
}

func (p Policy) Decide(ctx context.Context, requestID, traceID, toolName string) (Decision, error) {
	toolName = strings.TrimSpace(toolName)
	if toolName == "" || len([]rune(toolName)) > 256 {
		return "", audit.ErrInvalid
	}
	decision := Deny
	if value, ok := p.Allowed[toolName]; ok {
		decision = value
	}
	var err error
	switch decision {
	case Allow:
		err = nil
	case ApprovalRequired:
		err = ErrApprovalRequired
	default:
		decision = Deny
		err = ErrDenied
	}
	eventType := audit.EventToolDenied
	if decision == Allow {
		eventType = audit.EventToolAllowed
	} else if decision == ApprovalRequired {
		eventType = audit.EventToolApprovalRequired
	}
	if auditErr := p.Recorder.ToolDecision(ctx, eventType, requestID, traceID, toolName, audit.Decision(decision), ""); auditErr != nil {
		return "", audit.ErrWriteFailed
	}
	return decision, err
}
