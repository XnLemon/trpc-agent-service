package agent

import (
	"context"
	"errors"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
)

// KnowledgeAppMetadataKey is reserved for the platform's immutable app
// partition. Knowledge writes must set this key and runtime reads always add
// it to the upstream filter. It is deliberately not derived from user input.
const KnowledgeAppMetadataKey = "_trpc_app_id"

// ErrKnowledgeScope reports a missing or conflicting tenant/app execution
// boundary. A knowledge provider must fail closed rather than return an
// unscoped result.
var ErrKnowledgeScope = errors.New("knowledge app scope violation")

type scopedKnowledge struct {
	base     knowledge.Knowledge
	tenantID string
	appID    string
}

func newScopedKnowledge(base knowledge.Knowledge, tenantID, appID string) knowledge.Knowledge {
	if nilvalue.Is(base) || strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appID) == "" {
		return nil
	}
	return scopedKnowledge{base: base, tenantID: tenantID, appID: appID}
}

// Search adds the immutable app predicate to every upstream query and
// validates the returned documents as a defense in depth boundary. The
// upstream Knowledge interface is intentionally retained; this wrapper is the
// platform-owned tenant/app authorization layer around it.
func (service scopedKnowledge) Search(ctx context.Context, request *knowledge.SearchRequest) (*knowledge.SearchResult, error) {
	if nilvalue.Is(service.base) || nilvalue.Is(ctx) {
		return nil, ErrKnowledgeScope
	}
	metadata, ok := ExecutionMetadataFromContext(ctx)
	if !ok || metadata.TenantID != service.tenantID || metadata.AppID != service.appID {
		return nil, ErrKnowledgeScope
	}
	if request == nil {
		return nil, ErrKnowledgeScope
	}
	copyRequest := *request
	if request.History != nil {
		copyRequest.History = append([]knowledge.ConversationMessage(nil), request.History...)
	}
	filter := &knowledge.SearchFilter{}
	if request.SearchFilter != nil {
		*filter = *request.SearchFilter
		filter.DocumentIDs = append([]string(nil), request.SearchFilter.DocumentIDs...)
		filter.Metadata = make(map[string]any, len(request.SearchFilter.Metadata)+1)
		for key, value := range request.SearchFilter.Metadata {
			filter.Metadata[key] = value
		}
		if request.SearchFilter.Metadata != nil {
			if value, exists := request.SearchFilter.Metadata[KnowledgeAppMetadataKey]; exists && value != service.appID {
				return nil, ErrKnowledgeScope
			}
		}
	} else {
		filter.Metadata = make(map[string]any, 1)
	}
	filter.Metadata[KnowledgeAppMetadataKey] = service.appID
	copyRequest.SearchFilter = filter

	result, err := service.base.Search(ctx, &copyRequest)
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, ErrKnowledgeScope
	}
	filtered := make([]*knowledge.Result, 0, len(result.Documents)+1)
	for _, candidate := range result.Documents {
		if candidate == nil || candidate.Document == nil || !documentBelongsToApp(candidate.Document, service.appID) {
			continue
		}
		filtered = append(filtered, candidate)
	}
	// Some upstream implementations populate only the convenience Document
	// field. Treat it as another candidate, but never let its content bypass the
	// immutable metadata check.
	if len(filtered) == 0 && result.Document != nil && documentBelongsToApp(result.Document, service.appID) {
		filtered = append(filtered, &knowledge.Result{Document: result.Document, Score: result.Score})
	}
	output := *result
	output.Documents = filtered
	if len(filtered) == 0 {
		// No matching documents is a valid retrieval result. Only reject a
		// provider that returned unscoped text without a document proving its
		// partition; otherwise callers must be able to handle an empty search.
		if strings.TrimSpace(result.Text) != "" || result.Document != nil {
			return nil, ErrKnowledgeScope
		}
		output.Document = nil
		output.Score = 0
		return &output, nil
	}
	output.Document = filtered[0].Document
	output.Score = filtered[0].Score
	output.Text = output.Document.Content
	return &output, nil
}

func documentBelongsToApp(doc *document.Document, appID string) bool {
	if doc == nil {
		return false
	}
	value, ok := doc.Metadata[KnowledgeAppMetadataKey]
	app, ok := value.(string)
	return ok && app == appID
}
