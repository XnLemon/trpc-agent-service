package metrics

import (
	"context"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/audit"
	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
)

func TestValidateLabelsRejectsHighCardinality(t *testing.T) {
	if err := ValidateLabels(map[string]string{"component": "gateway", "status": "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLabels(map[string]string{"session_id": "sensitive"}); err == nil {
		t.Fatal("session_id must not be a metric label")
	}
	if err := ValidateLabels(map[string]string{"operation": "session-123"}); err == nil {
		t.Fatal("operation values must use the stable allowlist")
	}
	if err := ValidateLabels(map[string]string{"provider": "request-1234567890123456"}); err == nil {
		t.Fatal("provider values must reject high-cardinality identifiers")
	}
}

func TestCatalogNoopAcceptsAllowedLabels(t *testing.T) {
	catalog := New(observability.NewNoopProvider())
	labels := map[string]string{"component": "gateway", "operation": observability.OperationGatewayDispatch, "status": "ok"}
	ctx := context.Background()
	if err := catalog.Request(ctx, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Duration(ctx, 1.2, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Active(ctx, 1, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Lease(ctx, -1, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Retry(ctx, labels); err != nil {
		t.Fatal(err)
	}
	if err := catalog.State(ctx, 1, 0, labels); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogUsageUsesBoundedDimensions(t *testing.T) {
	catalog := New(observability.NewNoopProvider())
	total := audit.UsageTotal{TenantID: "tenant-a", AppID: "app-a", Channel: "telegram", Provider: "openai", Model: "gpt-family", ModelCostMinor: 3, ToolCostMinor: 2}
	if err := catalog.Usage(context.Background(), total, map[string]string{"component": "model"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Usage(context.Background(), total, map[string]string{"session_id": "sensitive"}); err == nil {
		t.Fatal("high-cardinality labels must be rejected")
	}
}
