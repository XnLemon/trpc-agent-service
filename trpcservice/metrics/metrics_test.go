package metrics

import (
	"context"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/observability"
)

func TestValidateLabelsRejectsHighCardinality(t *testing.T) {
	if err := ValidateLabels(map[string]string{"component": "gateway", "status": "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateLabels(map[string]string{"session_id": "sensitive"}); err == nil {
		t.Fatal("session_id must not be a metric label")
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
}
