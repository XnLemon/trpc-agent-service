package bootstrap

import (
	"context"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
	knowledgeauto "trpc.group/trpc-go/trpc-agent-go/knowledge/source/auto"
)

func TestEnvironmentKnowledgeProviderIndexesAndSearchesUpstreamSource(t *testing.T) {
	ctx := context.Background()
	value, err := (environmentNativeCapabilityProvider{capability: backend.CapabilityKnowledge}).New(ctx, backend.StorageFactoryInput{}, backend.CapabilityBinding{}, modelprofile.SecretValue{})
	if err != nil {
		t.Fatal(err)
	}
	service, ok := value.(*knowledge.BuiltinKnowledge)
	if !ok {
		t.Fatalf("knowledge provider returned %T", value)
	}
	defer service.Close()

	if err := service.AddSource(ctx, knowledgeauto.New([]string{"The tenant isolation policy requires every execution to use its authenticated tenant identity."})); err != nil {
		t.Fatal(err)
	}
	if err := service.Load(ctx, knowledge.WithShowStats(false)); err != nil {
		t.Fatal(err)
	}
	result, err := service.Search(ctx, &knowledge.SearchRequest{Query: "authenticated tenant identity", MaxResults: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Text == "" {
		t.Fatalf("upstream knowledge search returned no text: %+v", result)
	}
}
