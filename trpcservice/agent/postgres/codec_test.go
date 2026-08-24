package postgres

import (
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
)

func TestAgentPostgresRevisionCodec(t *testing.T) {
	revision := agent.Revision{Generation: agent.GenerationConfig{}, Runtime: agent.DefaultRuntimePolicy(), Tools: []agent.ToolAuthorization{{ToolID: "tool", Required: true}}}
	generation, runtime, tools, err := encodeAgentRevisionParts(revision)
	if err != nil {
		t.Fatal(err)
	}
	var decoded agent.Revision
	if err := decodeAgentRevisionParts(generation, runtime, &decoded); err != nil || decoded.Runtime.MaxLLMCalls != revision.Runtime.MaxLLMCalls {
		t.Fatalf("revision decode = %+v, err=%v", decoded, err)
	}
	var decodedTools []agent.ToolAuthorization
	if err := decodeJSON(tools, &decodedTools); err != nil || len(decodedTools) != 1 || decodedTools[0].ToolID != "tool" {
		t.Fatalf("tools decode = %+v, err=%v", decodedTools, err)
	}
	if err := decodeAgentRevisionParts([]byte("not-json"), []byte("{}"), &agent.Revision{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("malformed generation error = %v", err)
	}
}
