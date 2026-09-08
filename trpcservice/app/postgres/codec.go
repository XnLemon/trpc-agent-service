package postgres

import appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"

type storedGenerationConfig struct {
	appmodel.GenerationConfig
	MCPBindings []appmodel.MCPBinding        `json:"mcp_bindings,omitempty"`
	Chain       *appmodel.ChainConfiguration `json:"chain,omitempty"`
}

func encodeAgentRevisionParts(revision appmodel.Revision) ([]byte, []byte, []byte, error) {
	generation, err := encodeJSON(storedGenerationConfig{
		GenerationConfig: revision.Generation,
		MCPBindings:      cloneMCPBindings(revision.MCPBindings),
		Chain:            revision.Chain.Clone(),
	})
	if err != nil {
		return nil, nil, nil, err
	}
	runtime, err := encodeJSON(revision.Runtime)
	if err != nil {
		return nil, nil, nil, err
	}
	tools, err := encodeJSON(revision.Tools)
	if err != nil {
		return nil, nil, nil, err
	}
	return generation, runtime, tools, nil
}

func cloneMCPBindings(bindings []appmodel.MCPBinding) []appmodel.MCPBinding {
	if bindings == nil {
		return nil
	}
	clone := make([]appmodel.MCPBinding, len(bindings))
	for index, binding := range bindings {
		clone[index] = binding
		clone[index].Args = append([]string(nil), binding.Args...)
		clone[index].ToolAllow = append([]string(nil), binding.ToolAllow...)
	}
	return clone
}

func decodeAgentRevisionParts(generation, runtime []byte, revision *appmodel.Revision) error {
	var stored storedGenerationConfig
	if err := decodeJSON(generation, &stored); err != nil {
		return err
	}
	revision.Generation = stored.GenerationConfig
	revision.MCPBindings = cloneMCPBindings(stored.MCPBindings)
	revision.Chain = stored.Chain.Clone()
	return decodeJSON(runtime, &revision.Runtime)
}
