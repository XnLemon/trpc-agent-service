package mysql

import appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"

type storedGenerationConfig struct {
	appmodel.GenerationConfig
	MCPBindings         []appmodel.MCPBinding         `json:"mcp_bindings,omitempty"`
	Skills              []string                      `json:"skills,omitempty"`
	SkillAuthorizations []appmodel.SkillAuthorization `json:"skill_authorizations,omitempty"`
	Chain               *appmodel.ChainConfiguration  `json:"chain,omitempty"`
}

func encodeAgentRevisionParts(revision appmodel.Revision) ([]byte, []byte, []byte, error) {
	generation, err := encodeJSON(storedGenerationConfig{
		GenerationConfig:    revision.Generation,
		MCPBindings:         cloneMCPBindings(revision.MCPBindings),
		Skills:              append([]string(nil), revision.Skills...),
		SkillAuthorizations: cloneSkillAuthorizations(revision.SkillAuthorizations),
		Chain:               revision.Chain.Clone(),
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
		if binding.ToolPolicies != nil {
			clone[index].ToolPolicies = make(map[string]appmodel.MCPToolPolicy, len(binding.ToolPolicies))
			for name, policy := range binding.ToolPolicies {
				clone[index].ToolPolicies[name] = policy
			}
		}
	}
	return clone
}

func cloneSkillAuthorizations(values []appmodel.SkillAuthorization) []appmodel.SkillAuthorization {
	if values == nil {
		return nil
	}
	clone := make([]appmodel.SkillAuthorization, len(values))
	for index, value := range values {
		clone[index] = value.Clone()
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
	revision.Skills = append([]string(nil), stored.Skills...)
	revision.SkillAuthorizations = cloneSkillAuthorizations(stored.SkillAuthorizations)
	revision.Chain = stored.Chain.Clone()
	return decodeJSON(runtime, &revision.Runtime)
}
