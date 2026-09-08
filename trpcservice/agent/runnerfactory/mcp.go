package runnerfactory

import (
	"context"
	"fmt"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	runtimerunner "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/runner"
	servicetool "github.com/XnLemon/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// NewMCPToolSetFactory creates the default sealed-plan MCP materializer. It
// only reads MCP declarations projected from the published Revision; drafts or
// mutable control-plane objects never reach this boundary.
func NewMCPToolSetFactory(secrets modelprofile.SecretResolver, reviewers ...review.Reviewer) ToolSetFactory {
	var reviewer review.Reviewer
	if len(reviewers) > 0 {
		reviewer = reviewers[0]
	}
	return func(ctx context.Context, plan runtime.ExecutionPlan) ([]tool.ToolSet, error) {
		input, err := plan.AgentFactoryInput()
		if err != nil {
			return nil, fmt.Errorf("%w: MCP execution plan is invalid", runtimerunner.ErrInvalid)
		}
		if len(input.MCPBindings) == 0 {
			return nil, nil
		}
		if secrets == nil {
			return nil, fmt.Errorf("%w: MCP secret resolver is required", runtimerunner.ErrInvalid)
		}
		sets := make([]tool.ToolSet, 0, len(input.MCPBindings))
		seenTools := make(map[string]struct{})
		for _, binding := range input.MCPBindings {
			if binding.Name == "" {
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: MCP binding name is empty", runtimerunner.ErrInvalid)
			}
			set, materializeErr := servicetool.NewMCPToolSet(ctx, input.TenantID, binding, secrets)
			if materializeErr != nil {
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: MCP binding %q: %v", runtimerunner.ErrInvalid, binding.Name, materializeErr)
			}
			lifecycle, lifecycleErr := servicetool.WrapMCPToolSetLifecycle(set)
			if lifecycleErr != nil {
				_ = set.Close()
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: MCP binding %q lifecycle: %v", runtimerunner.ErrInvalid, binding.Name, lifecycleErr)
			}
			namespaced, namespaceErr := servicetool.NamespaceMCPToolSet(lifecycle, binding.Name)
			if namespaceErr != nil {
				_ = lifecycle.Close()
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: namespace MCP binding %q", runtimerunner.ErrInvalid, binding.Name)
			}
			governed, governanceErr := servicetool.GovernMCPToolSet(ctx, namespaced, input.TenantID, binding, reviewer)
			if governanceErr != nil {
				_ = namespaced.Close()
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: govern MCP binding %q", runtimerunner.ErrInvalid, binding.Name)
			}
			available := make(map[string]struct{}, len(binding.ToolAllow))
			for _, candidate := range governed.Tools(ctx) {
				if candidate == nil || candidate.Declaration() == nil || candidate.Declaration().Name == "" {
					_ = governed.Close()
					_ = closeToolSets(sets)
					return nil, fmt.Errorf("%w: MCP binding %q returned an invalid tool", runtimerunner.ErrInvalid, binding.Name)
				}
				name := candidate.Declaration().Name
				if _, duplicate := seenTools[name]; duplicate {
					_ = governed.Close()
					_ = closeToolSets(sets)
					return nil, fmt.Errorf("%w: duplicate MCP tool %q", runtimerunner.ErrInvalid, name)
				}
				seenTools[name] = struct{}{}
				available[name] = struct{}{}
			}
			for _, allowed := range binding.ToolAllow {
				if _, found := available["mcp_"+binding.Name+"__"+allowed]; !found {
					_ = governed.Close()
					_ = closeToolSets(sets)
					return nil, fmt.Errorf("%w: MCP binding %q did not advertise allowed tool %q", runtimerunner.ErrInvalid, binding.Name, allowed)
				}
			}
			sets = append(sets, governed)
		}
		return sets, nil
	}
}
