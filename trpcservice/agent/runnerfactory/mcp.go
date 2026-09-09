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
	return func(ctx context.Context, plan runtime.ExecutionPlan) (sets []tool.ToolSet, err error) {
		defer func() {
			if recover() != nil {
				_ = closeToolSets(sets)
				sets = nil
				err = fmt.Errorf("%w: MCP tool-set construction failed", runtimerunner.ErrInvalid)
			}
		}()
		if isNilFactoryValue(ctx) {
			return nil, fmt.Errorf("%w: MCP context is required", runtimerunner.ErrInvalid)
		}
		if err := factoryContextErr(ctx); err != nil {
			return nil, err
		}
		input, err := plan.AgentFactoryInput()
		if err != nil {
			return nil, fmt.Errorf("%w: MCP execution plan is invalid", runtimerunner.ErrInvalid)
		}
		if len(input.MCPBindings) == 0 {
			return nil, nil
		}
		sets = make([]tool.ToolSet, 0, len(input.MCPBindings))
		seenTools := make(map[string]struct{})
		seenBindings := make(map[string]struct{}, len(input.MCPBindings))
		for _, binding := range input.MCPBindings {
			value, normalizeErr := binding.Normalize()
			if normalizeErr != nil {
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: MCP binding is invalid", runtimerunner.ErrInvalid)
			}
			if _, duplicate := seenBindings[value.Name]; duplicate {
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: duplicate MCP binding %q", runtimerunner.ErrInvalid, value.Name)
			}
			seenBindings[value.Name] = struct{}{}
			set, materializeErr := servicetool.NewMCPToolSet(ctx, input.TenantID, value, secrets)
			if materializeErr != nil {
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: MCP binding %q: %v", runtimerunner.ErrInvalid, value.Name, materializeErr)
			}
			lifecycle, lifecycleErr := servicetool.WrapMCPToolSetLifecycle(set)
			if lifecycleErr != nil {
				_ = closeToolSet(set)
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: MCP binding %q lifecycle: %v", runtimerunner.ErrInvalid, value.Name, lifecycleErr)
			}
			namespaced, namespaceErr := servicetool.NamespaceMCPToolSet(lifecycle, value.Name)
			if namespaceErr != nil {
				_ = closeToolSet(lifecycle)
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: namespace MCP binding %q", runtimerunner.ErrInvalid, value.Name)
			}
			governed, governanceErr := servicetool.GovernMCPToolSet(ctx, namespaced, input.TenantID, input.AppID, value, reviewer)
			if governanceErr != nil {
				_ = closeToolSet(namespaced)
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: govern MCP binding %q", runtimerunner.ErrInvalid, value.Name)
			}
			available := make(map[string]struct{}, len(value.ToolAllow))
			governedTools, toolsOK := callToolSetTools(ctx, governed)
			if !toolsOK {
				_ = closeToolSet(governed)
				_ = closeToolSets(sets)
				return nil, fmt.Errorf("%w: MCP binding %q tools unavailable", runtimerunner.ErrInvalid, value.Name)
			}
			for _, candidate := range governedTools {
				declaration, declarationOK := safeToolDeclaration(candidate)
				if !declarationOK || declaration.Name == "" {
					_ = closeToolSet(governed)
					_ = closeToolSets(sets)
					return nil, fmt.Errorf("%w: MCP binding %q returned an invalid tool", runtimerunner.ErrInvalid, value.Name)
				}
				name := declaration.Name
				if _, duplicate := seenTools[name]; duplicate {
					_ = closeToolSet(governed)
					_ = closeToolSets(sets)
					return nil, fmt.Errorf("%w: duplicate MCP tool %q", runtimerunner.ErrInvalid, name)
				}
				seenTools[name] = struct{}{}
				available[name] = struct{}{}
			}
			for _, allowed := range value.ToolAllow {
				if _, found := available["mcp_"+value.Name+"__"+allowed]; !found {
					_ = closeToolSet(governed)
					_ = closeToolSets(sets)
					return nil, fmt.Errorf("%w: MCP binding %q did not advertise allowed tool %q", runtimerunner.ErrInvalid, value.Name, allowed)
				}
			}
			sets = append(sets, governed)
		}
		return sets, nil
	}
}

func callToolSetTools(ctx context.Context, set tool.ToolSet) (tools []tool.Tool, ok bool) {
	if isNilFactoryValue(ctx) || isNilFactoryValue(set) {
		return nil, false
	}
	defer func() {
		if recover() != nil {
			tools, ok = nil, false
		}
	}()
	return set.Tools(ctx), true
}
