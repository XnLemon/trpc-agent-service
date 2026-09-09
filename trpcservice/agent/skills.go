package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	"trpc.group/trpc-go/trpc-agent-go/skill"
)

var errSkillScope = errors.New("skill repository scope violation")

type tenantSkillRepositoryProvider struct {
	delegate skill.RepositoryProvider
	tenantID string
	appID    string
	allow    map[string]struct{}
}

func newTenantSkillRepositoryProvider(delegate skill.RepositoryProvider, tenantID, appID string, allowed []string) skill.RepositoryProvider {
	allow := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allow[strings.TrimSpace(name)] = struct{}{}
	}
	return tenantSkillRepositoryProvider{delegate: delegate, tenantID: tenantID, appID: appID, allow: allow}
}

// Repository resolves a tenant/app-bound upstream repository and then applies
// the immutable revision skill allowlist. The provider refuses calls without
// the request metadata installed by the platform gateway.
func (provider tenantSkillRepositoryProvider) Repository(ctx context.Context, scope skill.SkillScope) (repository skill.Repository, err error) {
	if nilvalue.Is(ctx) || nilvalue.Is(provider.delegate) {
		return nil, errSkillScope
	}
	defer func() {
		if recover() != nil {
			repository = nil
			err = errSkillScope
		}
	}()
	metadata, ok := ExecutionMetadataFromContext(ctx)
	if !ok || metadata.TenantID != provider.tenantID || metadata.AppID != provider.appID {
		return nil, errSkillScope
	}
	if strings.TrimSpace(scope.AppName) == "" || scope.AppName != provider.appID || scope.UserID != "" {
		return nil, fmt.Errorf("%w: app scope is required", errSkillScope)
	}
	repository, err = provider.delegate.Repository(ctx, scope)
	if err != nil {
		return nil, errSkillScope
	}
	if nilvalue.Is(repository) {
		return nil, errSkillScope
	}
	filtered := skill.NewFilteredRepository(repository, func(_ context.Context, summary skill.Summary) bool {
		_, allowed := provider.allow[summary.Name]
		return allowed
	})
	if nilvalue.Is(filtered) {
		return nil, errSkillScope
	}
	return filtered, nil
}
