package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	skillsecurity "github.com/XnLemon/trpc-agent-service/trpcservice/skill"
	upstreamskill "trpc.group/trpc-go/trpc-agent-go/skill"
)

var errSkillScope = errors.New("skill repository scope violation")

type tenantSkillRepositoryProvider struct {
	delegate       upstreamskill.RepositoryProvider
	tenantID       string
	appID          string
	revision       int64
	allow          map[string]struct{}
	authorizations []appmodel.SkillAuthorization
	trust          skillsecurity.TrustPolicy
}

// newTenantSkillRepositoryProvider is retained for direct legacy callers. New
// Runner construction uses newSecuredTenantSkillRepositoryProvider, which
// requires revision-pinned manifests and signatures.
func newTenantSkillRepositoryProvider(delegate upstreamskill.RepositoryProvider, tenantID, appID string, allowed []string) upstreamskill.RepositoryProvider {
	allow := make(map[string]struct{}, len(allowed))
	for _, name := range allowed {
		allow[strings.TrimSpace(name)] = struct{}{}
	}
	return tenantSkillRepositoryProvider{delegate: delegate, tenantID: tenantID, appID: appID, allow: allow}
}

func newSecuredTenantSkillRepositoryProvider(delegate upstreamskill.RepositoryProvider, tenantID, appID string, revision int64, authorizations []appmodel.SkillAuthorization, trust skillsecurity.TrustPolicy) upstreamskill.RepositoryProvider {
	allow := make(map[string]struct{}, len(authorizations))
	cloned := make([]appmodel.SkillAuthorization, len(authorizations))
	for index, authorization := range authorizations {
		cloned[index] = authorization.Clone()
		allow[cloned[index].Name] = struct{}{}
	}
	return tenantSkillRepositoryProvider{
		delegate: delegate, tenantID: tenantID, appID: appID, revision: revision,
		allow: allow, authorizations: cloned, trust: trust,
	}
}

// Repository resolves a tenant/App/Revision-bound upstream repository and then
// applies the immutable Revision allowlist and content attestation. The
// provider refuses calls without request metadata installed by the platform
// Runner boundary.
func (provider tenantSkillRepositoryProvider) Repository(ctx context.Context, scope upstreamskill.SkillScope) (repository upstreamskill.Repository, err error) {
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
	if !ok || metadata.TenantID != provider.tenantID || metadata.AppID != provider.appID || (provider.revision > 0 && metadata.Revision != provider.revision) {
		return nil, errSkillScope
	}
	if strings.TrimSpace(scope.AppName) == "" || scope.AppName != provider.appID || scope.UserID != "" {
		return nil, fmt.Errorf("%w: app scope is required", errSkillScope)
	}
	if provider.revision > 0 {
		bound, boundOK := skillsecurity.ScopeFromContext(ctx)
		if !boundOK || bound.TenantID != provider.tenantID || bound.AppID != provider.appID || bound.Revision != provider.revision {
			return nil, errSkillScope
		}
	}
	repository, err = provider.delegate.Repository(ctx, scope)
	if err != nil {
		return nil, errSkillScope
	}
	if nilvalue.Is(repository) {
		return nil, errSkillScope
	}
	if len(provider.authorizations) > 0 {
		secure, secureErr := skillsecurity.NewSecureRepository(repository, skillsecurity.Scope{
			TenantID: provider.tenantID, AppID: provider.appID, Revision: provider.revision,
		}, provider.authorizations, provider.trust)
		if secureErr != nil || nilvalue.Is(secure) {
			if secureErr == nil {
				secureErr = skillsecurity.ErrUntrustedSource
			}
			return nil, fmt.Errorf("%w: %w", errSkillScope, secureErr)
		}
		return secure, nil
	}
	filtered := upstreamskill.NewFilteredRepository(repository, func(_ context.Context, summary upstreamskill.Summary) bool {
		_, allowed := provider.allow[summary.Name]
		return allowed
	})
	if nilvalue.Is(filtered) {
		return nil, errSkillScope
	}
	return filtered, nil
}
