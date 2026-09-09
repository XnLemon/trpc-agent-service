package agent

import (
	"context"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/factory"
	"trpc.group/trpc-go/trpc-agent-go/artifact"
)

// scopedArtifactService binds every upstream artifact operation to the
// immutable execution metadata installed by policyRunner. The upstream
// SessionInfo remains useful for its own namespace, but it is not trusted as
// the sole authorization boundary.
type scopedArtifactService struct {
	base     artifact.Service
	tenantID string
	appID    string
}

func newScopedArtifactService(base artifact.Service, tenantID, appID string) artifact.Service {
	if nilvalue.Is(base) || tenantID == "" || appID == "" {
		return nil
	}
	return scopedArtifactService{base: base, tenantID: tenantID, appID: appID}
}

func (service scopedArtifactService) check(ctx context.Context, scope artifact.SessionInfo) error {
	if nilvalue.Is(service.base) || nilvalue.Is(ctx) {
		return factory.ErrCapabilityUnavailable
	}
	metadata, ok := ExecutionMetadataFromContext(ctx)
	if !ok || metadata.TenantID != service.tenantID || metadata.AppID != service.appID || scope.SessionID != metadata.SessionID {
		return factory.ErrCapabilityUnavailable
	}
	// The upstream runner may use the tenant-prefixed Session key, while the
	// platform export tool deliberately supplies the logical app/user pair.
	// Both forms are accepted only when they describe this exact execution;
	// the capability itself is already tenant-bound by StorageFactory.
	rawScope := scope.AppName == service.appID && scope.UserID == metadata.UserID
	scopedScope := scope.AppName == tenantScopedIdentifier(service.tenantID, service.appID) && scope.UserID == tenantScopedIdentifier(service.tenantID, metadata.UserID)
	if !rawScope && !scopedScope {
		return factory.ErrCapabilityUnavailable
	}
	return nil
}

func (service scopedArtifactService) SaveArtifact(ctx context.Context, scope artifact.SessionInfo, filename string, value *artifact.Artifact) (int, error) {
	if err := service.check(ctx, scope); err != nil {
		return 0, err
	}
	return service.base.SaveArtifact(ctx, scope, filename, value)
}

func (service scopedArtifactService) LoadArtifact(ctx context.Context, scope artifact.SessionInfo, filename string, version *int) (*artifact.Artifact, error) {
	if err := service.check(ctx, scope); err != nil {
		return nil, err
	}
	return service.base.LoadArtifact(ctx, scope, filename, version)
}

func (service scopedArtifactService) ListArtifactKeys(ctx context.Context, scope artifact.SessionInfo) ([]string, error) {
	if err := service.check(ctx, scope); err != nil {
		return nil, err
	}
	return service.base.ListArtifactKeys(ctx, scope)
}

func (service scopedArtifactService) DeleteArtifact(ctx context.Context, scope artifact.SessionInfo, filename string) error {
	if err := service.check(ctx, scope); err != nil {
		return err
	}
	return service.base.DeleteArtifact(ctx, scope, filename)
}

func (service scopedArtifactService) ListVersions(ctx context.Context, scope artifact.SessionInfo, filename string) ([]int, error) {
	if err := service.check(ctx, scope); err != nil {
		return nil, err
	}
	return service.base.ListVersions(ctx, scope, filename)
}

// Close preserves normal CapabilitySet ownership for per-run providers while
// allowing an explicitly non-closing shared provider to remain shared.
func (service scopedArtifactService) Close() error {
	if nilvalue.Is(service.base) {
		return nil
	}
	if closer, ok := service.base.(interface{ Close() error }); ok && !nilvalue.Is(closer) {
		return closer.Close()
	}
	return nil
}

var _ artifact.Service = scopedArtifactService{}
