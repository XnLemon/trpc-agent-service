package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/XnLemon/trpc-agent-service/trpcservice/internal/nilvalue"
	knowledgeadmin "github.com/XnLemon/trpc-agent-service/trpcservice/knowledge"
)

// VersionRepository persists platform Knowledge publication manifests in the
// same PostgreSQL database as the tenant/app control plane. It borrows the
// caller's pool and never closes it.
type VersionRepository struct {
	db *sql.DB
}

// NewVersionRepository creates a durable publication repository.
func NewVersionRepository(db *sql.DB) *VersionRepository {
	return &VersionRepository{db: db}
}

var _ knowledgeadmin.VersionStore = (*VersionRepository)(nil)

func (repository *VersionRepository) PublishVersion(ctx context.Context, value knowledgeadmin.Version) (knowledgeadmin.Version, error) {
	if err := validateVersionContext(ctx, value); err != nil {
		return knowledgeadmin.Version{}, err
	}
	if repository == nil || repository.db == nil {
		return knowledgeadmin.Version{}, knowledgeadmin.ErrUnavailable
	}
	tx, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return knowledgeadmin.Version{}, mapVersionError(ctx, err)
	}
	rollback := true
	defer func() {
		if rollback {
			_ = tx.Rollback()
		}
	}()
	// Serialize version allocation only for this tenant/app pair. The lock is
	// transaction-scoped and does not expose the lock key to callers.
	lockKey := value.TenantID + "\x00" + value.AppID
	if _, err := tx.ExecContext(ctx, `SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext($1))`, lockKey); err != nil {
		return knowledgeadmin.Version{}, mapVersionError(ctx, err)
	}
	var existing knowledgeadmin.Version
	err = tx.QueryRowContext(ctx, `SELECT tenant_id, app_id, version, document_count, content_digest, actor_id, published_at
		FROM public.runtime_knowledge_version WHERE tenant_id = $1 AND app_id = $2 AND content_digest = $3`, value.TenantID, value.AppID, value.ContentDigest).Scan(&existing.TenantID, &existing.AppID, &existing.Version, &existing.DocumentCount, &existing.ContentDigest, &existing.ActorID, &existing.PublishedAt)
	if err == nil {
		if !validStoredVersion(existing) {
			return knowledgeadmin.Version{}, knowledgeadmin.ErrUnavailable
		}
		if commitErr := tx.Commit(); commitErr != nil {
			return knowledgeadmin.Version{}, mapVersionError(ctx, commitErr)
		}
		rollback = false
		return existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return knowledgeadmin.Version{}, mapVersionError(ctx, err)
	}
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) + 1 FROM public.runtime_knowledge_version WHERE tenant_id = $1 AND app_id = $2`, value.TenantID, value.AppID).Scan(&value.Version); err != nil {
		return knowledgeadmin.Version{}, mapVersionError(ctx, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO public.runtime_knowledge_version
		(tenant_id, app_id, version, document_count, content_digest, actor_id, published_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`, value.TenantID, value.AppID, value.Version, value.DocumentCount, value.ContentDigest, value.ActorID, value.PublishedAt); err != nil {
		return knowledgeadmin.Version{}, mapVersionError(ctx, err)
	}
	if err := tx.Commit(); err != nil {
		return knowledgeadmin.Version{}, mapVersionError(ctx, err)
	}
	rollback = false
	return value, nil
}

func (repository *VersionRepository) ListVersions(ctx context.Context, scope knowledgeadmin.Scope) ([]knowledgeadmin.Version, error) {
	if nilvalue.Is(ctx) {
		return nil, knowledgeadmin.ErrInvalid
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return nil, err
	}
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	if repository == nil || repository.db == nil {
		return nil, knowledgeadmin.ErrUnavailable
	}
	rows, err := repository.db.QueryContext(ctx, `SELECT tenant_id, app_id, version, document_count, content_digest, actor_id, published_at
		FROM public.runtime_knowledge_version WHERE tenant_id = $1 AND app_id = $2 ORDER BY version DESC`, scope.TenantID, scope.AppID)
	if err != nil {
		return nil, mapVersionError(ctx, err)
	}
	defer func() { _ = rows.Close() }()
	values := make([]knowledgeadmin.Version, 0)
	for rows.Next() {
		var value knowledgeadmin.Version
		if err := rows.Scan(&value.TenantID, &value.AppID, &value.Version, &value.DocumentCount, &value.ContentDigest, &value.ActorID, &value.PublishedAt); err != nil || !validStoredVersion(value) {
			return nil, knowledgeadmin.ErrUnavailable
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, knowledgeadmin.ErrUnavailable
	}
	return values, nil
}

func validateVersionContext(ctx context.Context, value knowledgeadmin.Version) error {
	if nilvalue.Is(ctx) {
		return knowledgeadmin.ErrInvalid
	}
	if err := nilvalue.ContextErr(ctx); err != nil {
		return err
	}
	if (knowledgeadmin.Scope{TenantID: value.TenantID, AppID: value.AppID}).Validate() != nil || value.Version != 0 || value.DocumentCount < 0 || len(value.ContentDigest) != 64 || strings.ToLower(value.ContentDigest) != value.ContentDigest || value.PublishedAt.IsZero() || value.PublishedAt.Location() != time.UTC || value.ActorID != strings.TrimSpace(value.ActorID) || strings.TrimSpace(value.ActorID) == "" || len([]rune(value.ActorID)) > 256 {
		return knowledgeadmin.ErrInvalid
	}
	if _, err := hex.DecodeString(value.ContentDigest); err != nil {
		return knowledgeadmin.ErrInvalid
	}
	return nil
}

func validStoredVersion(value knowledgeadmin.Version) bool {
	if !utf8.ValidString(value.ActorID) || value.Version < 1 || value.DocumentCount < 0 || (knowledgeadmin.Scope{TenantID: value.TenantID, AppID: value.AppID}).Validate() != nil || len(value.ContentDigest) != sha256.Size*2 || strings.ToLower(value.ContentDigest) != value.ContentDigest || value.PublishedAt.IsZero() || value.PublishedAt.Location() != time.UTC || value.ActorID != strings.TrimSpace(value.ActorID) || value.ActorID == "" || len([]rune(value.ActorID)) > 256 {
		return false
	}
	for _, character := range value.ActorID {
		if unicode.IsControl(character) {
			return false
		}
	}
	_, err := hex.DecodeString(value.ContentDigest)
	return err == nil
}

func mapVersionError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if !nilvalue.Is(ctx) {
		if ctxErr := nilvalue.ContextErr(ctx); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, knowledgeadmin.ErrInvalid) || errors.Is(err, knowledgeadmin.ErrConflict) || errors.Is(err, knowledgeadmin.ErrUnavailable) {
		return err
	}
	if strings.Contains(strings.ToLower(err.Error()), "duplicate") || strings.Contains(strings.ToLower(err.Error()), "unique") {
		return knowledgeadmin.ErrConflict
	}
	return knowledgeadmin.ErrUnavailable
}
