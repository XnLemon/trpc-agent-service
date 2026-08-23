package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

// ModelRepository persists secret-free Model Profiles and their change
// outbox events.
type ModelRepository struct {
	db      *sql.DB
	catalog *model.ProviderCatalog
}

var _ model.Repository = (*ModelRepository)(nil)

// NewModelRepository creates a repository that revalidates every decoded
// profile against the same trusted ProviderCatalog used for writes.
func NewModelRepository(db *sql.DB, catalog *model.ProviderCatalog) *ModelRepository {
	return &ModelRepository{db: db, catalog: catalog}
}

func (r *ModelRepository) Create(ctx context.Context, input model.CreateInput) (*model.Profile, model.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, model.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, model.ChangeEvent{}, ErrStorage
	}
	value, err := model.NewProfile(input, r.catalog)
	if err != nil {
		return nil, model.ChangeEvent{}, err
	}
	event, err := model.PrepareCreatedChange(*value, r.catalog, input.Metadata)
	if err != nil {
		return nil, model.ChangeEvent{}, err
	}
	options, generation, err := encodeModelJSON(value.Configuration)
	if err != nil {
		return nil, model.ChangeEvent{}, ErrStorage
	}
	var eventID int64
	err = r.db.QueryRowContext(ctx, `
		SELECT public.control_plane_create_model_profile(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21
		)`,
		value.TenantID, value.ProfileID, value.ProfileKey, value.DisplayName, value.Description,
		string(value.Status), value.SchemaVersion, value.Configuration.Provider,
		value.Configuration.Model, value.Configuration.Endpoint, options, nullableText(value.Configuration.SecretRef),
		generation, value.ContentDigest, value.Version, value.CreatedAt, value.UpdatedAt,
		event.ActorType, event.ActorID, event.Reason, event.CorrelationID,
	).Scan(&eventID)
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	stored, err := r.Get(ctx, value.TenantID, value.ProfileID)
	if err != nil {
		return nil, model.ChangeEvent{}, err
	}
	committed, err := r.readModelEvent(ctx, eventID)
	if err != nil {
		return nil, model.ChangeEvent{}, err
	}
	return stored, committed, nil
}

func (r *ModelRepository) Get(ctx context.Context, tenantID, profileID string) (*model.Profile, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r == nil || r.db == nil {
		return nil, ErrStorage
	}
	value, err := scanModelProfile(r.catalog, r.db.QueryRowContext(ctx, modelSelect+` WHERE tenant_id = $1 AND profile_id = $2`, tenantID, profileID))
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("%w: %s", model.ErrNotFound, profileID)
		}
		return nil, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	return value, nil
}

func (r *ModelRepository) UpdateConfiguration(ctx context.Context, input model.UpdateConfigurationInput) (*model.Profile, model.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, model.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, model.ChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, model.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := scanModelProfile(r.catalog, tx.QueryRowContext(ctx, modelSelect+` WHERE tenant_id = $1 AND profile_id = $2 FOR UPDATE`, input.TenantID, input.ProfileID))
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	occurredAt := monotonicNow(current.UpdatedAt)
	updated, event, err := model.PrepareConfigurationChange(*current, input, r.catalog, occurredAt)
	if err != nil {
		return nil, model.ChangeEvent{}, err
	}
	options, generation, err := encodeModelJSON(updated.Configuration)
	if err != nil {
		return nil, model.ChangeEvent{}, ErrStorage
	}
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_update_model_profile(
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			$12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
		)`,
		updated.TenantID, updated.ProfileID, input.ExpectedVersion, updated.DisplayName,
		updated.Description, updated.SchemaVersion, updated.Configuration.Provider,
		updated.Configuration.Model, updated.Configuration.Endpoint, options,
		nullableText(updated.Configuration.SecretRef), generation, updated.ContentDigest,
		updated.UpdatedAt, event.EventType, event.PreviousStatus, event.CurrentStatus,
		event.PreviousDigest, event.CurrentDigest, event.ActorType, event.ActorID,
		event.Reason, event.CorrelationID,
	).Scan(&eventID)
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	stored, err := scanModelProfile(r.catalog, tx.QueryRowContext(ctx, modelSelect+` WHERE tenant_id = $1 AND profile_id = $2`, input.TenantID, input.ProfileID))
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	committed, err := scanModelEvent(tx.QueryRowContext(ctx, modelEventSelect+` WHERE event_id = $1`, eventID))
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, model.ChangeEvent{}, err
	}
	return stored, committed, nil
}

func (r *ModelRepository) TransitionStatus(ctx context.Context, input model.TransitionStatusInput) (*model.Profile, model.ChangeEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, model.ChangeEvent{}, err
	}
	if r == nil || r.db == nil {
		return nil, model.ChangeEvent{}, ErrStorage
	}
	tx, err := begin(ctx, r.db)
	if err != nil {
		return nil, model.ChangeEvent{}, err
	}
	defer rollback(tx)
	current, err := scanModelProfile(r.catalog, tx.QueryRowContext(ctx, modelSelect+` WHERE tenant_id = $1 AND profile_id = $2 FOR UPDATE`, input.TenantID, input.ProfileID))
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	_, _, err = model.PrepareStatusChange(*current, input, r.catalog, monotonicNow(current.UpdatedAt))
	if err != nil {
		return nil, model.ChangeEvent{}, err
	}
	var eventID int64
	err = tx.QueryRowContext(ctx, `
		SELECT public.control_plane_transition_model_profile_status(
			$1, $2, $3, $4, $5, $6, $7, $8
		)`, input.TenantID, input.ProfileID, input.ExpectedVersion, string(input.NextStatus),
		strings.TrimSpace(input.Metadata.ActorType), strings.TrimSpace(input.Metadata.ActorID),
		strings.TrimSpace(input.Metadata.Reason), strings.TrimSpace(input.Metadata.CorrelationID)).Scan(&eventID)
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	stored, err := scanModelProfile(r.catalog, tx.QueryRowContext(ctx, modelSelect+` WHERE tenant_id = $1 AND profile_id = $2`, input.TenantID, input.ProfileID))
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	committed, err := scanModelEvent(tx.QueryRowContext(ctx, modelEventSelect+` WHERE event_id = $1`, eventID))
	if err != nil {
		return nil, model.ChangeEvent{}, mapDBError(ctx, err, model.ErrNotFound, model.ErrDuplicateKey, model.ErrConflict, model.ErrInvalid)
	}
	if err := commit(ctx, tx); err != nil {
		return nil, model.ChangeEvent{}, err
	}
	return stored, committed, nil
}

const modelSelect = `SELECT tenant_id, profile_id, profile_key, display_name, description,
       status, schema_version, provider, model, endpoint, options, secret_ref,
       generation, content_digest, version, created_at, updated_at
FROM public.model_profile`

func scanModelProfile(catalog *model.ProviderCatalog, row rowScanner) (*model.Profile, error) {
	var value model.Profile
	var status string
	var options, generation []byte
	if err := row.Scan(&value.TenantID, &value.ProfileID, &value.ProfileKey, &value.DisplayName,
		&value.Description, &status, &value.SchemaVersion, &value.Configuration.Provider,
		&value.Configuration.Model, &value.Configuration.Endpoint, &options,
		&value.Configuration.SecretRef, &generation, &value.ContentDigest, &value.Version,
		&value.CreatedAt, &value.UpdatedAt); err != nil {
		return nil, err
	}
	value.Status = model.Status(status)
	if err := decodeModelJSON(options, generation, &value.Configuration); err != nil {
		return nil, ErrStorage
	}
	value.CreatedAt = asUTC(value.CreatedAt)
	value.UpdatedAt = asUTC(value.UpdatedAt)
	if err := value.Validate(catalog); err != nil {
		return nil, ErrStorage
	}
	clone := value.Clone()
	return &clone, nil
}

const modelEventSelect = `SELECT event_type, tenant_id, profile_id,
       previous_status, current_status, previous_digest, current_digest,
       actor_type, actor_id, reason, correlation_id, previous_version,
       next_version, occurred_at
FROM public.model_profile_change_outbox`

func scanModelEvent(row rowScanner) (model.ChangeEvent, error) {
	var event model.ChangeEvent
	var eventType, currentStatus string
	var previousStatus sql.NullString
	var previousDigest sql.NullString
	if err := row.Scan(&eventType, &event.TenantID, &event.ProfileID, &previousStatus,
		&currentStatus, &previousDigest, &event.CurrentDigest, &event.ActorType,
		&event.ActorID, &event.Reason, &event.CorrelationID, &event.PreviousVersion,
		&event.NextVersion, &event.OccurredAt); err != nil {
		return model.ChangeEvent{}, err
	}
	event.EventType = model.EventType(eventType)
	if previousStatus.Valid {
		event.PreviousStatus = model.Status(previousStatus.String)
	}
	event.CurrentStatus = model.Status(currentStatus)
	if previousDigest.Valid {
		event.PreviousDigest = previousDigest.String
	}
	event.OccurredAt = asUTC(event.OccurredAt)
	return event, nil
}

func (r *ModelRepository) readModelEvent(ctx context.Context, eventID int64) (model.ChangeEvent, error) {
	return scanModelEvent(r.db.QueryRowContext(ctx, modelEventSelect+` WHERE event_id = $1`, eventID))
}

func monotonicNow(previous time.Time) time.Time {
	now := time.Now().UTC()
	if now.Before(previous) {
		return previous
	}
	return now
}
