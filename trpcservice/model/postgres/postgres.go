// Package postgres exposes the PostgreSQL implementation of the Model Profile
// repository at the Model domain boundary.
package postgres

import (
	"database/sql"

	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

// Repository is the PostgreSQL-backed Model Profile repository.
type Repository = storagepostgres.ModelRepository

var _ model.Repository = (*Repository)(nil)

// NewRepository creates a Model Profile repository over a PostgreSQL pool.
func NewRepository(db *sql.DB, catalog *model.ProviderCatalog) *Repository {
	return storagepostgres.NewModelRepository(db, catalog)
}
