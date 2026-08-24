// Package postgres exposes the PostgreSQL implementation of the Backend
// Profile repository at the Backend domain boundary.
package postgres

import (
	"database/sql"

	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

// Repository is the PostgreSQL-backed Backend Profile repository.
type Repository = storagepostgres.BackendRepository

var _ backend.Repository = (*Repository)(nil)

// NewRepository creates a Backend Profile repository over a PostgreSQL pool.
func NewRepository(db *sql.DB, catalog *backend.ProviderCatalog) *Repository {
	return storagepostgres.NewBackendRepository(db, catalog)
}
