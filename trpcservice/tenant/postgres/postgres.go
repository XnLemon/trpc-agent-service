// Package postgres exposes the PostgreSQL implementation of the Tenant
// repository at the Tenant domain boundary.
package postgres

import (
	"database/sql"

	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

// Repository is the PostgreSQL-backed Tenant repository.
type Repository = storagepostgres.TenantRepository

var _ tenant.Repository = (*Repository)(nil)

// NewRepository creates a Tenant repository over a PostgreSQL pool.
func NewRepository(db *sql.DB) *Repository {
	return storagepostgres.NewTenantRepository(db)
}
