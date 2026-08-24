// Package postgres exposes the PostgreSQL implementation of the Agent App
// repository at the Agent domain boundary.
package postgres

import (
	"database/sql"

	"github.com/XnLemon/trpc-agent-service/trpcservice/agent"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

// Repository is the PostgreSQL-backed Agent App repository.
type Repository = storagepostgres.AgentRepository

var _ agent.Repository = (*Repository)(nil)

// NewRepository creates an Agent App repository over a PostgreSQL pool.
func NewRepository(db *sql.DB) *Repository {
	return storagepostgres.NewAgentRepository(db)
}
