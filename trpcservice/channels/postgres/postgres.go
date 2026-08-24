// Package postgres exposes the PostgreSQL implementation of the Channel
// Binding repository at the Channels domain boundary.
package postgres

import (
	"database/sql"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

// Repository is the PostgreSQL-backed Channel Binding repository and candidate
// capability index.
type Repository = storagepostgres.ChannelRepository

var _ channels.Repository = (*Repository)(nil)
var _ channels.CandidateIndex = (*Repository)(nil)
var _ channels.CandidateConsumer = (*Repository)(nil)

// NewRepository creates a Channel Binding repository over a PostgreSQL pool.
func NewRepository(db *sql.DB) *Repository {
	return storagepostgres.NewChannelRepository(db)
}
