package postgres

import storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"

var (
	ErrStorage = storagepostgres.ErrStorage
	begin      = storagepostgres.Begin
	rollback   = storagepostgres.Rollback
	commit     = storagepostgres.Commit
	mapDBError = storagepostgres.MapError
)

type rowScanner = storagepostgres.RowScanner
