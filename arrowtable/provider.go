package arrowtable

import (
	"AutoNormDb/database"
	"github.com/dolthub/go-mysql-server/sql"
)

// Provider aliases the shared database provider so existing arrowtable callers
// continue to compile.
type Provider = database.Provider

// NewProvider forwards to the shared provider constructor.
func NewProvider(dbs ...sql.Database) *Provider {
	return database.NewProvider(dbs...)
}
