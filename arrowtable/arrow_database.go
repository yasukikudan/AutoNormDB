package arrowtable

import "AutoNormDb/database"

// Database is retained for backwards compatibility with existing Arrow table
// callers. It simply aliases the shared database implementation so that Arrow
// backed tables can coexist with other table types.
type Database = database.Database

// NewDatabase constructs a database that can store Arrow-backed tables alongside
// other sql.Table implementations.
func NewDatabase(name string) *Database {
	return database.NewDatabase(name)
}
