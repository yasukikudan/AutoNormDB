package parquettable

import "AutoNormDb/database"

// Database re-exports the shared implementation so that Parquet-backed tables
// can be mixed with other table types inside the same provider.
type Database = database.Database

// NewDatabase constructs a database capable of storing Parquet-backed tables.
func NewDatabase(name string) *Database {
	return database.NewDatabase(name)
}
