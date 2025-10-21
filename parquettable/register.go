package parquettable

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

// RegisterParquetTable reads the Parquet file at path and registers it as
// tableName within dbName on the supplied memory.DbProvider.
func RegisterParquetTable(prov *memory.DbProvider, dbName, tableName, path string) error {
	if prov == nil {
		return fmt.Errorf("nil provider")
	}

	ctx := sql.NewEmptyContext()
	db, err := prov.Database(ctx, dbName)
	if err != nil {
		return fmt.Errorf("lookup database %s: %w", dbName, err)
	}

	parquetDB, ok := db.(*Database)
	if !ok {
		return fmt.Errorf("database %s does not support Parquet registration", dbName)
	}

	tbl, err := NewParquetBackedTable(tableName, path)
	if err != nil {
		return fmt.Errorf("create parquet table for %s: %w", tableName, err)
	}

	if err := parquetDB.AddTable(tbl); err != nil {
		return fmt.Errorf("register table %s: %w", tableName, err)
	}

	return nil
}
