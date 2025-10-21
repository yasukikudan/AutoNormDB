package parquetloader

// このパッケージは Parquet ファイルを go-mysql-server から直接参照可能な
// ParquetBackedTable として登録するローダー処理を提供します。Parquet を Arrow
// テーブルに展開することなく、ファイルをそのまま SQL テーブルとして公開することが
// できるため、大きなデータセットでも効率的に扱えます。

import (
	"fmt"
	"path/filepath"
	"strings"

	"AutoNormDb/database"
	"AutoNormDb/parquettable"
)

// LoadParquetIntoDB registers the given Parquet file as a table within a new
// database and returns a provider exposing that database.
func LoadParquetIntoDB(filePath, dbName, tableName string) (*database.Provider, error) {
	db := database.NewDatabase(dbName)
	prov := database.NewProvider(db)

	if err := registerParquetFile(filePath, tableName, db); err != nil {
		return nil, err
	}

	return prov, nil
}

// LoadParquetFilesIntoDB registers multiple Parquet files as tables within the
// same database. Each file becomes a table whose name is derived from the file
// name without its extension.
func LoadParquetFilesIntoDB(filePaths []string, dbName string) (*database.Provider, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no parquet files provided")
	}

	db := database.NewDatabase(dbName)
	prov := database.NewProvider(db)

	for _, filePath := range filePaths {
		tableName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
		if tableName == "" {
			return nil, fmt.Errorf("could not derive table name from %q", filePath)
		}

		if err := registerParquetFile(filePath, tableName, db); err != nil {
			return nil, err
		}
	}

	return prov, nil
}

func registerParquetFile(filePath, tableName string, db *database.Database) error {
	tbl, err := parquettable.NewParquetBackedTable(tableName, filePath)
	if err != nil {
		return fmt.Errorf("create parquet table for %s: %w", tableName, err)
	}

	if err := db.AddTable(tbl); err != nil {
		return fmt.Errorf("register table %s: %w", tableName, err)
	}

	return nil
}
