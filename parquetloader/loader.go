package parquetloader

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	arrow "github.com/apache/arrow/go/v15/arrow"
	arrow_array "github.com/apache/arrow/go/v15/arrow/array"
	arrow_memory "github.com/apache/arrow/go/v15/arrow/memory"
	arrow_file "github.com/apache/arrow/go/v15/parquet/file"
	arrow_pqarrow "github.com/apache/arrow/go/v15/parquet/pqarrow"

	"AutoNormDb/arrowtable"
)

// LoadParquetIntoDB loads the given parquet file into an Arrow-backed
// go-mysql-server database, creating the database and table as needed. It
// returns a provider ready to be served via go-mysql-server.
func LoadParquetIntoDB(filePath, dbName, tableName string) (*arrowtable.Provider, error) {
	db := arrowtable.NewDatabase(dbName)
	pro := arrowtable.NewProvider(db)

	if err := loadParquetFileIntoDB(context.Background(), filePath, tableName, db); err != nil {
		return nil, err
	}

	return pro, nil
}

// LoadParquetFilesIntoDB loads multiple parquet files into the same database.
// Each file will become a table named after the file's base name without the
// extension.
func LoadParquetFilesIntoDB(filePaths []string, dbName string) (*arrowtable.Provider, error) {
	if len(filePaths) == 0 {
		return nil, fmt.Errorf("no parquet files provided")
	}

	db := arrowtable.NewDatabase(dbName)
	pro := arrowtable.NewProvider(db)

	ctx := context.Background()
	for _, filePath := range filePaths {
		tableName := strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
		if tableName == "" {
			return nil, fmt.Errorf("could not derive table name from %q", filePath)
		}

		if err := loadParquetFileIntoDB(ctx, filePath, tableName, db); err != nil {
			return nil, err
		}
	}

	return pro, nil
}

func loadParquetFileIntoDB(ctx context.Context, filePath, tableName string, db *arrowtable.Database) error {
	f, err := arrow_file.OpenParquetFile(filePath, false)
	if err != nil {
		return fmt.Errorf("open parquet %q: %w", filePath, err)
	}
	defer f.Close()

	pool := arrow_memory.NewGoAllocator()
	props := arrow_pqarrow.ArrowReadProperties{BatchSize: 4096}
	fr, err := arrow_pqarrow.NewFileReader(f, props, pool)
	if err != nil {
		return fmt.Errorf("new pqarrow reader for %q: %w", filePath, err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return fmt.Errorf("get record reader for %q: %w", filePath, err)
	}
	defer rr.Release()

	schema := rr.Schema()
	var records []arrow.Record
	totalRows := int64(0)
	batches := 0

	for rr.Next() {
		rec := rr.Record()
		if rec == nil {
			continue
		}

		rec.Retain()
		records = append(records, rec)
		totalRows += int64(rec.NumRows())
		batches++
		rec.Release()
	}

	arrTable := arrow_array.NewTableFromRecords(schema, records)
	for _, rec := range records {
		rec.Release()
	}
	defer arrTable.Release()

	arrowTbl, err := arrowtable.NewArrowBackedTable(tableName, arrTable)
	if err != nil {
		return fmt.Errorf("wrap arrow table for %q: %w", filePath, err)
	}

	if err := db.AddTable(arrowTbl); err != nil {
		return fmt.Errorf("register table %s: %w", tableName, err)
	}

	log.Printf("batches=%d rows=%d for table %s from %s", batches, totalRows, tableName, filePath)
	return nil
}
