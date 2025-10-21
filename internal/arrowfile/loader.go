package arrowfile

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/pqarrow"
)

// LoadArrowTable loads one or more Arrow-backed files into a single Arrow table.
// Supported formats are Parquet (.parquet) and Feather/Arrow IPC (.feather,
// .arrow). When multiple files are supplied their schemas must match exactly.
func LoadArrowTable(paths []string, pool memory.Allocator) (arrow.Table, error) {
	if len(paths) == 0 {
		return nil, fmt.Errorf("no input paths provided")
	}

	tables := make([]arrow.Table, 0, len(paths))
	for _, p := range paths {
		tbl, err := loadSinglePath(p, pool)
		if err != nil {
			releaseTables(tables)
			return nil, err
		}
		tables = append(tables, tbl)
	}

	if len(tables) == 1 {
		return tables[0], nil
	}

	combined, err := concatTables(tables)
	releaseTables(tables)
	if err != nil {
		return nil, err
	}
	return combined, nil
}

func loadSinglePath(path string, pool memory.Allocator) (arrow.Table, error) {
	ext := strings.ToLower(filepath.Ext(path))
	switch ext {
	case ".parquet":
		return loadParquet(path, pool)
	case ".feather", ".arrow":
		return loadFeather(path, pool)
	default:
		return nil, fmt.Errorf("unsupported file type: %s (allowed: .parquet, .feather, .arrow)", ext)
	}
}

func loadParquet(path string, pool memory.Allocator) (arrow.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet: %w", err)
	}
	defer reader.Close()

	props := pqarrow.ArrowReadProperties{}
	pqReader, err := pqarrow.NewFileReader(reader, props, pool)
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet: %w", err)
	}

	table, err := pqReader.ReadTable(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to read parquet: %w", err)
	}
	return table, nil
}

func loadFeather(path string, pool memory.Allocator) (arrow.Table, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open feather file: %w", err)
	}
	defer f.Close()

	reader, err := ipc.NewFileReader(f)
	if err != nil {
		return nil, fmt.Errorf("failed to read feather: %w", err)
	}
	defer reader.Release()

	schema := reader.Schema()
	if schema == nil {
		return nil, fmt.Errorf("failed to read feather: empty schema")
	}

	var records []arrow.Record
	for reader.Next() {
		rec := reader.Record()
		rec.Retain()
		records = append(records, rec)
	}
	if err := reader.Err(); err != nil {
		releaseRecords(records)
		return nil, fmt.Errorf("failed to read feather: %w", err)
	}

	table := array.NewTableFromRecords(schema, records)
	releaseRecords(records)
	return table, nil
}

func concatTables(tables []arrow.Table) (arrow.Table, error) {
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables to concatenate")
	}

	schema := tables[0].Schema()
	for i := 1; i < len(tables); i++ {
		if !arrow.SchemaEqual(schema, tables[i].Schema()) {
			return nil, fmt.Errorf("incompatible schemas across files")
		}
	}

	var records []arrow.Record
	for _, tbl := range tables {
		reader, err := array.NewTableReader(tbl, math.MaxInt32)
		if err != nil {
			releaseRecords(records)
			return nil, err
		}
		for reader.Next() {
			rec := reader.Record()
			rec.Retain()
			records = append(records, rec)
		}
		if err := reader.Err(); err != nil {
			reader.Release()
			releaseRecords(records)
			return nil, err
		}
		reader.Release()
	}

	result := array.NewTableFromRecords(schema, records)
	releaseRecords(records)
	return result, nil
}

func releaseTables(tables []arrow.Table) {
	for _, tbl := range tables {
		if tbl != nil {
			tbl.Release()
		}
	}
}

func releaseRecords(records []arrow.Record) {
	for _, rec := range records {
		if rec != nil {
			rec.Release()
		}
	}
}
