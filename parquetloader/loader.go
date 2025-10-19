package parquetloader

import (
	"context"
	"fmt"
	"log"
	"time"

	arrow "github.com/apache/arrow/go/v15/arrow"
	arrow_array "github.com/apache/arrow/go/v15/arrow/array"
	arrow_memory "github.com/apache/arrow/go/v15/arrow/memory"
	arrow_file "github.com/apache/arrow/go/v15/parquet/file"
	arrow_pqarrow "github.com/apache/arrow/go/v15/parquet/pqarrow"

	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// LoadParquetIntoDB loads the given parquet file into an in-memory go-mysql-server
// database, creating the database and table as needed. It returns a DbProvider
// ready to be served via go-mysql-server.
func LoadParquetIntoDB(filePath, dbName, tableName string) (*memory.DbProvider, error) {
	f, err := arrow_file.OpenParquetFile(filePath, false)
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}
	defer f.Close()

	ctx := context.Background()
	pool := arrow_memory.NewGoAllocator()
	props := arrow_pqarrow.ArrowReadProperties{BatchSize: 4096}
	fr, err := arrow_pqarrow.NewFileReader(f, props, pool)
	if err != nil {
		return nil, fmt.Errorf("new pqarrow reader: %w", err)
	}

	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("get record reader: %w", err)
	}
	defer rr.Release()

	arSchema := rr.Schema()
	sqlSchema, err := makeSQLSchemaFromArrow(arSchema, tableName)
	if err != nil {
		return nil, fmt.Errorf("make sql schema: %w", err)
	}

	db := memory.NewDatabase(dbName)
	db.BaseDatabase.EnablePrimaryKeyIndexes()
	pro := memory.NewDBProvider(db)

	pkSchema := sql.NewPrimaryKeySchema(sqlSchema)
	tbl := memory.NewTable(db, tableName, pkSchema, db.GetForeignKeyCollection())
	db.AddTable(tableName, tbl)

	sess := memory.NewSession(sql.NewBaseSession(), pro)
	qctx := sql.NewContext(ctx, sql.WithSession(sess))

	inserted := 0
	batch := 0
	for rr.Next() {
		rec := rr.Record()
		if rec == nil {
			continue
		}
		rec.Retain()
		nRows := int(rec.NumRows())
		nCols := int(rec.NumCols())

		if nRows == 0 || nCols == 0 {
			rec.Release()
			batch++
			continue
		}

		for r := 0; r < nRows; r++ {
			rowVals := make([]interface{}, nCols)
			for c := 0; c < nCols; c++ {
				col := rec.Column(c)
				val, convErr := valueAt(col, arSchema.Field(c), r)
				if convErr != nil {
					rec.Release()
					return nil, fmt.Errorf("convert value col=%d row=%d: %w", c, r, convErr)
				}
				rowVals[c] = val
			}
			if err := tbl.Insert(qctx, sql.NewRow(rowVals...)); err != nil {
				rec.Release()
				return nil, fmt.Errorf("insert row %d: %w", r, err)
			}
			inserted++
		}

		rec.Release()
		batch++
	}

	log.Printf("batches=%d inserted=%d", batch, inserted)
	log.Printf("Imported %d rows into %s.%s", inserted, dbName, tableName)

	return pro, nil
}

func makeSQLSchemaFromArrow(s *arrow.Schema, tableName string) (sql.Schema, error) {
	out := make(sql.Schema, len(s.Fields()))
	for i, f := range s.Fields() {
		tp, err := arrowTypeToSQLType(f.Type)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", f.Name, err)
		}
		out[i] = &sql.Column{
			Name:     f.Name,
			Type:     tp,
			Nullable: f.Nullable,
			Source:   tableName,
		}
	}
	return out, nil
}

func arrowTypeToSQLType(dt arrow.DataType) (sql.Type, error) {
	switch t := dt.(type) {
	case *arrow.StringType, *arrow.LargeStringType:
		return types.Text, nil
	case *arrow.BooleanType:
		return types.Boolean, nil
	case *arrow.Int32Type:
		return types.Int32, nil
	case *arrow.Int64Type:
		return types.Int64, nil
	case *arrow.Float64Type:
		return types.Float64, nil
	case *arrow.TimestampType:
		return types.MustCreateDatetimeType(query.Type_DATETIME, 6), nil
	default:
		return nil, fmt.Errorf("unsupported arrow type: %s", t)
	}
}

func valueAt(col arrow.Array, field arrow.Field, row int) (interface{}, error) {
	if col.IsNull(row) {
		return nil, nil
	}
	switch arr := col.(type) {
	case *arrow_array.String:
		return arr.Value(row), nil
	case *arrow_array.LargeString:
		return arr.Value(row), nil
	case *arrow_array.Boolean:
		return arr.Value(row), nil
	case *arrow_array.Int32:
		return arr.Value(row), nil
	case *arrow_array.Int64:
		return arr.Value(row), nil
	case *arrow_array.Float64:
		return arr.Value(row), nil
	case *arrow_array.Timestamp:
		tsType := field.Type.(*arrow.TimestampType)
		iv := arr.Value(row)
		return arrowTimestampToTime(int64(iv), tsType), nil
	default:
		return nil, fmt.Errorf("unsupported array kind: %T", col)
	}
}

func arrowTimestampToTime(v int64, t *arrow.TimestampType) time.Time {
	switch t.Unit {
	case arrow.Second:
		return time.Unix(v, 0).UTC()
	case arrow.Millisecond:
		sec := v / 1_000
		nsec := (v % 1_000) * int64(time.Millisecond)
		return time.Unix(sec, nsec).UTC()
	case arrow.Microsecond:
		sec := v / 1_000_000
		nsec := (v % 1_000_000) * int64(time.Microsecond)
		return time.Unix(sec, nsec).UTC()
	case arrow.Nanosecond:
		sec := v / 1_000_000_000
		nsec := v % 1_000_000_000
		return time.Unix(sec, nsec).UTC()
	default:
		return time.Unix(v, 0).UTC()
	}
}
