package parquettable

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strings"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/compute"
	arrowmemory "github.com/apache/arrow/go/v15/arrow/memory"
	arrowfile "github.com/apache/arrow/go/v15/parquet/file"
	arrowpqarrow "github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"AutoNormDb/engine/arrowbackend"
	"AutoNormDb/table/arrowtable"
)

// ParquetBackedTable exposes a Parquet file as a go-mysql-server table. Row
// groups are mapped to partitions so that the engine can parallelise reads
// across large datasets. The table honours column projections so that only the
// required columns are decoded from the underlying file.
type ParquetBackedTable struct {
	name         string
	path         string
	baseSchema   sql.Schema
	schema       sql.Schema
	columnLookup map[string]int
	projection   []int
	pushed       []sql.Expression
	numRowGroups int
}

var _ sql.FilteredTable = (*ParquetBackedTable)(nil)

// NewParquetBackedTable opens the provided Parquet file to build the SQL schema
// and discover the number of available row groups.
func NewParquetBackedTable(name, path string) (*ParquetBackedTable, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve parquet path %q: %w", path, err)
	}

	rdr, err := arrowfile.OpenParquetFile(abs, false)
	if err != nil {
		return nil, fmt.Errorf("open parquet file %q: %w", abs, err)
	}
	defer rdr.Close()

	allocator := arrowmemory.NewGoAllocator()
	props := arrowpqarrow.ArrowReadProperties{BatchSize: 4096}
	fr, err := arrowpqarrow.NewFileReader(rdr, props, allocator)
	if err != nil {
		return nil, fmt.Errorf("construct pqarrow reader for %q: %w", abs, err)
	}

	arrSchema, err := fr.Schema()
	if err != nil {
		return nil, fmt.Errorf("derive arrow schema for %q: %w", abs, err)
	}

	sqlSchema, err := arrowtable.ArrowSchemaToSQLSchema(arrSchema, name)
	if err != nil {
		return nil, fmt.Errorf("convert schema for %q: %w", abs, err)
	}

	base := make(sql.Schema, len(sqlSchema))
	copy(base, sqlSchema)

	lookup := make(map[string]int, len(base))
	for i, col := range base {
		lookup[strings.ToLower(col.Name)] = i
	}

	numRowGroups := rdr.NumRowGroups()

	return &ParquetBackedTable{
		name:         name,
		path:         abs,
		baseSchema:   base,
		schema:       base,
		columnLookup: lookup,
		numRowGroups: numRowGroups,
	}, nil
}

// Name implements sql.Table.
func (t *ParquetBackedTable) Name() string { return t.name }

// String implements fmt.Stringer for debugging convenience.
func (t *ParquetBackedTable) String() string { return t.name }

// Schema implements sql.Table.
func (t *ParquetBackedTable) Schema() sql.Schema { return t.schema }

// Collation implements sql.Table by returning the default MySQL collation.
func (t *ParquetBackedTable) Collation() sql.CollationID { return sql.Collation_Default }

// Filters returns the filter expressions that will be pushed down into Arrow
// compute. A nil slice signals that no pushdown is configured.
func (t *ParquetBackedTable) Filters() []sql.Expression {
	if len(t.pushed) == 0 {
		return nil
	}
	out := make([]sql.Expression, len(t.pushed))
	copy(out, t.pushed)
	return out
}

// HandledFilters reports the subset of provided filters that this table can
// evaluate with Arrow compute kernels.
func (t *ParquetBackedTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	var handled []sql.Expression
	for _, f := range filters {
		if arrowbackend.CanHandleForCompute(f) {
			handled = append(handled, f)
		}
	}
	return handled
}

// WithFilters returns a shallow copy of the table that records the supported
// filters requested by go-mysql-server. Non-pushable filters are evaluated by
// the engine after rows are materialised.
func (t *ParquetBackedTable) WithFilters(ctx *sql.Context, filters []sql.Expression) sql.Table {
	_ = ctx
	nt := *t
	nt.pushed = t.HandledFilters(filters)
	return &nt
}

type rowGroupPartition struct {
	index int
	all   bool
}

func (p *rowGroupPartition) Key() []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(p.index))
	return buf
}

// Partitions implements sql.PartitionedTable. Each row group becomes a distinct
// partition so that go-mysql-server can evaluate them independently.
func (t *ParquetBackedTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	if t.numRowGroups == 0 {
		return sql.PartitionsToPartitionIter(&rowGroupPartition{index: 0, all: true}), nil
	}

	parts := make([]sql.Partition, t.numRowGroups)
	for i := 0; i < t.numRowGroups; i++ {
		parts[i] = &rowGroupPartition{index: i}
	}
	return sql.PartitionsToPartitionIter(parts...), nil
}

// PartitionRows implements sql.PartitionedTable. The iterator streams Arrow
// record batches directly from the Parquet file and converts them to sql.Row on
// demand.
func (t *ParquetBackedTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	rp, ok := p.(*rowGroupPartition)
	if !ok {
		return nil, fmt.Errorf("unexpected partition type %T", p)
	}

	goCtx := context.Background()
	if ctx != nil {
		goCtx = ctx
	}

	rdr, err := arrowfile.OpenParquetFile(t.path, false)
	if err != nil {
		return nil, fmt.Errorf("open parquet file %q: %w", t.path, err)
	}

	allocator := arrowmemory.NewGoAllocator()
	props := arrowpqarrow.ArrowReadProperties{BatchSize: 4096}
	fr, err := arrowpqarrow.NewFileReader(rdr, props, allocator)
	if err != nil {
		rdr.Close()
		return nil, fmt.Errorf("construct pqarrow reader for %q: %w", t.path, err)
	}

	var cols []int
	if len(t.projection) > 0 {
		cols = append(cols, t.projection...)
	}

	var rowGroups []int
	if !rp.all {
		rowGroups = []int{rp.index}
	}

	rr, err := fr.GetRecordReader(goCtx, cols, rowGroups)
	if err != nil {
		rdr.Close()
		return nil, fmt.Errorf("create record reader for %q: %w", t.path, err)
	}

	return &parquetRowIter{reader: rr, file: rdr, filters: t.Filters()}, nil
}

// WithProjections implements sql.ProjectedTable. The returned copy remembers
// which columns were requested by go-mysql-server so that the Parquet reader can
// avoid decoding unnecessary data.
func (t *ParquetBackedTable) WithProjections(_ *sql.Context, columns []string) sql.Table {
	if len(columns) == 0 {
		nt := *t
		nt.schema = t.baseSchema
		nt.projection = nil
		return &nt
	}

	indices := make([]int, 0, len(columns))
	projected := make(sql.Schema, 0, len(columns))
	for _, col := range columns {
		idx, ok := t.columnLookup[strings.ToLower(col)]
		if !ok {
			continue
		}
		indices = append(indices, idx)
		projected = append(projected, t.baseSchema[idx])
	}

	if len(indices) == 0 {
		nt := *t
		nt.schema = t.baseSchema
		nt.projection = nil
		return &nt
	}

	nt := *t
	nt.projection = indices
	nt.schema = projected
	return &nt
}

type parquetRowIter struct {
	reader  arrowpqarrow.RecordReader
	file    *arrowfile.Reader
	filters []sql.Expression

	current arrow.Record
	row     int
}

func (it *parquetRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if it.current != nil && it.row < int(it.current.NumRows()) {
			return it.extractRow()
		}

		if it.current != nil {
			it.current.Release()
			it.current = nil
		}

		if !it.reader.Next() {
			if err := it.reader.Err(); err != nil && err != io.EOF {
				return nil, err
			}
			return nil, io.EOF
		}

		rec := it.reader.Record()
		if rec == nil || rec.NumRows() == 0 {
			continue
		}
		rec.Retain()

		if len(it.filters) > 0 && rec.NumCols() > 0 {
			filtered, err := it.applyFilters(ctx, rec)
			rec.Release()
			if err != nil {
				return nil, err
			}
			if filtered == nil || filtered.NumRows() == 0 {
				if filtered != nil {
					filtered.Release()
				}
				continue
			}
			rec = filtered
		}

		it.current = rec
		it.row = 0
	}
}

func (it *parquetRowIter) applyFilters(ctx *sql.Context, rec arrow.Record) (arrow.Record, error) {
	goCtx := context.Background()
	if ctx != nil {
		goCtx = ctx
	}

	mask, err := arrowbackend.BuildMaskForBatch(goCtx, rec, it.filters)
	if err != nil {
		return nil, err
	}
	defer mask.Release()

	opts := compute.FilterOptions{NullSelection: compute.SelectionDropNulls}
	filtered, err := compute.FilterRecordBatch(goCtx, rec, mask, &opts)
	if err != nil {
		return nil, err
	}
	return filtered, nil
}

func (it *parquetRowIter) extractRow() (sql.Row, error) {
	cols := int(it.current.NumCols())
	row := make(sql.Row, cols)
	for i := 0; i < cols; i++ {
		col := it.current.Column(i)
		if col.IsNull(it.row) {
			row[i] = nil
			continue
		}
		row[i] = valueAt(col, it.row)
	}
	it.row++
	return row, nil
}

func (it *parquetRowIter) Close(*sql.Context) error {
	if it.current != nil {
		it.current.Release()
		it.current = nil
	}
	if it.reader != nil {
		it.reader.Release()
		it.reader = nil
	}
	if it.file != nil {
		_ = it.file.Close()
		it.file = nil
	}
	return nil
}

func valueAt(col arrow.Array, idx int) any {
	switch arr := col.(type) {
	case *array.Int8:
		return arr.Value(idx)
	case *array.Int16:
		return arr.Value(idx)
	case *array.Int32:
		return arr.Value(idx)
	case *array.Int64:
		return arr.Value(idx)
	case *array.Uint8:
		return int64(arr.Value(idx))
	case *array.Uint16:
		return int64(arr.Value(idx))
	case *array.Uint32:
		return int64(arr.Value(idx))
	case *array.Uint64:
		v := arr.Value(idx)
		if v > math.MaxInt64 {
			return int64(math.MaxInt64)
		}
		return int64(v)
	case *array.Float32:
		return arr.Value(idx)
	case *array.Float64:
		return arr.Value(idx)
	case *array.Boolean:
		return arr.Value(idx)
	case *array.String:
		return arr.Value(idx)
	case *array.LargeString:
		return arr.Value(idx)
	case *array.Binary:
		return string(arr.Value(idx))
	case *array.LargeBinary:
		return string(arr.Value(idx))
	case *array.FixedSizeBinary:
		return string(arr.Value(idx))
	case *array.Timestamp:
		return int64(arr.Value(idx))
	case *array.Date32:
		return int32(arr.Value(idx))
	case *array.Date64:
		return int64(arr.Value(idx))
	case *array.Decimal128:
		if dt, ok := arr.DataType().(*arrow.Decimal128Type); ok {
			num := arr.Value(idx)
			return decimal.NewFromBigInt(num.BigInt(), -dt.Scale)
		}
		return nil
	case *array.Decimal256:
		if dt, ok := arr.DataType().(*arrow.Decimal256Type); ok {
			num := arr.Value(idx)
			return decimal.NewFromBigInt(num.BigInt(), -dt.Scale)
		}
		return nil
	default:
		return nil
	}
}
