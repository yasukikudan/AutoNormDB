package arrowtable

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/ipc"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

// ArrowTable represents a logical table backed by multiple Arrow IPC chunk files.
type ArrowTable struct {
	name   string
	schema sql.Schema
	chunks []string
}

// NewArrowTable constructs a new ArrowTable using the provided chunk directory.
func NewArrowTable(name string, schema sql.Schema, chunkDir string) (*ArrowTable, error) {
	paths, err := filepath.Glob(filepath.Join(chunkDir, "*.arrow"))
	if err != nil {
		return nil, err
	}
	return &ArrowTable{name: name, schema: schema, chunks: paths}, nil
}

// Name implements sql.Table.
func (t *ArrowTable) Name() string { return t.name }

// String implements fmt.Stringer.
func (t *ArrowTable) String() string { return t.name }

// Schema implements sql.Table.
func (t *ArrowTable) Schema() sql.Schema { return t.schema }

// chunkPartition represents a single Arrow chunk file.
type chunkPartition struct{ path string }

// Key implements sql.Partition.
func (p *chunkPartition) Key() []byte { return []byte(p.path) }

// partIter iterates partitions for ArrowTable.
type partIter struct {
	paths []string
	idx   int
}

// Next implements sql.PartitionIter.
func (it *partIter) Next(*sql.Context) (sql.Partition, error) {
	if it.idx >= len(it.paths) {
		return nil, io.EOF
	}
	p := &chunkPartition{path: it.paths[it.idx]}
	it.idx++
	return p, nil
}

// Close implements sql.PartitionIter.
func (it *partIter) Close(*sql.Context) error { return nil }

// Partitions implements sql.Table.
func (t *ArrowTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &partIter{paths: t.chunks}, nil
}

// rowIter iterates rows from a single chunk.
type rowIter struct {
	rec    array.Record
	row    int64
	closed bool
}

// Next implements sql.RowIter.
func (it *rowIter) Next(*sql.Context) (sql.Row, error) {
	if it.closed || it.row >= it.rec.NumRows() {
		return nil, io.EOF
	}

	row := make(sql.Row, it.rec.NumCols())
	for c := 0; c < int(it.rec.NumCols()); c++ {
		col := it.rec.Column(c)
		row[c] = valueAt(col, int(it.row))
	}
	it.row++
	return row, nil
}

// Close implements sql.RowIter.
func (it *rowIter) Close(*sql.Context) error {
	if !it.closed {
		it.closed = true
		if it.rec != nil {
			it.rec.Release()
		}
	}
	return nil
}

// PartitionRows implements sql.Table.
func (t *ArrowTable) PartitionRows(ctx *sql.Context, part sql.Partition) (sql.RowIter, error) {
	cp := part.(*chunkPartition)
	rec, err := readArrowRecord(cp.path)
	if err != nil {
		return nil, err
	}
	return &rowIter{rec: rec}, nil
}

// readArrowRecord reads an IPC file and concatenates record batches into one record.
func readArrowRecord(path string) (array.Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	rdr, err := ipc.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	defer func() {
		rdr.Release()
		_ = f.Close()
	}()

	var rec array.Record
	for rdr.Next() {
		batch := rdr.Record()
		batch.Retain()
		if rec == nil {
			rec = batch
			continue
		}

		joined := concatRecords(rec, batch)
		rec.Release()
		batch.Release()
		rec = joined
	}

	if rec == nil {
		return nil, fmt.Errorf("empty arrow record: %s", path)
	}

	return rec, nil
}

// concatRecords concatenates two records vertically (row-wise).
func concatRecords(a, b array.Record) array.Record {
	if a.NumCols() != b.NumCols() {
		panic("concatRecords: column count mismatch")
	}

	cols := make([]arrow.Array, a.NumCols())
	for i := range cols {
		cols[i] = concatArrays(a.Column(i), b.Column(i))
	}

	rec := array.NewRecord(a.Schema(), cols, a.NumRows()+b.NumRows())
	for _, col := range cols {
		col.Release()
	}
	return rec
}

// concatArrays concatenates supported Arrow array types.
func concatArrays(x, y arrow.Array) arrow.Array {
	switch a := x.(type) {
	case *array.Int64:
		b := y.(*array.Int64)
		bld := array.NewInt64Builder(memory.DefaultAllocator)
		defer bld.Release()
		appendInt64Array(bld, a)
		appendInt64Array(bld, b)
		return bld.NewArray()
	case *array.Float64:
		b := y.(*array.Float64)
		bld := array.NewFloat64Builder(memory.DefaultAllocator)
		defer bld.Release()
		appendFloat64Array(bld, a)
		appendFloat64Array(bld, b)
		return bld.NewArray()
	case *array.String:
		b := y.(*array.String)
		bld := array.NewStringBuilder(memory.DefaultAllocator)
		defer bld.Release()
		appendStringArray(bld, a)
		appendStringArray(bld, b)
		return bld.NewArray()
	case *array.Boolean:
		b := y.(*array.Boolean)
		bld := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer bld.Release()
		appendBoolArray(bld, a)
		appendBoolArray(bld, b)
		return bld.NewArray()
	default:
		panic("concatArrays: unsupported type")
	}
}

func appendInt64Array(bld *array.Int64Builder, arr *array.Int64) {
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			bld.AppendNull()
			continue
		}
		bld.Append(arr.Value(i))
	}
}

func appendFloat64Array(bld *array.Float64Builder, arr *array.Float64) {
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			bld.AppendNull()
			continue
		}
		bld.Append(arr.Value(i))
	}
}

func appendStringArray(bld *array.StringBuilder, arr *array.String) {
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			bld.AppendNull()
			continue
		}
		bld.Append(arr.Value(i))
	}
}

func appendBoolArray(bld *array.BooleanBuilder, arr *array.Boolean) {
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			bld.AppendNull()
			continue
		}
		bld.Append(arr.Value(i))
	}
}

func valueAt(col arrow.Array, i int) any {
	if col.IsNull(i) {
		return nil
	}

	switch arr := col.(type) {
	case *array.Int64:
		return arr.Value(i)
	case *array.Float64:
		return arr.Value(i)
	case *array.String:
		return arr.Value(i)
	case *array.Boolean:
		return arr.Value(i)
	default:
		return nil
	}
}
