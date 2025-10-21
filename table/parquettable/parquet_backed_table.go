package parquettable

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/compute"
	arrowmemory "github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/apache/arrow/go/v15/parquet"
	arrowfile "github.com/apache/arrow/go/v15/parquet/file"
	"github.com/apache/arrow/go/v15/parquet/metadata"
	arrowpqarrow "github.com/apache/arrow/go/v15/parquet/pqarrow"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"

	"AutoNormDb/engine/arrowbackend"
	"AutoNormDb/table/arrowtable"
)

const defaultParquetBatchSize int64 = 1 << 16 // 65536 rows per batch for higher throughput

func parquetBatchSize() int64 {
	if env := os.Getenv("AUTONORM_PARQUET_BATCH"); env != "" {
		if v, err := strconv.Atoi(env); err == nil && v > 0 {
			return int64(v)
		}
	}
	return defaultParquetBatchSize
}

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
	props := arrowpqarrow.ArrowReadProperties{BatchSize: parquetBatchSize()}
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

	filters := t.Filters()
	constraints, unsat := buildColumnConstraints(filters)
	if unsat {
		return sql.RowsToRowIter(), nil
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
	props := arrowpqarrow.ArrowReadProperties{BatchSize: parquetBatchSize()}
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
	if len(constraints) > 0 && rdr.MetaData() != nil {
		var candidates []int
		if rp.all {
			total := len(rdr.MetaData().GetRowGroups())
			if total > 0 {
				candidates = make([]int, total)
				for i := range candidates {
					candidates[i] = i
				}
			}
		} else {
			candidates = []int{rp.index}
		}
		if len(candidates) > 0 {
			pruned := pruneRowGroups(rdr.MetaData(), t.columnLookup, candidates, constraints)
			if len(pruned) == 0 {
				rdr.Close()
				return sql.RowsToRowIter(), nil
			}
			rowGroups = pruned
		} else if !rp.all {
			rowGroups = []int{rp.index}
		}
	} else if !rp.all {
		rowGroups = []int{rp.index}
	}

	rr, err := fr.GetRecordReader(goCtx, cols, rowGroups)
	if err != nil {
		rdr.Close()
		return nil, fmt.Errorf("create record reader for %q: %w", t.path, err)
	}

	return &parquetRowIter{reader: rr, file: rdr, filters: filters}, nil
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

	current sql.RowIter
}

func (it *parquetRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	for {
		if it.current != nil {
			row, err := it.current.Next(ctx)
			if err == nil {
				return row, nil
			}
			if errors.Is(err, io.EOF) {
				_ = it.current.Close(ctx)
				it.current = nil
				continue
			}
			return nil, err
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

		iter := arrowtable.NewArrowRowIterFromRecord(rec)
		rec.Release()
		it.current = iter
	}
}

func (it *parquetRowIter) Close(ctx *sql.Context) error {
	var err error
	if it.current != nil {
		if cerr := it.current.Close(ctx); err == nil {
			err = cerr
		}
		it.current = nil
	}
	if it.reader != nil {
		it.reader.Release()
		it.reader = nil
	}
	if it.file != nil {
		if ferr := it.file.Close(); err == nil {
			err = ferr
		}
		it.file = nil
	}
	return err
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

type compareOp int

const (
	compareOpEqual compareOp = iota
	compareOpGreater
	compareOpGreaterEqual
	compareOpLess
	compareOpLessEqual
)

type valueKind int

const (
	kindInvalid valueKind = iota
	kindInt
	kindFloat
	kindString
	kindBool
)

type comparableValue struct {
	kind    valueKind
	i64     int64
	f64     float64
	str     string
	boolVal bool
}

func (v comparableValue) compare(other comparableValue) (int, bool) {
	if v.kind != other.kind {
		return 0, false
	}
	switch v.kind {
	case kindInt:
		switch {
		case v.i64 < other.i64:
			return -1, true
		case v.i64 > other.i64:
			return 1, true
		default:
			return 0, true
		}
	case kindFloat:
		switch {
		case v.f64 < other.f64:
			return -1, true
		case v.f64 > other.f64:
			return 1, true
		default:
			return 0, true
		}
	case kindString:
		return strings.Compare(v.str, other.str), true
	case kindBool:
		switch {
		case !v.boolVal && other.boolVal:
			return -1, true
		case v.boolVal && !other.boolVal:
			return 1, true
		default:
			return 0, true
		}
	default:
		return 0, false
	}
}

type columnConstraint struct {
	min          *comparableValue
	max          *comparableValue
	minInclusive bool
	maxInclusive bool
	invalid      bool
}

func (c *columnConstraint) tightenMin(val comparableValue, inclusive bool) {
	if c.invalid {
		return
	}
	if c.min == nil {
		v := val
		c.min = &v
		c.minInclusive = inclusive
		return
	}
	cmp, ok := c.min.compare(val)
	if !ok {
		c.invalid = true
		return
	}
	if cmp < 0 {
		v := val
		c.min = &v
		c.minInclusive = inclusive
	} else if cmp == 0 {
		c.minInclusive = c.minInclusive && inclusive
	}
}

func (c *columnConstraint) tightenMax(val comparableValue, inclusive bool) {
	if c.invalid {
		return
	}
	if c.max == nil {
		v := val
		c.max = &v
		c.maxInclusive = inclusive
		return
	}
	cmp, ok := c.max.compare(val)
	if !ok {
		c.invalid = true
		return
	}
	if cmp > 0 {
		v := val
		c.max = &v
		c.maxInclusive = inclusive
	} else if cmp == 0 {
		c.maxInclusive = c.maxInclusive && inclusive
	}
}

func (c *columnConstraint) contradiction() bool {
	if c.invalid {
		return false
	}
	if c.min == nil || c.max == nil {
		return false
	}
	cmp, ok := c.min.compare(*c.max)
	if !ok {
		return false
	}
	if cmp > 0 {
		return true
	}
	if cmp == 0 && (!c.minInclusive || !c.maxInclusive) {
		return true
	}
	return false
}

func makeComparable(v interface{}) (comparableValue, bool) {
	switch val := v.(type) {
	case int:
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case int8:
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case int16:
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case int32:
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case int64:
		return comparableValue{kind: kindInt, i64: val}, true
	case uint:
		if uint64(val) > math.MaxInt64 {
			return comparableValue{}, false
		}
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case uint8:
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case uint16:
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case uint32:
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case uint64:
		if val > math.MaxInt64 {
			return comparableValue{}, false
		}
		return comparableValue{kind: kindInt, i64: int64(val)}, true
	case float32:
		if math.IsNaN(float64(val)) {
			return comparableValue{}, false
		}
		return comparableValue{kind: kindFloat, f64: float64(val)}, true
	case float64:
		if math.IsNaN(val) {
			return comparableValue{}, false
		}
		return comparableValue{kind: kindFloat, f64: val}, true
	case string:
		return comparableValue{kind: kindString, str: val}, true
	case []byte:
		return comparableValue{kind: kindString, str: string(val)}, true
	case parquet.ByteArray:
		return comparableValue{kind: kindString, str: string(val)}, true
	case parquet.FixedLenByteArray:
		return comparableValue{kind: kindString, str: string(val)}, true
	case bool:
		return comparableValue{kind: kindBool, boolVal: val}, true
	default:
		return comparableValue{}, false
	}
}

func buildColumnConstraints(filters []sql.Expression) (map[string]*columnConstraint, bool) {
	if len(filters) == 0 {
		return nil, false
	}
	constraints := make(map[string]*columnConstraint)
	var unsat bool
	for _, f := range filters {
		applyConstraintExpr(constraints, f, &unsat)
		if unsat {
			return nil, true
		}
	}
	for name, c := range constraints {
		if c == nil || c.invalid {
			delete(constraints, name)
		}
	}
	if len(constraints) == 0 {
		return nil, false
	}
	return constraints, false
}

func applyConstraintExpr(constraints map[string]*columnConstraint, expr sql.Expression, unsat *bool) {
	if *unsat {
		return
	}
	switch e := expr.(type) {
	case *expression.And:
		applyConstraintExpr(constraints, e.LeftChild, unsat)
		applyConstraintExpr(constraints, e.RightChild, unsat)
	case *expression.Between:
		field, ok := e.Val.(*expression.GetField)
		if !ok {
			return
		}
		column := strings.ToLower(field.Name())
		constraint := ensureConstraint(constraints, column)
		lower, lok := e.Lower.(*expression.Literal)
		upper, uok := e.Upper.(*expression.Literal)
		if lok {
			if val, ok := makeComparable(lower.Value()); ok {
				constraint.tightenMin(val, true)
			} else {
				constraint.invalid = true
			}
		}
		if uok {
			if val, ok := makeComparable(upper.Value()); ok {
				constraint.tightenMax(val, true)
			} else {
				constraint.invalid = true
			}
		}
		if constraint.contradiction() {
			*unsat = true
		}
	case *expression.Equals:
		applyBinaryConstraint(constraints, e.LeftChild, e.RightChild, compareOpEqual, unsat)
	case *expression.GreaterThan:
		applyBinaryConstraint(constraints, e.LeftChild, e.RightChild, compareOpGreater, unsat)
	case *expression.GreaterThanOrEqual:
		applyBinaryConstraint(constraints, e.LeftChild, e.RightChild, compareOpGreaterEqual, unsat)
	case *expression.LessThan:
		applyBinaryConstraint(constraints, e.LeftChild, e.RightChild, compareOpLess, unsat)
	case *expression.LessThanOrEqual:
		applyBinaryConstraint(constraints, e.LeftChild, e.RightChild, compareOpLessEqual, unsat)
	}
}

func applyBinaryConstraint(constraints map[string]*columnConstraint, left, right sql.Expression, op compareOp, unsat *bool) {
	if field, ok := left.(*expression.GetField); ok {
		if lit, ok := right.(*expression.Literal); ok {
			applyConstraintForField(constraints, strings.ToLower(field.Name()), lit.Value(), op, unsat)
		}
		return
	}
	if field, ok := right.(*expression.GetField); ok {
		if lit, ok := left.(*expression.Literal); ok {
			flipped := flipOp(op)
			applyConstraintForField(constraints, strings.ToLower(field.Name()), lit.Value(), flipped, unsat)
		}
	}
}

func applyConstraintForField(constraints map[string]*columnConstraint, column string, raw interface{}, op compareOp, unsat *bool) {
	constraint := ensureConstraint(constraints, column)
	val, ok := makeComparable(raw)
	if !ok {
		constraint.invalid = true
		return
	}
	switch op {
	case compareOpEqual:
		constraint.tightenMin(val, true)
		constraint.tightenMax(val, true)
	case compareOpGreater:
		constraint.tightenMin(val, false)
	case compareOpGreaterEqual:
		constraint.tightenMin(val, true)
	case compareOpLess:
		constraint.tightenMax(val, false)
	case compareOpLessEqual:
		constraint.tightenMax(val, true)
	}
	if constraint.contradiction() {
		*unsat = true
	}
}

func flipOp(op compareOp) compareOp {
	switch op {
	case compareOpGreater:
		return compareOpLess
	case compareOpGreaterEqual:
		return compareOpLessEqual
	case compareOpLess:
		return compareOpGreater
	case compareOpLessEqual:
		return compareOpGreaterEqual
	default:
		return compareOpEqual
	}
}

func ensureConstraint(m map[string]*columnConstraint, column string) *columnConstraint {
	c, ok := m[column]
	if !ok {
		c = &columnConstraint{}
		m[column] = c
	}
	return c
}

func pruneRowGroups(meta *metadata.FileMetaData, lookup map[string]int, candidates []int, constraints map[string]*columnConstraint) []int {
	if meta == nil || len(constraints) == 0 {
		return candidates
	}
	out := make([]int, 0, len(candidates))
	for _, idx := range candidates {
		if idx < 0 || idx >= len(meta.GetRowGroups()) {
			continue
		}
		rg := meta.RowGroup(idx)
		if rowGroupMatches(rg, lookup, constraints) {
			out = append(out, idx)
		}
	}
	return out
}

func rowGroupMatches(rg *metadata.RowGroupMetaData, lookup map[string]int, constraints map[string]*columnConstraint) bool {
	for name, constraint := range constraints {
		if constraint == nil {
			continue
		}
		idx, ok := lookup[name]
		if !ok {
			continue
		}
		chunk, err := rg.ColumnChunk(idx)
		if err != nil {
			return true
		}
		stats, err := chunk.Statistics()
		if err != nil || stats == nil || !stats.HasMinMax() {
			continue
		}
		minVal, maxVal, ok := statMinMax(stats)
		if !ok {
			continue
		}
		if constraint.min != nil {
			cmp, ok := maxVal.compare(*constraint.min)
			if ok {
				if cmp < 0 {
					return false
				}
				if cmp == 0 && !constraint.minInclusive {
					return false
				}
			}
		}
		if constraint.max != nil {
			cmp, ok := minVal.compare(*constraint.max)
			if ok {
				if cmp > 0 {
					return false
				}
				if cmp == 0 && !constraint.maxInclusive {
					return false
				}
			}
		}
	}
	return true
}

func statMinMax(stats metadata.TypedStatistics) (comparableValue, comparableValue, bool) {
	switch s := stats.(type) {
	case *metadata.Int32Statistics:
		min, okMin := makeComparable(s.Min())
		max, okMax := makeComparable(s.Max())
		return min, max, okMin && okMax
	case *metadata.Int64Statistics:
		min, okMin := makeComparable(s.Min())
		max, okMax := makeComparable(s.Max())
		return min, max, okMin && okMax
	case *metadata.Float32Statistics:
		min, okMin := makeComparable(s.Min())
		max, okMax := makeComparable(s.Max())
		return min, max, okMin && okMax
	case *metadata.Float64Statistics:
		min, okMin := makeComparable(s.Min())
		max, okMax := makeComparable(s.Max())
		return min, max, okMin && okMax
	case *metadata.BooleanStatistics:
		min, okMin := makeComparable(s.Min())
		max, okMax := makeComparable(s.Max())
		return min, max, okMin && okMax
	case *metadata.ByteArrayStatistics:
		min, okMin := makeComparable([]byte(s.Min()))
		max, okMax := makeComparable([]byte(s.Max()))
		return min, max, okMin && okMax
	case *metadata.FixedLenByteArrayStatistics:
		min, okMin := makeComparable([]byte(s.Min()))
		max, okMax := makeComparable([]byte(s.Max()))
		return min, max, okMin && okMax
	default:
		return comparableValue{}, comparableValue{}, false
	}
}
