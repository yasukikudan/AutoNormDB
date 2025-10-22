package arrowtable

// このファイルでは Apache Arrow のテーブルを go-mysql-server 互換の sql.Table として
// 露出させるラッパーを提供します。Arrow のレコードバッチを go-mysql-server のパーティション
// にマッピングし、SQL 実行エンジンから効率的に行イテレーションできるようにしています。

import (
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/dolthub/go-mysql-server/sql"

	"AutoNormDb/engine/arrowbackend"
)

const PartitionSize = 1 << 16

type RowRange struct {
	Chunk  int
	Offset int64
	Length int64
}

type PartitionMeta struct {
	Index    int
	RowCount int64
	Segments []RowRange
}

type PartitionManager struct {
	Parts []*PartitionMeta
}

// ArrowBackedTable exposes an in-memory Arrow table through the sql.Table
// interface expected by go-mysql-server. Each partition comprises 65,536 rows at
// most and may span multiple Arrow chunks.
type ArrowBackedTable struct {
	name                 string
	baseSchema           sql.Schema
	schema               sql.Schema
	columnLookup         map[string]int
	projection           []int
	projectedArrowSchema *arrow.Schema
	arrTable             arrow.Table
	pushed               []sql.Expression
	pm                   *PartitionManager
}

// NewArrowBackedTable wraps the provided Arrow table. The Arrow table is
// retained so that it remains valid for the lifetime of the ArrowBackedTable.
// A row-count-based partition manager is constructed so that the table can be
// consumed through go-mysql-server's partitioned table interface.
func NewArrowBackedTable(name string, arrTable arrow.Table) (*ArrowBackedTable, error) {
	schema, err := ArrowSchemaToSQLSchema(arrTable.Schema(), name)
	if err != nil {
		return nil, err
	}

	pm, err := buildPartitionManager(arrTable)
	if err != nil {
		return nil, err
	}

	base := make(sql.Schema, len(schema))
	copy(base, schema)

	lookup := make(map[string]int, len(base))
	for i, col := range base {
		lookup[strings.ToLower(col.Name)] = i
	}

	arrTable.Retain()
	return &ArrowBackedTable{
		name:         name,
		baseSchema:   base,
		schema:       base,
		columnLookup: lookup,
		arrTable:     arrTable,
		pm:           pm,
	}, nil
}

// Name implements sql.Table.
func (t *ArrowBackedTable) Name() string { return t.name }

// String implements fmt.Stringer for debugging convenience.
func (t *ArrowBackedTable) String() string { return t.name }

// Schema implements sql.Table.
func (t *ArrowBackedTable) Schema() sql.Schema { return t.schema }

// Filters returns the filter expressions that will be pushed down into Arrow
// compute. When no filters are tracked the method returns nil so that go-mysql-
// server understands no pushdown has been configured.
func (t *ArrowBackedTable) Filters() []sql.Expression {
	if len(t.pushed) == 0 {
		return nil
	}
	out := make([]sql.Expression, len(t.pushed))
	copy(out, t.pushed)
	return out
}

// HandledFilters reports the subset of the supplied filters that
// ArrowBackedTable knows how to evaluate with Arrow compute kernels.
func (t *ArrowBackedTable) HandledFilters(filters []sql.Expression) []sql.Expression {
	var handled []sql.Expression
	for _, f := range filters {
		if arrowbackend.CanHandleForCompute(f) {
			handled = append(handled, f)
		}
	}
	return handled
}

// WithFilters returns a shallow copy of the table that remembers the set of
// pushable filters. Non-pushable predicates will be evaluated by the engine
// outside of this table implementation.
func (t *ArrowBackedTable) WithFilters(ctx *sql.Context, filters []sql.Expression) sql.Table {
	_ = ctx
	nt := *t
	nt.pushed = t.HandledFilters(filters)
	return &nt
}

// WithProjections implements sql.ProjectedTable. The returned copy remembers
// which columns were requested so that Arrow record materialisation can skip
// unnecessary arrays.
func (t *ArrowBackedTable) WithProjections(columns []string) sql.Table {
	nt := *t

	if len(columns) == 0 {
		nt.schema = sql.Schema{}
		nt.projection = make([]int, 0)
		nt.projectedArrowSchema = projectArrowSchema(t.arrTable.Schema(), nt.projection)
		return &nt
	}

	indices := make([]int, len(columns))
	projected := make(sql.Schema, len(columns))
	for i, col := range columns {
		idx, ok := t.columnLookup[strings.ToLower(col)]
		if !ok {
			nt.schema = nt.baseSchema
			nt.projection = nil
			nt.projectedArrowSchema = nil
			return &nt
		}
		indices[i] = idx
		projected[i] = t.baseSchema[idx]
	}

	nt.projection = indices
	nt.schema = projected
	nt.projectedArrowSchema = projectArrowSchema(t.arrTable.Schema(), indices)
	return &nt
}

// Projections reports the column names currently projected for this table, or
// nil when all columns should be returned.
func (t *ArrowBackedTable) Projections() []string {
	if t.projection == nil {
		return nil
	}

	names := make([]string, len(t.schema))
	for i, col := range t.schema {
		names[i] = col.Name
	}
	return names
}

type chunkPartition struct {
	idx  int
	meta *PartitionMeta
}

func (p *chunkPartition) Key() []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(p.idx))
	return buf
}

// Partitions implements sql.Table. Each logical partition spans up to
// PartitionSize rows and may be composed of multiple Arrow chunks.
func (t *ArrowBackedTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	if t.pm == nil || len(t.pm.Parts) == 0 {
		return sql.PartitionsToPartitionIter(), nil
	}

	parts := make([]sql.Partition, 0, len(t.pm.Parts))
	for i, meta := range t.pm.Parts {
		parts = append(parts, &chunkPartition{idx: i, meta: meta})
	}
	return sql.PartitionsToPartitionIter(parts...), nil
}

// PartitionRows implements sql.PartitionedTable. Each partition is materialised
// into one or more Arrow records depending on how many chunk segments it spans.
// Filters are pushed into Arrow compute when supported, otherwise the full
// record slices are exposed to go-mysql-server for row-wise evaluation.
func (t *ArrowBackedTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	cp, ok := p.(*chunkPartition)
	if !ok {
		return nil, fmt.Errorf("unexpected partition type %T", p)
	}
	meta := cp.meta
	if meta == nil || meta.RowCount == 0 {
		return sql.RowsToRowIter(), nil
	}

	src := &segmentRecordSource{table: t, segments: meta.Segments}
	var plan arrowbackend.ExecPlan
	if len(t.pushed) > 0 && t.arrTable != nil && t.arrTable.NumCols() > 0 {
		plan = arrowbackend.NewStaticExecPlan(t.pushed)
	}
	return arrowbackend.NewArrowRowIter(src, plan, nil), nil
}

type segmentRecordSource struct {
	table    *ArrowBackedTable
	segments []RowRange
	idx      int
	current  arrow.Record
	err      error
}

func (s *segmentRecordSource) Next() bool {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}

	for s.idx < len(s.segments) {
		rec, err := s.table.sliceRecordForSegment(s.segments[s.idx])
		s.idx++
		if err != nil {
			s.err = err
			return false
		}
		if rec == nil {
			continue
		}
		if rec.NumRows() == 0 {
			rec.Release()
			continue
		}
		s.current = rec
		return true
	}

	return false
}

func (s *segmentRecordSource) Record() arrow.Record { return s.current }

func (s *segmentRecordSource) Err() error { return s.err }

func (s *segmentRecordSource) Release() {
	if s.current != nil {
		s.current.Release()
		s.current = nil
	}
}

// Collation implements sql.Table Collation support; Arrow arrays are
// byte-oriented so we return the default collation.
func (t *ArrowBackedTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (t *ArrowBackedTable) sliceRecordForSegment(seg RowRange) (arrow.Record, error) {
	if seg.Length == 0 {
		return nil, nil
	}

	if t.arrTable == nil {
		return nil, fmt.Errorf("no underlying Arrow table")
	}

	if t.arrTable.NumCols() == 0 {
		rec := array.NewRecord(t.arrTable.Schema(), nil, seg.Length)
		return rec, nil
	}

	if t.projection == nil {
		numCols := int(t.arrTable.NumCols())
		cols := make([]arrow.Array, numCols)
		for i := 0; i < numCols; i++ {
			data := t.arrTable.Column(i).Data()
			if seg.Chunk < 0 || seg.Chunk >= len(data.Chunks()) {
				return nil, fmt.Errorf("segment chunk index out of range: chunk=%d col=%d", seg.Chunk, i)
			}
			chunk := data.Chunk(seg.Chunk)
			clen := int64(chunk.Len())
			if seg.Offset < 0 || seg.Offset+seg.Length > clen {
				return nil, fmt.Errorf("segment slice out of bounds: chunk=%d len=%d want=[%d,%d)", seg.Chunk, chunk.Len(), seg.Offset, seg.Offset+seg.Length)
			}
			arr := array.NewSlice(chunk, seg.Offset, seg.Offset+seg.Length)
			cols[i] = arr
		}

		rec := array.NewRecord(t.arrTable.Schema(), cols, seg.Length)
		for _, col := range cols {
			col.Release()
		}
		return rec, nil
	}

	if len(t.projection) == 0 {
		schema := t.projectedArrowSchema
		if schema == nil {
			schema = projectArrowSchema(t.arrTable.Schema(), t.projection)
		}
		return array.NewRecord(schema, nil, seg.Length), nil
	}

	cols := make([]arrow.Array, len(t.projection))
	for outIdx, colIdx := range t.projection {
		if colIdx < 0 || colIdx >= int(t.arrTable.NumCols()) {
			return nil, fmt.Errorf("projection column index out of range: %d", colIdx)
		}
		data := t.arrTable.Column(colIdx).Data()
		if seg.Chunk < 0 || seg.Chunk >= len(data.Chunks()) {
			return nil, fmt.Errorf("segment chunk index out of range: chunk=%d col=%d", seg.Chunk, colIdx)
		}
		chunk := data.Chunk(seg.Chunk)
		clen := int64(chunk.Len())
		if seg.Offset < 0 || seg.Offset+seg.Length > clen {
			return nil, fmt.Errorf("segment slice out of bounds: chunk=%d len=%d want=[%d,%d)", seg.Chunk, chunk.Len(), seg.Offset, seg.Offset+seg.Length)
		}
		arr := array.NewSlice(chunk, seg.Offset, seg.Offset+seg.Length)
		cols[outIdx] = arr
	}

	schema := t.projectedArrowSchema
	if schema == nil {
		schema = projectArrowSchema(t.arrTable.Schema(), t.projection)
	}

	rec := array.NewRecord(schema, cols, seg.Length)
	for _, col := range cols {
		col.Release()
	}
	return rec, nil
}

func projectArrowSchema(base *arrow.Schema, indices []int) *arrow.Schema {
	if base == nil {
		return nil
	}
	if indices == nil {
		return base
	}
	if len(indices) == 0 {
		md := base.Metadata()
		return arrow.NewSchema(nil, &md)
	}

	fields := make([]arrow.Field, 0, len(indices))
	for _, idx := range indices {
		if idx < 0 || idx >= len(base.Fields()) {
			continue
		}
		fields = append(fields, base.Field(idx))
	}
	if len(fields) == 0 {
		return base
	}
	md := base.Metadata()
	return arrow.NewSchema(fields, &md)
}

func buildPartitionManager(tbl arrow.Table) (*PartitionManager, error) {
	pm := &PartitionManager{}
	rows := tbl.NumRows()
	if rows == 0 {
		return pm, nil
	}

	if tbl.NumCols() == 0 {
		var idx int
		var done int64
		for done < rows {
			take := int64(PartitionSize)
			if remaining := rows - done; remaining < take {
				take = remaining
			}
			pm.Parts = append(pm.Parts, &PartitionMeta{
				Index:    idx,
				RowCount: take,
				Segments: []RowRange{{Chunk: 0, Offset: done, Length: take}},
			})
			idx++
			done += take
		}
		return pm, nil
	}

	chunks := tbl.Column(0).Data().Chunks()
	if len(chunks) == 0 {
		return pm, nil
	}

	partIdx := 0
	current := &PartitionMeta{Index: partIdx}
	remaining := int64(PartitionSize)

	for ci, chunk := range chunks {
		clen := int64(chunk.Len())
		off := int64(0)
		for off < clen {
			if remaining == 0 {
				pm.Parts = append(pm.Parts, current)
				partIdx++
				current = &PartitionMeta{Index: partIdx}
				remaining = PartitionSize
			}
			take := clen - off
			if take > remaining {
				take = remaining
			}
			current.Segments = append(current.Segments, RowRange{Chunk: ci, Offset: off, Length: take})
			current.RowCount += take
			off += take
			remaining -= take
		}
	}

	if current.RowCount > 0 {
		pm.Parts = append(pm.Parts, current)
	}
	return pm, nil
}

var _ sql.FilteredTable = (*ArrowBackedTable)(nil)
var _ sql.ProjectedTable = (*ArrowBackedTable)(nil)
