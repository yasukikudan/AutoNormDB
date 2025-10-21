package arrowtable

import (
	"io"
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func buildTestArrowTable(t *testing.T) arrow.Table {
	t.Helper()

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32},
		{Name: "v", Type: arrow.PrimitiveTypes.Float64},
	}, nil)

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	idb := builder.Field(0).(*array.Int32Builder)
	vb := builder.Field(1).(*array.Float64Builder)

	idb.AppendValues([]int32{1, 2, 3, 4}, nil)
	vb.AppendValues([]float64{0.1, 0.2, 0.3, 0.4}, nil)

	rec := builder.NewRecord()
	defer rec.Release()

	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func buildArrowTableWithRows(t *testing.T, rows int64) arrow.Table {
	t.Helper()

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	ids := builder.Field(0).(*array.Int64Builder)
	for i := int64(0); i < rows; i++ {
		ids.Append(i)
	}

	rec := builder.NewRecord()
	defer rec.Release()

	return array.NewTableFromRecords(schema, []arrow.Record{rec})
}

func buildArrowTableWithChunks(t *testing.T, chunkSizes []int64) arrow.Table {
	t.Helper()

	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

	var records []arrow.Record
	var start int64
	for _, n := range chunkSizes {
		builder := array.NewRecordBuilder(mem, schema)
		ids := builder.Field(0).(*array.Int64Builder)
		for i := int64(0); i < n; i++ {
			ids.Append(start + i)
		}
		rec := builder.NewRecord()
		builder.Release()
		records = append(records, rec)
		start += n
	}

	tbl := array.NewTableFromRecords(schema, records)
	for _, rec := range records {
		rec.Release()
	}
	return tbl
}

func collectAllRows(t *testing.T, tbl sql.Table, ctx *sql.Context) []sql.Row {
	t.Helper()

	parts, err := tbl.Partitions(ctx)
	if err != nil {
		t.Fatalf("Partitions error: %v", err)
	}
	defer func() {
		if err := parts.Close(ctx); err != nil {
			t.Fatalf("Partitions close error: %v", err)
		}
	}()

	var rows []sql.Row
	for {
		part, err := parts.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Next partition error: %v", err)
		}

		iter, err := tbl.PartitionRows(ctx, part)
		if err != nil {
			t.Fatalf("PartitionRows error: %v", err)
		}
		for {
			row, err := iter.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = iter.Close(ctx)
				t.Fatalf("iter.Next error: %v", err)
			}
			rows = append(rows, row)
		}
		if err := iter.Close(ctx); err != nil {
			t.Fatalf("iter.Close error: %v", err)
		}
	}

	return rows
}

func TestBuildPartitionManagerRowCounts(t *testing.T) {
	cases := []struct {
		rows int64
	}{
		{rows: 0},
		{rows: 1},
		{rows: PartitionSize - 1},
		{rows: PartitionSize},
		{rows: PartitionSize + 1},
		{rows: 2*PartitionSize - 1},
		{rows: 2 * PartitionSize},
	}

	for _, tt := range cases {
		tbl := buildArrowTableWithRows(t, tt.rows)
		pm, err := buildPartitionManager(tbl)
		if err != nil {
			t.Fatalf("buildPartitionManager(%d rows): %v", tt.rows, err)
		}
		tbl.Release()

		var expected int
		if tt.rows > 0 {
			expected = int((tt.rows + PartitionSize - 1) / PartitionSize)
		}
		if got := len(pm.Parts); got != expected {
			t.Fatalf("rows=%d expected %d parts, got %d", tt.rows, expected, got)
		}

		var total int64
		for _, part := range pm.Parts {
			total += part.RowCount
		}
		if total != tt.rows {
			t.Fatalf("rows=%d expected total row count %d, got %d", tt.rows, tt.rows, total)
		}
	}
}

func TestBuildPartitionManagerSegmentsAcrossChunks(t *testing.T) {
	tbl := buildArrowTableWithChunks(t, []int64{10_000, 80_000, 20_000})
	pm, err := buildPartitionManager(tbl)
	if err != nil {
		t.Fatalf("buildPartitionManager: %v", err)
	}
	tbl.Release()

	if len(pm.Parts) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(pm.Parts))
	}

	first := pm.Parts[0]
	if first.RowCount != PartitionSize {
		t.Fatalf("first partition expected %d rows, got %d", PartitionSize, first.RowCount)
	}
	if len(first.Segments) != 2 {
		t.Fatalf("first partition expected 2 segments, got %d", len(first.Segments))
	}
	if first.Segments[0] != (RowRange{Chunk: 0, Offset: 0, Length: 10_000}) {
		t.Fatalf("unexpected first segment: %#v", first.Segments[0])
	}
	if first.Segments[1] != (RowRange{Chunk: 1, Offset: 0, Length: PartitionSize - 10_000}) {
		t.Fatalf("unexpected second segment: %#v", first.Segments[1])
	}

	second := pm.Parts[1]
	if second.RowCount != 44_464 {
		t.Fatalf("second partition expected 44464 rows, got %d", second.RowCount)
	}
	if len(second.Segments) != 2 {
		t.Fatalf("second partition expected 2 segments, got %d", len(second.Segments))
	}
	if second.Segments[0] != (RowRange{Chunk: 1, Offset: PartitionSize - 10_000, Length: 24_464}) {
		t.Fatalf("unexpected segment[0]: %#v", second.Segments[0])
	}
	if second.Segments[1] != (RowRange{Chunk: 2, Offset: 0, Length: 20_000}) {
		t.Fatalf("unexpected segment[1]: %#v", second.Segments[1])
	}
}

func TestPartitionRowsSequentialIteration(t *testing.T) {
	tbl := buildArrowTableWithRows(t, PartitionSize+10)
	abt, err := NewArrowBackedTable("t", tbl)
	if err != nil {
		t.Fatalf("NewArrowBackedTable: %v", err)
	}
	defer abt.arrTable.Release()
	tbl.Release()

	rows := collectAllRows(t, abt, sql.NewEmptyContext())
	if len(rows) != int(PartitionSize+10) {
		t.Fatalf("expected %d rows, got %d", PartitionSize+10, len(rows))
	}
	for i, row := range rows {
		if row[0].(int64) != int64(i) {
			t.Fatalf("row %d expected id %d, got %v", i, i, row[0])
		}
	}
}

func TestArrowBackedTableFilterPushdown(t *testing.T) {
	tbl := buildTestArrowTable(t)
	abt, err := NewArrowBackedTable("t", tbl)
	if err != nil {
		t.Fatalf("NewArrowBackedTable: %v", err)
	}
	defer abt.arrTable.Release()
	tbl.Release()

	ctx := sql.NewEmptyContext()

	idField := expression.NewGetField(0, types.Int32, "id", false)
	vField := expression.NewGetField(1, types.Float64, "v", false)

	gt := expression.NewGreaterThan(idField, expression.NewLiteral(int64(2), types.Int64))
	lte := expression.NewLessThanOrEqual(vField, expression.NewLiteral(0.3, types.Float64))
	combined := expression.NewAnd(gt, lte)

	handled := abt.HandledFilters([]sql.Expression{combined})
	if len(handled) != 1 {
		t.Fatalf("expected handled filters to include AND expression, got %d", len(handled))
	}

	filtered := abt.WithFilters(ctx, []sql.Expression{combined}).(*ArrowBackedTable)
	rows := collectAllRows(t, filtered, ctx)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after pushdown, got %d", len(rows))
	}
	if got := rows[0][0]; got != int32(3) {
		t.Fatalf("unexpected id value %v", got)
	}
	if got := rows[0][1]; got != 0.3 {
		t.Fatalf("unexpected v value %v", got)
	}
}

func TestArrowBackedTableBetweenCasts(t *testing.T) {
	tbl := buildTestArrowTable(t)
	abt, err := NewArrowBackedTable("t", tbl)
	if err != nil {
		t.Fatalf("NewArrowBackedTable: %v", err)
	}
	defer abt.arrTable.Release()
	tbl.Release()

	ctx := sql.NewEmptyContext()

	between := expression.NewBetween(
		expression.NewGetField(0, types.Int32, "id", false),
		expression.NewLiteral(int16(2), types.Int16),
		expression.NewLiteral(int64(3), types.Int64),
	)

	filtered := abt.WithFilters(ctx, []sql.Expression{between}).(*ArrowBackedTable)
	rows := collectAllRows(t, filtered, ctx)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for BETWEEN predicate, got %d", len(rows))
	}
	ids := []interface{}{rows[0][0], rows[1][0]}
	if ids[0] != int32(2) || ids[1] != int32(3) {
		t.Fatalf("unexpected ids after BETWEEN filter: %v", ids)
	}
}

func TestSliceRecordForSegmentBoundsCheck(t *testing.T) {
	tbl := buildArrowTableWithRows(t, 10)
	abt, err := NewArrowBackedTable("t", tbl)
	if err != nil {
		t.Fatalf("NewArrowBackedTable: %v", err)
	}
	defer abt.arrTable.Release()
	tbl.Release()

	_, err = abt.sliceRecordForSegment(RowRange{Chunk: 0, Offset: 0, Length: 20})
	if err == nil {
		t.Fatalf("expected sliceRecordForSegment to error for out-of-range segment")
	}
}
