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
	t.Cleanup(builder.Release)

	idb := builder.Field(0).(*array.Int32Builder)
	vb := builder.Field(1).(*array.Float64Builder)

	idb.AppendValues([]int32{1, 2, 3, 4}, nil)
	vb.AppendValues([]float64{0.1, 0.2, 0.3, 0.4}, nil)

	record := builder.NewRecord()
	t.Cleanup(record.Release)

	tbl := array.NewTableFromRecords(schema, []arrow.Record{record})
	t.Cleanup(tbl.Release)
	return tbl
}

func collectRows(t *testing.T, tbl sql.Table, ctx *sql.Context) []sql.Row {
	t.Helper()

	parts, err := tbl.Partitions(ctx)
	if err != nil {
		t.Fatalf("Partitions error: %v", err)
	}
	t.Cleanup(func() { _ = parts.Close(ctx) })

	part, err := parts.Next(ctx)
	if err != nil {
		t.Fatalf("Next partition error: %v", err)
	}

	iter, err := tbl.PartitionRows(ctx, part)
	if err != nil {
		t.Fatalf("PartitionRows error: %v", err)
	}
	t.Cleanup(func() { _ = iter.Close(ctx) })

	var rows []sql.Row
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("iter.Next error: %v", err)
		}
		rows = append(rows, row)
	}
	return rows
}

func TestArrowBackedTableFilterPushdown(t *testing.T) {
	tbl := buildTestArrowTable(t)
	abt, err := NewArrowBackedTable("t", tbl)
	if err != nil {
		t.Fatalf("NewArrowBackedTable: %v", err)
	}

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
	if len(filtered.Filters()) != 1 {
		t.Fatalf("expected one pushed filter, got %d", len(filtered.Filters()))
	}

	rows := collectRows(t, filtered, ctx)
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

	ctx := sql.NewEmptyContext()

	between := expression.NewBetween(
		expression.NewGetField(0, types.Int32, "id", false),
		expression.NewLiteral(int16(2), types.Int16),
		expression.NewLiteral(int64(3), types.Int64),
	)

	filtered := abt.WithFilters(ctx, []sql.Expression{between}).(*ArrowBackedTable)
	rows := collectRows(t, filtered, ctx)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows for BETWEEN predicate, got %d", len(rows))
	}
	ids := []interface{}{rows[0][0], rows[1][0]}
	if ids[0] != int32(2) || ids[1] != int32(3) {
		t.Fatalf("unexpected ids after BETWEEN filter: %v", ids)
	}
}
