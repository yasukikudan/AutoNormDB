package arrowtable

import (
	"testing"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestArrowSchemaToSQLSchemaDecimal(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "price", Type: &arrow.Decimal128Type{Precision: 7, Scale: 2}},
	}, nil)

	cols, err := ArrowSchemaToSQLSchema(schema, "prices")
	if err != nil {
		t.Fatalf("ArrowSchemaToSQLSchema error: %v", err)
	}

	if len(cols) != 1 {
		t.Fatalf("expected 1 column, got %d", len(cols))
	}

	col := cols[0]
	if col.Name != "price" {
		t.Fatalf("unexpected column name %q", col.Name)
	}
	if col.Source != "prices" {
		t.Fatalf("unexpected column source %q", col.Source)
	}

	dec, ok := col.Type.(sql.DecimalType)
	if !ok {
		t.Fatalf("expected decimal type, got %T", col.Type)
	}

	if dec.Precision() != 7 {
		t.Fatalf("unexpected precision %d", dec.Precision())
	}
	if dec.Scale() != 2 {
		t.Fatalf("unexpected scale %d", dec.Scale())
	}

	expected := types.MustCreateDecimalType(7, 2)
	if dec.String() != expected.String() {
		t.Fatalf("expected decimal type %s, got %s", expected.String(), dec.String())
	}
}

func TestArrowSchemaToSQLSchemaUnsignedIntegers(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "u8", Type: arrow.PrimitiveTypes.Uint8},
		{Name: "u16", Type: arrow.PrimitiveTypes.Uint16},
		{Name: "u32", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "u64", Type: arrow.PrimitiveTypes.Uint64},
	}, nil)

	cols, err := ArrowSchemaToSQLSchema(schema, "nums")
	if err != nil {
		t.Fatalf("ArrowSchemaToSQLSchema error: %v", err)
	}

	if len(cols) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(cols))
	}

	cases := []struct {
		idx int
		typ sql.Type
	}{
		{0, types.Uint8},
		{1, types.Uint16},
		{2, types.Uint32},
		{3, types.Uint64},
	}

	for _, tc := range cases {
		if cols[tc.idx].Type != tc.typ {
			t.Fatalf("column %d expected type %T, got %T", tc.idx, tc.typ, cols[tc.idx].Type)
		}
	}
}
