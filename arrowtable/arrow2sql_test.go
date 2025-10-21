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
