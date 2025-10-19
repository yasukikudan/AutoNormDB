package arrowtable

import (
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ArrowSchemaToSQLSchema converts an Arrow schema into a go-mysql-server schema.
//
// The resulting columns reuse the provided table name as their Source value so
// that go-mysql-server reports the rows as belonging to the Arrow-backed table.
func ArrowSchemaToSQLSchema(s *arrow.Schema, source string) (sql.Schema, error) {
	fields := s.Fields()
	cols := make(sql.Schema, len(fields))

	for i, f := range fields {
		typ, err := arrowFieldToSQLType(f)
		if err != nil {
			return nil, err
		}
		cols[i] = &sql.Column{
			Name:     f.Name,
			Type:     typ,
			Nullable: f.Nullable,
			Source:   source,
		}
	}

	return cols, nil
}

func arrowFieldToSQLType(f arrow.Field) (sql.Type, error) {
	switch f.Type.(type) {
	case *arrow.Int8Type, *arrow.Uint8Type:
		return types.Int8, nil
	case *arrow.Int16Type, *arrow.Uint16Type:
		return types.Int16, nil
	case *arrow.Int32Type, *arrow.Uint32Type, *arrow.Date32Type:
		return types.Int32, nil
	case *arrow.Int64Type, *arrow.Uint64Type, *arrow.TimestampType, *arrow.Date64Type:
		return types.Int64, nil
	case *arrow.Float32Type:
		return types.Float32, nil
	case *arrow.Float64Type:
		return types.Float64, nil
	case *arrow.BooleanType:
		return types.Boolean, nil
	case *arrow.StringType, *arrow.LargeStringType:
		return types.LongText, nil
	case *arrow.BinaryType, *arrow.LargeBinaryType, *arrow.FixedSizeBinaryType:
		return types.LongBlob, nil
	default:
		return nil, fmt.Errorf("unsupported Arrow type for column %q: %s", f.Name, f.Type)
	}
}
