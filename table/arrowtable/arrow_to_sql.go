package arrowtable

// Arrow のスキーマ情報を go-mysql-server の SQL スキーマに変換するユーティリティです。
// ここで型対応表を定義することで、Parquet から読み込んだ Arrow テーブルを SQL テーブル
// として認識させることができます。

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
		// Arrow のフィールド型を go-mysql-server が理解できる sql.Type に変換します。
		typ, err := arrowFieldToSQLType(f)
		if err != nil {
			return nil, err
		}
		// go-mysql-server では各列がどのテーブル由来かを Source で管理するため、引数で受け取った
		// source 名を設定します。Nullable フラグも Arrow の定義を踏襲します。
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
	// Arrow のデータ型を go-mysql-server の型にマッピングします。MySQL 側でサポートの薄い
	// 型はより広い互換型（例: 文字列は LongText）へフォールバックしています。
	switch dt := f.Type.(type) {
	case *arrow.Int8Type:
		return types.Int8, nil
	case *arrow.Uint8Type:
		return types.Uint8, nil
	case *arrow.Int16Type:
		return types.Int16, nil
	case *arrow.Uint16Type:
		return types.Uint16, nil
	case *arrow.Int32Type, *arrow.Date32Type:
		return types.Int32, nil
	case *arrow.Uint32Type:
		return types.Uint32, nil
	case *arrow.Int64Type, *arrow.TimestampType, *arrow.Date64Type:
		return types.Int64, nil
	case *arrow.Uint64Type:
		return types.Uint64, nil
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
	case *arrow.Decimal128Type:
		return arrowDecimalToSQLType(f.Name, dt.Precision, dt.Scale)
	case *arrow.Decimal256Type:
		return arrowDecimalToSQLType(f.Name, dt.Precision, dt.Scale)
	default:
		// マッピング表に存在しない型に遭遇した場合はエラーとして返し、呼び出し元で
		// 適切な対処（例えばスキップやカスタム変換）を行ってもらいます。
		return nil, fmt.Errorf("unsupported Arrow type for column %q: %s", f.Name, f.Type)
	}
}

const maxUint8Value = int32(^uint8(0))

func arrowDecimalToSQLType(column string, precision, scale int32) (sql.Type, error) {
	if precision < 0 || scale < 0 {
		return nil, fmt.Errorf("invalid decimal definition for column %q: precision=%d scale=%d", column, precision, scale)
	}
	if precision > maxUint8Value || scale > maxUint8Value {
		return nil, fmt.Errorf("decimal precision/scale for column %q exceeds supported range: precision=%d scale=%d", column, precision, scale)
	}

	decType, err := types.CreateDecimalType(uint8(precision), uint8(scale))
	if err != nil {
		return nil, fmt.Errorf("unsupported decimal definition for column %q: %w", column, err)
	}

	return decType, nil
}
