package arrowtable

// このファイルでは Arrow のレコードバッチを go-mysql-server の行イテレータに変換する
// 実装を提供します。Arrow の列指向データを行指向で取り出し、SQL エンジンが 1 行ずつ
// 消費できるように変換するのが主目的です。

import (
	"io"
	"math"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/dolthub/go-mysql-server/sql"
)

type arrowRowIter struct {
	cols []arrow.Array
	row  int
	n    int
}

func newArrowRowIter(tbl arrow.Table, chunkIdx int) sql.RowIter {
	numCols := int(tbl.NumCols())
	if numCols == 0 {
		// 列が存在しないテーブル（たとえば COUNT 用に論理行だけがあるケース）では、空の
		// 行を指定された件数分返すイテレータを生成します。
		return &arrowRowIter{cols: nil, row: 0, n: int(tbl.NumRows())}
	}

	cols := make([]arrow.Array, numCols)
	for c := 0; c < numCols; c++ {
		// 各列について対象チャンクの配列を Retain し、イテレータが Close されるまで寿命を確保します。
		chunk := tbl.Column(c).Data().Chunk(chunkIdx)
		chunk.Retain()
		cols[c] = chunk
	}

	return &arrowRowIter{cols: cols, row: 0, n: cols[0].Len()}
}

func (it *arrowRowIter) Next(*sql.Context) (sql.Row, error) {
	if it.row >= it.n {
		// すべての行を読み終えたら io.EOF を返してイテレーション完了を通知します。
		return nil, io.EOF
	}

	if len(it.cols) == 0 {
		// 列なしテーブルでは空行（長さ 0 の sql.Row）を返しつつ、行カウントを進めます。
		it.row++
		return sql.Row{}, nil
	}

	r := make(sql.Row, len(it.cols))
	for i, col := range it.cols {
		if col.IsNull(it.row) {
			// Arrow のヌル値は go-mysql-server では nil として扱うため、そのまま設定します。
			r[i] = nil
			continue
		}
		// 列データ型に応じた Go 値を抽出し、sql.Row に格納します。
		r[i] = valueAt(col, it.row)
	}

	it.row++
	return r, nil
}

func (it *arrowRowIter) Close(*sql.Context) error {
	// Retain していた列配列の参照カウントを減らし、メモリ解放を促します。
	for _, col := range it.cols {
		col.Release()
	}
	it.cols = nil
	return nil
}

func valueAt(col arrow.Array, idx int) any {
	// Arrow の各配列型に対応する go-mysql-server の型へ変換します。符号なし整数は MySQL が
	// 符号付き 64bit 整数で扱うため、オーバーフローを避ける処理を加えています。
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
	default:
		// 未対応型に遭遇した場合は nil を返し、呼び出し側で後続処理を検討できるようにします。
		return nil
	}
}
