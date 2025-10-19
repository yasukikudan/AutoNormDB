package arrowtable

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
		return &arrowRowIter{cols: nil, row: 0, n: int(tbl.NumRows())}
	}

	cols := make([]arrow.Array, numCols)
	for c := 0; c < numCols; c++ {
		chunk := tbl.Column(c).Data().Chunk(chunkIdx)
		chunk.Retain()
		cols[c] = chunk
	}

	return &arrowRowIter{cols: cols, row: 0, n: cols[0].Len()}
}

func (it *arrowRowIter) Next(*sql.Context) (sql.Row, error) {
	if it.row >= it.n {
		return nil, io.EOF
	}

	if len(it.cols) == 0 {
		it.row++
		return sql.Row{}, nil
	}

	r := make(sql.Row, len(it.cols))
	for i, col := range it.cols {
		if col.IsNull(it.row) {
			r[i] = nil
			continue
		}
		r[i] = valueAt(col, it.row)
	}

	it.row++
	return r, nil
}

func (it *arrowRowIter) Close(*sql.Context) error {
	for _, col := range it.cols {
		col.Release()
	}
	it.cols = nil
	return nil
}

func valueAt(col arrow.Array, idx int) any {
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
		return nil
	}
}
