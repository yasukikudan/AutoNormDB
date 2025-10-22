package arrowbackend

import (
	"context"
	"io"
	"math"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/compute"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"
)

// RecordSource abstracts the source of Arrow records. Implementations can
// stream records from Parquet, in-memory tables, or any other backend capable
// of producing Arrow record batches.
type RecordSource interface {
	Next() bool
	Record() arrow.Record
	Err() error
	Release()
}

// ExecPlan encapsulates a compiled filter that can be evaluated against Arrow
// records. It returns a boolean selection mask along with the selectivity so
// that callers can choose the most efficient evaluation strategy.
type ExecPlan interface {
	BuildMask(ctx context.Context, rec arrow.Record) (mask arrow.Array, selectivity float64, err error)
	Close()
}

// ArrowRowIter exposes Arrow records as go-mysql-server row iterators. It can
// stream data from any RecordSource and optionally apply ExecPlan-based
// filtering.
type ArrowRowIter struct {
	src     RecordSource
	plan    ExecPlan
	current arrow.Record
	getters []colGetter
	rowBuf  sql.Row

	selIdxs []int
	pos     int
}

// ArrowRowIterOpts allows future configuration of ArrowRowIter behaviour.
type ArrowRowIterOpts struct{}

// NewArrowRowIter constructs an ArrowRowIter for the provided record source and
// optional execution plan.
func NewArrowRowIter(src RecordSource, plan ExecPlan, _ *ArrowRowIterOpts) *ArrowRowIter {
	return &ArrowRowIter{src: src, plan: plan, rowBuf: make(sql.Row, 0)}
}

// Next implements sql.RowIter. It materialises one row at a time from the
// underlying Arrow records.
func (it *ArrowRowIter) Next(ctx *sql.Context) (sql.Row, error) {
	goCtx := context.Background()
	if ctx != nil {
		goCtx = ctx
	}

	for {
		if it.current != nil && it.pos < len(it.selIdxs) {
			idx := it.selIdxs[it.pos]
			it.pos++
			row := it.rowBuf
			for i, getter := range it.getters {
				row[i] = getter(idx)
			}
			return row, nil
		}

		if it.current != nil {
			it.current.Release()
			it.current = nil
			it.getters = nil
			it.rowBuf = it.rowBuf[:0]
			it.selIdxs = it.selIdxs[:0]
			it.pos = 0
		}

		if !it.src.Next() {
			if err := it.src.Err(); err != nil && err != io.EOF {
				return nil, err
			}
			return nil, io.EOF
		}

		rec := it.src.Record()
		if rec == nil || rec.NumRows() == 0 {
			continue
		}

		rec.Retain()
		it.current = rec
		it.rebuildGetters(rec)

		if err := it.prepareSelection(goCtx); err != nil {
			return nil, err
		}
		if len(it.selIdxs) == 0 {
			continue
		}
	}
}

// Close releases all resources associated with the iterator.
func (it *ArrowRowIter) Close(*sql.Context) error {
	if it.current != nil {
		it.current.Release()
		it.current = nil
	}
	if it.plan != nil {
		it.plan.Close()
		it.plan = nil
	}
	if it.src != nil {
		it.src.Release()
		it.src = nil
	}
	it.getters = nil
	it.rowBuf = it.rowBuf[:0]
	it.selIdxs = nil
	it.pos = 0
	return nil
}

func (it *ArrowRowIter) prepareSelection(ctx context.Context) error {
	if it.current == nil {
		it.selIdxs = it.selIdxs[:0]
		it.pos = 0
		return nil
	}

	totalRows := int(it.current.NumRows())
	if totalRows == 0 {
		it.selIdxs = it.selIdxs[:0]
		it.pos = 0
		return nil
	}

	if it.plan == nil {
		it.setAllRows(totalRows)
		return nil
	}

	mask, _, err := it.plan.BuildMask(ctx, it.current)
	if err != nil {
		return err
	}
	if mask == nil {
		it.setAllRows(totalRows)
		return nil
	}

	boolMask, ok := mask.(*array.Boolean)
	if !ok {
		filtered, err := compute.FilterRecordBatch(ctx, it.current, mask, &compute.FilterOptions{NullSelection: compute.SelectionDropNulls})
		mask.Release()
		if err != nil {
			return err
		}
		it.current.Release()
		it.current = filtered
		it.rebuildGetters(it.current)
		it.setAllRows(int(it.current.NumRows()))
		return nil
	}

	selected := 0
	length := int(boolMask.Len())
	for i := 0; i < length; i++ {
		if !boolMask.IsNull(i) && boolMask.Value(i) {
			selected++
		}
	}

	if selected == 0 {
		mask.Release()
		it.selIdxs = it.selIdxs[:0]
		it.pos = 0
		return nil
	}

	if selected == length {
		mask.Release()
		it.setAllRows(length)
		return nil
	}

	selectivity := float64(selected) / float64(length)
	if selectivity >= 0.5 {
		filtered, err := compute.FilterRecordBatch(ctx, it.current, mask, &compute.FilterOptions{NullSelection: compute.SelectionDropNulls})
		mask.Release()
		if err != nil {
			return err
		}
		it.current.Release()
		it.current = filtered
		it.rebuildGetters(it.current)
		it.setAllRows(int(it.current.NumRows()))
		return nil
	}

	it.ensureIndexCap(selected)
	sel := it.selIdxs[:selected]
	idx := 0
	for i := 0; i < length; i++ {
		if !boolMask.IsNull(i) && boolMask.Value(i) {
			sel[idx] = i
			idx++
		}
	}
	it.selIdxs = sel[:idx]
	mask.Release()
	it.pos = 0
	return nil
}

func (it *ArrowRowIter) setAllRows(n int) {
	it.ensureIndexCap(n)
	sel := it.selIdxs[:n]
	for i := range sel {
		sel[i] = i
	}
	it.selIdxs = sel
	it.pos = 0
}

func (it *ArrowRowIter) ensureIndexCap(n int) {
	if cap(it.selIdxs) < n {
		it.selIdxs = make([]int, 0, n)
		return
	}
	it.selIdxs = it.selIdxs[:0]
}

type colGetter func(row int) any

func (it *ArrowRowIter) rebuildGetters(rec arrow.Record) {
	numCols := int(rec.NumCols())
	if cap(it.getters) < numCols {
		it.getters = make([]colGetter, numCols)
	}
	it.getters = it.getters[:numCols]
	for i := 0; i < numCols; i++ {
		it.getters[i] = makeGetter(rec.Column(i))
	}
	it.ensureRowBuf(numCols)
}

func (it *ArrowRowIter) ensureRowBuf(n int) {
	if n == 0 {
		if it.rowBuf == nil {
			it.rowBuf = make(sql.Row, 0)
			return
		}
		it.rowBuf = it.rowBuf[:0]
		return
	}
	if cap(it.rowBuf) < n {
		it.rowBuf = make(sql.Row, n)
		return
	}
	it.rowBuf = it.rowBuf[:n]
}

func makeGetter(col arrow.Array) colGetter {
	switch c := col.(type) {
	case *array.Int32:
		return func(r int) any {
			if c.IsNull(r) {
				return nil
			}
			return c.Value(r)
		}
	case *array.Int64:
		return func(r int) any {
			if c.IsNull(r) {
				return nil
			}
			return c.Value(r)
		}
	case *array.Uint64:
		return func(r int) any {
			if c.IsNull(r) {
				return nil
			}
			v := c.Value(r)
			if v > math.MaxInt64 {
				return int64(math.MaxInt64)
			}
			return int64(v)
		}
	case *array.Float64:
		return func(r int) any {
			if c.IsNull(r) {
				return nil
			}
			return c.Value(r)
		}
	case *array.String:
		return func(r int) any {
			if c.IsNull(r) {
				return nil
			}
			return c.Value(r)
		}
	case *array.Boolean:
		return func(r int) any {
			if c.IsNull(r) {
				return nil
			}
			return c.Value(r)
		}
	default:
		generic := col
		return func(r int) any {
			if generic.IsNull(r) {
				return nil
			}
			return defaultValueAt(generic, r)
		}
	}
}

func defaultValueAt(col arrow.Array, idx int) any {
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
	case *array.Decimal128:
		if dt, ok := arr.DataType().(*arrow.Decimal128Type); ok {
			num := arr.Value(idx)
			return decimal.NewFromBigInt(num.BigInt(), -dt.Scale)
		}
		return nil
	case *array.Decimal256:
		if dt, ok := arr.DataType().(*arrow.Decimal256Type); ok {
			num := arr.Value(idx)
			return decimal.NewFromBigInt(num.BigInt(), -dt.Scale)
		}
		return nil
	default:
		return nil
	}
}

type staticExecPlan struct {
	filters []sql.Expression
}

// NewStaticExecPlan constructs a simple ExecPlan that evaluates the provided
// filters for every record batch using Arrow compute.
func NewStaticExecPlan(filters []sql.Expression) ExecPlan {
	if len(filters) == 0 {
		return nil
	}
	copied := make([]sql.Expression, len(filters))
	copy(copied, filters)
	return &staticExecPlan{filters: copied}
}

func (p *staticExecPlan) BuildMask(ctx context.Context, rec arrow.Record) (arrow.Array, float64, error) {
	if len(p.filters) == 0 || rec == nil || rec.NumRows() == 0 {
		return nil, 1, nil
	}

	mask, err := BuildMaskForBatch(ctx, rec, p.filters)
	if err != nil {
		return nil, 0, err
	}
	if mask == nil {
		return nil, 1, nil
	}

	boolMask, ok := mask.(*array.Boolean)
	if !ok {
		return mask, 1, nil
	}

	total := int(boolMask.Len())
	if total == 0 {
		return mask, 0, nil
	}

	selected := 0
	for i := 0; i < total; i++ {
		if !boolMask.IsNull(i) && boolMask.Value(i) {
			selected++
		}
	}

	if selected == total {
		mask.Release()
		return nil, 1, nil
	}

	return mask, float64(selected) / float64(total), nil
}

func (p *staticExecPlan) Close() {
	p.filters = nil
}
