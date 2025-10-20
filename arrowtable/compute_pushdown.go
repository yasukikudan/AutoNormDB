package arrowtable

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/compute"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
)

// buildMaskForBatch converts the pushed down SQL expressions into Arrow compute
// predicates and combines them using AND semantics. The resulting boolean array
// can be consumed by FilterRecordBatch to physically prune rows from the
// provided record batch.
func buildMaskForBatch(ctx context.Context, rb arrow.Record, pushed []sql.Expression) (arrow.Array, error) {
	if len(pushed) == 0 {
		builder := array.NewBooleanBuilder(memory.DefaultAllocator)
		defer builder.Release()
		vals := make([]bool, rb.NumRows())
		for i := range vals {
			vals[i] = true
		}
		builder.AppendValues(vals, nil)
		return builder.NewArray(), nil
	}

	var combined compute.Datum
	for i, expr := range pushed {
		d, err := compileOnePredicate(ctx, rb, expr)
		if err != nil {
			if combined != nil {
				combined.Release()
			}
			return nil, err
		}

		if i == 0 {
			combined = d
			continue
		}

		out, err := compute.CallFunction(ctx, "and_kleene", nil, combined, d)
		combined.Release()
		d.Release()
		if err != nil {
			return nil, err
		}
		combined = out
	}

	defer combined.Release()

	arrDatum, ok := combined.(*compute.ArrayDatum)
	if !ok {
		return nil, fmt.Errorf("unexpected datum type %T for filter output", combined)
	}
	return arrDatum.MakeArray(), nil
}

func compileOnePredicate(ctx context.Context, rb arrow.Record, pred sql.Expression) (compute.Datum, error) {
	switch p := pred.(type) {
	case *expression.Equals:
		return compare(ctx, rb, p.Left(), p.Right(), "equal")
	case *expression.GreaterThan:
		return compare(ctx, rb, p.Left(), p.Right(), "greater")
	case *expression.GreaterThanOrEqual:
		return compare(ctx, rb, p.Left(), p.Right(), "greater_equal")
	case *expression.LessThan:
		return compare(ctx, rb, p.Left(), p.Right(), "less")
	case *expression.LessThanOrEqual:
		return compare(ctx, rb, p.Left(), p.Right(), "less_equal")
	case *expression.Between:
		lower, err := compare(ctx, rb, p.Val, p.Lower, "greater_equal")
		if err != nil {
			return nil, err
		}
		upper, err := compare(ctx, rb, p.Val, p.Upper, "less_equal")
		if err != nil {
			lower.Release()
			return nil, err
		}
		out, err := compute.CallFunction(ctx, "and_kleene", nil, lower, upper)
		lower.Release()
		upper.Release()
		if err != nil {
			return nil, err
		}
		return out, nil
	case *expression.And:
		left, err := compileOnePredicate(ctx, rb, p.LeftChild)
		if err != nil {
			return nil, err
		}
		right, err := compileOnePredicate(ctx, rb, p.RightChild)
		if err != nil {
			left.Release()
			return nil, err
		}
		out, err := compute.CallFunction(ctx, "and_kleene", nil, left, right)
		left.Release()
		right.Release()
		if err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("predicate %T", pred))
	}
}

func compare(ctx context.Context, rb arrow.Record, left, right sql.Expression, op string) (compute.Datum, error) {
	ld, err := toDatum(rb, left)
	if err != nil {
		return nil, err
	}
	rd, err := toDatum(rb, right)
	if err != nil {
		ld.Release()
		return nil, err
	}

	ld, rd, err = alignDatumTypes(ctx, ld, rd)
	if err != nil {
		ld.Release()
		rd.Release()
		return nil, err
	}

	out, err := compute.CallFunction(ctx, op, nil, ld, rd)
	ld.Release()
	rd.Release()
	if err != nil {
		return nil, err
	}
	return out, nil
}

func toDatum(rb arrow.Record, expr sql.Expression) (compute.Datum, error) {
	switch e := expr.(type) {
	case *expression.GetField:
		name := e.Name()
		idxs := rb.Schema().FieldIndices(name)
		if len(idxs) == 0 {
			return nil, sql.ErrTableColumnNotFound.New(name)
		}
		col := rb.Column(idxs[0])
		return compute.NewDatum(col), nil
	case *expression.Literal:
		return compute.NewDatum(e.Val), nil
	default:
		return nil, sql.ErrUnsupportedFeature.New(fmt.Sprintf("expression %T", expr))
	}
}

func alignDatumTypes(ctx context.Context, left, right compute.Datum) (compute.Datum, compute.Datum, error) {
	lt := datumType(left)
	rt := datumType(right)
	if lt == nil || rt == nil || arrow.TypeEqual(lt, rt) {
		return left, right, nil
	}

	if _, ok := left.(*compute.ScalarDatum); ok {
		casted, err := compute.CastDatum(ctx, left, compute.SafeCastOptions(rt))
		if err != nil {
			return left, right, err
		}
		left.Release()
		left = casted
		lt = datumType(left)
	}

	if _, ok := right.(*compute.ScalarDatum); ok && !arrow.TypeEqual(lt, datumType(right)) {
		casted, err := compute.CastDatum(ctx, right, compute.SafeCastOptions(lt))
		if err != nil {
			return left, right, err
		}
		right.Release()
		right = casted
		rt = datumType(right)
	}

	if arrow.TypeEqual(lt, rt) {
		return left, right, nil
	}

	if casted, err := compute.CastDatum(ctx, right, compute.SafeCastOptions(lt)); err == nil {
		right.Release()
		right = casted
		return left, right, nil
	}
	if casted, err := compute.CastDatum(ctx, left, compute.SafeCastOptions(rt)); err == nil {
		left.Release()
		left = casted
		return left, right, nil
	}

	return left, right, fmt.Errorf("unable to align Arrow datum types: %s vs %s", lt, rt)
}

func datumType(d compute.Datum) arrow.DataType {
	switch v := d.(type) {
	case compute.ArrayLikeDatum:
		return v.Type()
	case *compute.ScalarDatum:
		return v.Type()
	default:
		return nil
	}
}
