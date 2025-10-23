package arrowbackend

import (
	"math"
	"testing"

	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/memory"
)

func TestMakeGetterUint64(t *testing.T) {
	builder := array.NewUint64Builder(memory.DefaultAllocator)
	t.Cleanup(builder.Release)

	builder.Append(1)
	builder.AppendNull()
	builder.Append(math.MaxUint64)

	arr := builder.NewArray()
	t.Cleanup(arr.Release)

	getter := makeGetter(arr)

	if v := getter(0); v != uint64(1) {
		t.Fatalf("expected uint64(1), got %v (%T)", v, v)
	}
	if v := getter(1); v != nil {
		t.Fatalf("expected nil for null, got %v (%T)", v, v)
	}
	if v := getter(2); v != uint64(math.MaxUint64) {
		t.Fatalf("expected uint64(MaxUint64), got %v (%T)", v, v)
	}
}

func TestMakeGetterUint32(t *testing.T) {
	builder := array.NewUint32Builder(memory.DefaultAllocator)
	t.Cleanup(builder.Release)

	builder.Append(42)

	arr := builder.NewArray()
	t.Cleanup(arr.Release)

	getter := makeGetter(arr)
	if v := getter(0); v != uint32(42) {
		t.Fatalf("expected uint32(42), got %v (%T)", v, v)
	}
}
