package arrowtable

import (
	"encoding/binary"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/dolthub/go-mysql-server/sql"
)

// ArrowBackedTable exposes an in-memory Arrow table through the sql.Table
// interface expected by go-mysql-server. Each Arrow RecordBatch (chunk) becomes
// a partition that the engine can iterate independently.
type ArrowBackedTable struct {
	name     string
	schema   sql.Schema
	arrTable arrow.Table
}

// NewArrowBackedTable wraps the provided Arrow table. The Arrow table is
// retained so that it remains valid for the lifetime of the ArrowBackedTable.
func NewArrowBackedTable(name string, arrTable arrow.Table) (*ArrowBackedTable, error) {
	schema, err := ArrowSchemaToSQLSchema(arrTable.Schema(), name)
	if err != nil {
		return nil, err
	}

	arrTable.Retain()
	return &ArrowBackedTable{name: name, schema: schema, arrTable: arrTable}, nil
}

// Name implements sql.Table.
func (t *ArrowBackedTable) Name() string { return t.name }

// String implements fmt.Stringer for debugging convenience.
func (t *ArrowBackedTable) String() string { return t.name }

// Schema implements sql.Table.
func (t *ArrowBackedTable) Schema() sql.Schema { return t.schema }

type chunkPartition struct {
	idx int
}

func (p *chunkPartition) Key() []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(p.idx))
	return buf
}

// Partitions implements sql.Table. Each Arrow chunk becomes one partition.
func (t *ArrowBackedTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	numChunks := 0
	if t.arrTable.NumCols() > 0 {
		numChunks = len(t.arrTable.Column(0).Data().Chunks())
	} else if t.arrTable.NumRows() > 0 {
		// Tables without columns can still report logical rows. Expose a
		// single partition so that PartitionRows can return empty rows
		// while honouring the reported row count.
		numChunks = 1
	}

	if numChunks == 0 {
		return sql.PartitionsToPartitionIter(), nil
	}

	parts := make([]sql.Partition, 0, numChunks)
	for i := 0; i < numChunks; i++ {
		parts = append(parts, &chunkPartition{idx: i})
	}
	return sql.PartitionsToPartitionIter(parts...), nil
}

// PartitionRows implements sql.PartitionedTable.
func (t *ArrowBackedTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	_ = ctx
	cp := p.(*chunkPartition)
	return newArrowRowIter(t.arrTable, cp.idx), nil
}

// Collation implements sql.Table Collation support; Arrow arrays are
// byte-oriented so we return the default collation.
func (t *ArrowBackedTable) Collation() sql.CollationID {
	return sql.Collation_Default
}
