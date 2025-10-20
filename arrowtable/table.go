package arrowtable

// このファイルでは Apache Arrow のテーブルを go-mysql-server 互換の sql.Table として
// 露出させるラッパーを提供します。Arrow のレコードバッチを go-mysql-server のパーティション
// にマッピングし、SQL 実行エンジンから効率的に行イテレーションできるようにしています。

import (
	"encoding/binary"

	"github.com/apache/arrow/go/v15/arrow"
	"github.com/dolthub/go-mysql-server/sql"
)

// ArrowBackedTable exposes an in-memory Arrow table through the sql.Table
// interface expected by go-mysql-server. Each Arrow RecordBatch (chunk) becomes
// a partition that the engine can iterate independently.
// ArrowBackedTable は、go-mysql-server が期待する sql.Table インターフェースを
// 介してメモリ上の Arrow テーブルを公開します。各 Arrow RecordBatch（チャンク）が
// 1 つのパーティションとなり、エンジンはそれぞれを独立して走査できます。
type ArrowBackedTable struct {
	name     string
	schema   sql.Schema
	arrTable arrow.Table
}

// NewArrowBackedTable wraps the provided Arrow table. The Arrow table is
// retained so that it remains valid for the lifetime of the ArrowBackedTable.
// NewArrowBackedTable は引数として受け取った Arrow テーブルをラップします。
// Arrow テーブルは Retain され、ArrowBackedTable のライフタイム全体で有効な
// 参照が保たれるようにしています。
func NewArrowBackedTable(name string, arrTable arrow.Table) (*ArrowBackedTable, error) {
	schema, err := ArrowSchemaToSQLSchema(arrTable.Schema(), name)
	if err != nil {
		return nil, err
	}

	arrTable.Retain()
	return &ArrowBackedTable{name: name, schema: schema, arrTable: arrTable}, nil
}

// Name implements sql.Table.
// sql.Table インターフェースで要求される Name メソッドを実装し、テーブル名を返します。
func (t *ArrowBackedTable) Name() string { return t.name }

// String implements fmt.Stringer for debugging convenience.
// fmt.Stringer を実装しており、デバッグ時にテーブル名を文字列として扱いやすくします。
func (t *ArrowBackedTable) String() string { return t.name }

// Schema implements sql.Table.
// sql.Table の Schema メソッドを実装し、SQL 用のスキーマ情報を返します。
func (t *ArrowBackedTable) Schema() sql.Schema { return t.schema }

type chunkPartition struct {
	idx int
}

func (p *chunkPartition) Key() []byte {
	// go-mysql-server ではパーティションの識別子をバイト列で返す必要があるため、チャンク番号を
	// 32bit のビッグエンディアン整数としてエンコードします。これにより安定した順序付けが可能です。
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(p.idx))
	return buf
}

// Partitions implements sql.Table. Each Arrow chunk becomes one partition.
// Partitions は sql.Table の要件を満たし、各 Arrow チャンクを 1 つのパーティションとして扱います。
// Arrow の列データが存在する場合はチャンク数を基準に、列がなく行数のみがある場合は
// 論理的な行数に対応する 1 つのパーティションを用意することで、空行を返しつつ行数を保証します。
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
// sql.PartitionedTable の PartitionRows を実装し、指定されたパーティションに対応する
// Arrow データから行イテレータを生成します。
func (t *ArrowBackedTable) PartitionRows(ctx *sql.Context, p sql.Partition) (sql.RowIter, error) {
	_ = ctx
	cp := p.(*chunkPartition)
	// 指定されたパーティション（＝ Arrow のチャンク）に対応する行イテレータを生成します。
	// newArrowRowIter はチャンク内の列配列を Retain し、呼び出し側の Close で解放する設計です。
	return newArrowRowIter(t.arrTable, cp.idx), nil
}

// Collation implements sql.Table Collation support; Arrow arrays are
// byte-oriented so we return the default collation.
// Collation は sql.Table の照合順序サポートを実装します。Arrow 配列はバイト配列ベースで
// 表現されるため、デフォルトの照合順序（Collation_Default）を返します。
func (t *ArrowBackedTable) Collation() sql.CollationID {
	return sql.Collation_Default
}
