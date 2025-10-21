package database

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Database は go-mysql-server の sql.Database を最小限に実装した読み取り専用の
// データベースです。テーブルやその断片を「パーティション」と見なし、名前を
// 大文字小文字を区別せずに管理することで、Arrow や Parquet など異なる実装を
// 一つのデータベースに統合します。
type Database struct {
	name       string
	mu         sync.RWMutex
	partitions map[string]sql.Table
}

// NewDatabase は指定した名称を持つ空のデータベースを作成します。
func NewDatabase(name string) *Database {
	return &Database{
		name:       name,
		partitions: make(map[string]sql.Table),
	}
}

// Name は sql.Nameable を実装し、データベース名を返します。
func (db *Database) Name() string { return db.name }

// IsReadOnly はローダーが作成するデータベースが読み取り専用であることを示します。
func (db *Database) IsReadOnly() bool { return true }

// Partitions は登録されているパーティションを元の名称でコピーして返します。
// テスト支援用であり、返却されたマップを変更しても内部状態には影響しません。
func (db *Database) Partitions() map[string]sql.Table {
	db.mu.RLock()
	defer db.mu.RUnlock()

	out := make(map[string]sql.Table, len(db.partitions))
	for _, tbl := range db.partitions {
		out[tbl.Name()] = tbl
	}
	return out
}

// Tables は後方互換性のために公開されるパーティション一覧の別名です。
func (db *Database) Tables() map[string]sql.Table { return db.Partitions() }

// AddPartition はパーティション（テーブル）を登録します。名称は大文字小文字を
// 区別せずに保持され、同名のパーティションが既に存在する場合はエラーになります。
func (db *Database) AddPartition(t sql.Table) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	key := strings.ToLower(t.Name())
	if _, ok := db.partitions[key]; ok {
		return sql.ErrTableAlreadyExists.New(t.Name())
	}
	db.partitions[key] = t
	return nil
}

// AddTable は後方互換性のために提供されるラッパーで、AddPartition を呼び出します。
func (db *Database) AddTable(t sql.Table) error { return db.AddPartition(t) }

// MustAddPartition はパーティションを登録し、失敗した場合は panic します。
func (db *Database) MustAddPartition(t sql.Table) {
	if err := db.AddPartition(t); err != nil {
		panic(fmt.Errorf("add partition %s: %w", t.Name(), err))
	}
}

// MustAddTable は後方互換性のために提供されるラッパーで、MustAddPartition を呼び出します。
func (db *Database) MustAddTable(t sql.Table) { db.MustAddPartition(t) }

// GetTableInsensitive は名称を大文字小文字を区別せずに解決し、該当するパーティションを返します。
func (db *Database) GetTableInsensitive(_ *sql.Context, tblName string) (sql.Table, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	tbl, ok := db.partitions[strings.ToLower(tblName)]
	if !ok {
		return nil, false, nil
	}
	return tbl, true, nil
}

// GetTableNames は登録済みパーティションの名称を昇順で返します。
func (db *Database) GetTableNames(*sql.Context) ([]string, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.partitions))
	for _, tbl := range db.partitions {
		names = append(names, tbl.Name())
	}
	sort.Strings(names)
	return names, nil
}
