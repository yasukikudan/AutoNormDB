package arrowtable

// Database 型は Arrow テーブル群を go-mysql-server に公開するための薄いラッパーです。
// 読み取り専用である点を明示しつつ、テーブルの登録・検索・列挙といった基本操作だけを
// 提供します。内部ではテーブル名を小文字化して保存することで、名前比較を大小文字非依存に
// しています。

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Database is a simple read-only implementation of sql.Database backed by Arrow
// tables.
type Database struct {
	name   string
	mu     sync.RWMutex
	tables map[string]sql.Table
}

// NewDatabase constructs a Database with the provided name.
func NewDatabase(name string) *Database {
	return &Database{
		name:   name,
		tables: make(map[string]sql.Table),
	}
}

// Name implements sql.Nameable.
func (db *Database) Name() string {
	// go-mysql-server は Database インターフェース経由で名称を参照するため、保持している
	// 名前をそのまま返します。
	return db.name
}

// IsReadOnly reports that Arrow-backed databases are read-only.
func (db *Database) IsReadOnly() bool {
	// Arrow テーブルはイミュータブルに扱うため、書き込み操作が存在しないことを示します。
	return true
}

// AddTable registers the provided table with the database.
func (db *Database) AddTable(t sql.Table) error {
	// 同時登録に備えて排他ロックを取得し、テーブル名の小文字版をキーとして登録します。
	db.mu.Lock()
	defer db.mu.Unlock()

	name := t.Name()
	lower := strings.ToLower(name)
	if _, ok := db.tables[lower]; ok {
		return sql.ErrTableAlreadyExists.New(name)
	}
	db.tables[lower] = t
	return nil
}

// GetTableInsensitive implements sql.Database.
func (db *Database) GetTableInsensitive(_ *sql.Context, tblName string) (sql.Table, bool, error) {
	// 読み取りロックを取得し、大小文字を無視したテーブル名検索を行います。存在しない場合は
	// 第二戻り値を false として返し、エラーは発生させません。
	db.mu.RLock()
	defer db.mu.RUnlock()

	tbl, ok := db.tables[strings.ToLower(tblName)]
	if !ok {
		return nil, false, nil
	}
	return tbl, true, nil
}

// GetTableNames implements sql.Database.
func (db *Database) GetTableNames(*sql.Context) ([]string, error) {
	// テーブル名のスライスを作成し、決定的な順序で返すために sort.Strings で辞書順ソートします。
	db.mu.RLock()
	defer db.mu.RUnlock()

	names := make([]string, 0, len(db.tables))
	for _, tbl := range db.tables {
		names = append(names, tbl.Name())
	}
	sort.Strings(names)
	return names, nil
}

// MustAddTable registers the table and panics if the name already exists. This
// is a convenience helper for loaders that expect unique table names.
func (db *Database) MustAddTable(t sql.Table) {
	// AddTable でエラーが発生した場合は panic を投げ、呼び出し元で重複登録などの異常系を
	// 早期に検知できるようにしています。
	if err := db.AddTable(t); err != nil {
		panic(fmt.Errorf("add table %s: %w", t.Name(), err))
	}
}
