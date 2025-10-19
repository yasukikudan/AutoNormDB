package arrowtable

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
	return db.name
}

// IsReadOnly reports that Arrow-backed databases are read-only.
func (db *Database) IsReadOnly() bool {
	return true
}

// AddTable registers the provided table with the database.
func (db *Database) AddTable(t sql.Table) error {
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
	if err := db.AddTable(t); err != nil {
		panic(fmt.Errorf("add table %s: %w", t.Name(), err))
	}
}
