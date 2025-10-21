package database

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Database is a minimal read-only sql.Database implementation that can store
// heterogeneous sql.Table implementations. Tables are keyed case-insensitively
// so that callers can register arrow-backed, Parquet-backed, or other
// go-mysql-server compatible tables under a single database instance.
type Database struct {
	name   string
	mu     sync.RWMutex
	tables map[string]sql.Table
}

// NewDatabase constructs an empty Database with the given name.
func NewDatabase(name string) *Database {
	return &Database{
		name:   name,
		tables: make(map[string]sql.Table),
	}
}

// Name implements sql.Nameable.
func (db *Database) Name() string { return db.name }

// IsReadOnly reports that loader-created databases do not support mutation.
func (db *Database) IsReadOnly() bool { return true }

// Tables returns a copy of the registered tables keyed by their original
// names. It is primarily intended for test helpers.
func (db *Database) Tables() map[string]sql.Table {
	db.mu.RLock()
	defer db.mu.RUnlock()

	out := make(map[string]sql.Table, len(db.tables))
	for _, tbl := range db.tables {
		out[tbl.Name()] = tbl
	}
	return out
}

// AddTable registers a new table with the database.
func (db *Database) AddTable(t sql.Table) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	key := strings.ToLower(t.Name())
	if _, ok := db.tables[key]; ok {
		return sql.ErrTableAlreadyExists.New(t.Name())
	}
	db.tables[key] = t
	return nil
}

// MustAddTable registers the table and panics if a table with the same name is
// already present.
func (db *Database) MustAddTable(t sql.Table) {
	if err := db.AddTable(t); err != nil {
		panic(fmt.Errorf("add table %s: %w", t.Name(), err))
	}
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
