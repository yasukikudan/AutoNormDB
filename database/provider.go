package database

import (
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Provider is a minimal sql.DatabaseProvider that manages a set of databases by
// case-insensitive name.
type Provider struct {
	mu  sync.RWMutex
	dbs map[string]sql.Database
}

// NewProvider constructs a Provider containing the supplied databases.
func NewProvider(dbs ...sql.Database) *Provider {
	p := &Provider{dbs: make(map[string]sql.Database, len(dbs))}
	for _, db := range dbs {
		p.dbs[strings.ToLower(db.Name())] = db
	}
	return p
}

// Database implements sql.DatabaseProvider.
func (p *Provider) Database(_ *sql.Context, name string) (sql.Database, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	db, ok := p.dbs[strings.ToLower(name)]
	if ok {
		return db, nil
	}
	return nil, sql.ErrDatabaseNotFound.New(name)
}

// HasDatabase implements sql.DatabaseProvider.
func (p *Provider) HasDatabase(_ *sql.Context, name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.dbs[strings.ToLower(name)]
	return ok
}

// AllDatabases implements sql.DatabaseProvider.
func (p *Provider) AllDatabases(*sql.Context) []sql.Database {
	p.mu.RLock()
	defer p.mu.RUnlock()

	dbs := make([]sql.Database, 0, len(p.dbs))
	for _, db := range p.dbs {
		dbs = append(dbs, db)
	}
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name() < dbs[j].Name() })
	return dbs
}
