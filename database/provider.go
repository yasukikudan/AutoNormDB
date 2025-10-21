package database

import (
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Provider は名称を大文字小文字を区別せずに管理する最小構成の sql.DatabaseProvider です。
// 異なる種類のデータベースを一つのプロバイダーに統合し、利用側からは一貫した解決方法を提供します。
type Provider struct {
	mu  sync.RWMutex
	dbs map[string]sql.Database
}

// NewProvider は任意個のデータベースを受け取り、名称をキーとした Provider を作成します。
func NewProvider(dbs ...sql.Database) *Provider {
	p := &Provider{dbs: make(map[string]sql.Database, len(dbs))}
	for _, db := range dbs {
		p.dbs[strings.ToLower(db.Name())] = db
	}
	return p
}

// Database は sql.DatabaseProvider を実装し、名称に対応するデータベースを返します。
// 該当する名称が存在しない場合は ErrDatabaseNotFound を返します。
func (p *Provider) Database(_ *sql.Context, name string) (sql.Database, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	db, ok := p.dbs[strings.ToLower(name)]
	if ok {
		return db, nil
	}
	return nil, sql.ErrDatabaseNotFound.New(name)
}

// HasDatabase は名称に対応するデータベースの存在有無だけを判定します。
func (p *Provider) HasDatabase(_ *sql.Context, name string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.dbs[strings.ToLower(name)]
	return ok
}

// AllDatabases はプロバイダーが管理するデータベースを名称順に返します。
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
