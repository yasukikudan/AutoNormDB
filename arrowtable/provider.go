package arrowtable

// Provider 型は go-mysql-server が要求する sql.DatabaseProvider インターフェースの
// 最小実装であり、Arrow ベースのデータベースを複数まとめて管理します。MySQL クライアント
// からの接続時にデータベースを名前で引き当てる役割を担います。

import (
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
)

// Provider is a minimal sql.DatabaseProvider for Arrow-backed databases.
type Provider struct {
	mu  sync.RWMutex
	dbs map[string]sql.Database
}

// NewProvider constructs a Provider containing the supplied databases.
func NewProvider(dbs ...sql.Database) *Provider {
	// 事前に容量を確保したマップを生成し、渡されたデータベースを名前の小文字版で格納します。
	// MySQL はデータベース名の大小文字扱いが環境によって異なるため、ここでは大小文字を無視した
	// マッチングを保証するために小文字キーを採用しています。
	p := &Provider{dbs: make(map[string]sql.Database, len(dbs))}
	for _, db := range dbs {
		p.dbs[strings.ToLower(db.Name())] = db
	}
	return p
}

// Database implements sql.DatabaseProvider.
func (p *Provider) Database(_ *sql.Context, name string) (sql.Database, error) {
	// 共有マップに対する読み取りのみなので RLock を取得し、同時アクセスを許可します。
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
	// Database と同様に読み取りロックでガードし、存在確認を大小文字非依存で行います。
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.dbs[strings.ToLower(name)]
	return ok
}

// AllDatabases implements sql.DatabaseProvider.
func (p *Provider) AllDatabases(*sql.Context) []sql.Database {
	// 読み取りロックを取得したうえでマップの値をスライスにコピーし、安定した順序で返すために
	// 名前でソートします。go-mysql-server は返却順序に依存しないものの、決定的な並びの方が
	// デバッグ時に扱いやすくなります。
	p.mu.RLock()
	defer p.mu.RUnlock()

	dbs := make([]sql.Database, 0, len(p.dbs))
	for _, db := range p.dbs {
		dbs = append(dbs, db)
	}
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name() < dbs[j].Name() })
	return dbs
}
