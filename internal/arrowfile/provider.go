package arrowfile

import (
	"time"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

// ProviderConfig captures the settings used to expose file-backed tables
// through a sql.DatabaseProvider.
type ProviderConfig struct {
	// DatabaseName is the logical name clients will use when referencing
	// the database. When left empty it defaults to "AutoNormDB".
	DatabaseName string
	// AllowedRoots constrains which paths may be queried. When empty the
	// current directory is allowed.
	AllowedRoots []string
	// EnableGlob toggles support for wildcard file matching.
	EnableGlob bool
	// CacheEntries controls the optional Arrow table cache size. When zero
	// or negative no cache is used.
	CacheEntries int
	// CacheTTL specifies how long cached tables remain valid. A non-positive
	// duration keeps entries indefinitely.
	CacheTTL time.Duration
}

// NewProvider constructs a sql.DatabaseProvider backed by a FileDatabase using
// the supplied configuration. Both the server process and CLI reuse this helper
// so file resolution behaves identically regardless of the interface.
func NewProvider(cfg ProviderConfig) sql.DatabaseProvider {
	name := cfg.DatabaseName
	if name == "" {
		name = "AutoNormDB"
	}

	db := NewFileDatabase(name)
	db.AllowedRoots = append([]string(nil), cfg.AllowedRoots...)
	db.EnableGlob = cfg.EnableGlob
	if cfg.CacheEntries > 0 {
		db.Cache = NewTableCache(cfg.CacheEntries, cfg.CacheTTL)
	}

	if len(db.AllowedRoots) == 0 {
		db.AllowedRoots = []string{"."}
	}

	return memory.NewDBProvider(db)
}
