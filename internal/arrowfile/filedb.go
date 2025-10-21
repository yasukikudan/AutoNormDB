package arrowfile

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/dolthub/go-mysql-server/sql"

	"AutoNormDb/table/arrowtable"
)

// FileDatabase resolves table names to filesystem paths or glob patterns and
// exposes them as sql.Table instances backed by Arrow tables.
type FileDatabase struct {
	name         string
	AllowedRoots []string
	EnableGlob   bool
	Cache        *TableCache
}

// NewFileDatabase constructs a database with the given name.
func NewFileDatabase(name string) *FileDatabase {
	return &FileDatabase{name: name}
}

// Name implements sql.Nameable.
func (db *FileDatabase) Name() string { return db.name }

// IsReadOnly reports that file-backed tables do not support mutation.
func (db *FileDatabase) IsReadOnly() bool { return true }

// GetTableInsensitive resolves the supplied table name as a path or glob
// pattern. Non-path names fall through so other databases may resolve them.
func (db *FileDatabase) GetTableInsensitive(ctx *sql.Context, name string) (sql.Table, bool, error) {
	_ = ctx
	raw := normalizeName(name)
	if !isPathLike(raw) {
		return nil, false, nil
	}

	absPattern, err := filepath.Abs(raw)
	if err != nil {
		return nil, false, err
	}
	absPattern = filepath.Clean(absPattern)

	paths, err := resolvePaths(absPattern, db.EnableGlob)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, err
		}
		return nil, false, err
	}
	if len(paths) == 0 {
		return nil, false, fmt.Errorf("no files matched pattern")
	}

	normalized := make([]string, len(paths))
	for i, p := range paths {
		abs, err := filepath.Abs(p)
		if err != nil {
			return nil, false, err
		}
		abs = filepath.Clean(abs)
		if err := ensureAllowed(abs, db.AllowedRoots); err != nil {
			return nil, false, err
		}
		if !isSupportedExtension(abs) {
			return nil, false, fmt.Errorf("unsupported file type: %s (allowed: .parquet, .feather, .arrow)", strings.ToLower(filepath.Ext(abs)))
		}
		normalized[i] = abs
	}
	sort.Strings(normalized)

	key := cacheKeyFromPaths(normalized)
	if tbl, ok := db.Cache.Get(key); ok {
		wrapped, err := arrowtable.NewArrowBackedTable(raw, tbl)
		tbl.Release()
		if err != nil {
			return nil, false, err
		}
		return wrapped, true, nil
	}

	pool := memory.NewGoAllocator()
	table, err := LoadArrowTable(normalized, pool)
	if err != nil {
		return nil, false, err
	}

	if db.Cache != nil {
		db.Cache.Set(key, table)
	}

	wrapped, err := arrowtable.NewArrowBackedTable(raw, table)
	table.Release()
	if err != nil {
		if db.Cache != nil {
			db.Cache.Delete(key)
		}
		return nil, false, err
	}
	return wrapped, true, nil
}

// GetTableNames reports no pre-defined tables.
func (db *FileDatabase) GetTableNames(*sql.Context) ([]string, error) {
	return nil, nil
}

func cacheKeyFromPaths(paths []string) string {
	h := sha256.New()
	for _, p := range paths {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func normalizeName(name string) string {
	if len(name) < 2 {
		return name
	}
	if (name[0] == '\'' && name[len(name)-1] == '\'') ||
		(name[0] == '"' && name[len(name)-1] == '"') ||
		(name[0] == '`' && name[len(name)-1] == '`') {
		return name[1 : len(name)-1]
	}
	return name
}

func isSupportedExtension(path string) bool {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".parquet", ".feather", ".arrow":
		return true
	default:
		return false
	}
}

func isPathLike(name string) bool {
	if name == "" {
		return false
	}
	if strings.HasPrefix(name, "./") || strings.HasPrefix(name, "../") || strings.HasPrefix(name, "/") {
		return true
	}
	if strings.Contains(name, "\\") {
		return true
	}
	return isSupportedExtension(name)
}

func ensureAllowed(path string, roots []string) error {
	if len(roots) == 0 {
		roots = []string{"."}
	}
	for _, root := range roots {
		if root == "" {
			root = "."
		}
		absRoot, err := filepath.Abs(root)
		if err != nil {
			continue
		}
		absRoot = filepath.Clean(absRoot)
		if path == absRoot {
			return nil
		}
		if strings.HasPrefix(path+string(os.PathSeparator), absRoot+string(os.PathSeparator)) {
			return nil
		}
	}
	return fmt.Errorf("permission denied: path not under allowed roots")
}

func resolvePaths(raw string, enableGlob bool) ([]string, error) {
	if !enableGlob || !hasGlob(raw) {
		return []string{raw}, nil
	}
	matches, err := filepath.Glob(raw)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no files matched pattern")
	}
	return matches, nil
}

func hasGlob(path string) bool {
	return strings.ContainsAny(path, "*?[]")
}
