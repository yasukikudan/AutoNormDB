package arrowfile

import (
	"container/list"
	"sync"
	"time"

	"github.com/apache/arrow/go/v15/arrow"
)

// TableCache is an optional LRU cache for Arrow tables keyed by path.
type TableCache struct {
	mu         sync.Mutex
	maxEntries int
	ttl        time.Duration
	entries    map[string]*cacheEntry
	order      *list.List
}

type cacheEntry struct {
	key       string
	table     arrow.Table
	expiresAt time.Time
	element   *list.Element
}

// NewTableCache constructs a cache storing up to maxEntries tables.
func NewTableCache(maxEntries int, ttl time.Duration) *TableCache {
	if maxEntries <= 0 {
		return nil
	}
	return &TableCache{
		maxEntries: maxEntries,
		ttl:        ttl,
		entries:    make(map[string]*cacheEntry),
		order:      list.New(),
	}
}

// Get fetches a table from the cache and retains it when found.
func (c *TableCache) Get(key string) (arrow.Table, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	ent, ok := c.entries[key]
	if !ok {
		return nil, false
	}
	if c.ttl > 0 && time.Now().After(ent.expiresAt) {
		c.removeEntry(ent)
		return nil, false
	}
	c.order.MoveToFront(ent.element)
	ent.table.Retain()
	return ent.table, true
}

// Set stores the provided table under the key. The table is retained by the
// cache for the duration of its lifetime.
func (c *TableCache) Set(key string, tbl arrow.Table) {
	if c == nil || tbl == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	if ent, ok := c.entries[key]; ok {
		ent.table.Release()
		ent.table = tbl
		ent.table.Retain()
		ent.expiresAt = c.expiry()
		c.order.MoveToFront(ent.element)
		return
	}

	ent := &cacheEntry{key: key, table: tbl, expiresAt: c.expiry()}
	ent.table.Retain()
	ent.element = c.order.PushFront(ent)
	c.entries[key] = ent
	c.enforceLimit()
}

// Delete removes a cached entry if present.
func (c *TableCache) Delete(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	ent, ok := c.entries[key]
	if !ok {
		return
	}
	c.removeEntry(ent)
}

// Close releases all cached tables.
func (c *TableCache) Close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, ent := range c.entries {
		if ent.table != nil {
			ent.table.Release()
		}
	}
	c.entries = make(map[string]*cacheEntry)
	c.order.Init()
}

func (c *TableCache) expiry() time.Time {
	if c.ttl <= 0 {
		return time.Time{}
	}
	return time.Now().Add(c.ttl)
}

func (c *TableCache) enforceLimit() {
	for len(c.entries) > c.maxEntries {
		back := c.order.Back()
		if back == nil {
			break
		}
		ent := back.Value.(*cacheEntry)
		c.removeEntry(ent)
	}
}

func (c *TableCache) removeEntry(ent *cacheEntry) {
	delete(c.entries, ent.key)
	if ent.element != nil {
		c.order.Remove(ent.element)
	}
	if ent.table != nil {
		ent.table.Release()
	}
}
