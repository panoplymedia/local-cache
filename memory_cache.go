package cache

import (
	"sync"
	"time"
)

type MemoryCache struct {
	Dat map[string]cacheElement
	mu  sync.Mutex
}

type cacheElement struct {
	expiresAt time.Time
	dat       []byte
}

func newMemoryCache() *MemoryCache {
	d := map[string]cacheElement{}

	var m MemoryCache
	m.Dat = d
	return &m
}

func (m MemoryCache) Read(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	el, exists := m.Dat[key]
	if exists && time.Now().UTC().Before(el.expiresAt) {
		return el.dat, true
	} else if exists {
		// evict key since it exists and it's expired
		delete(m.Dat, key)
	}
	return []byte{}, false
}

func (m MemoryCache) Write(key string, val []byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var e time.Time
	if ttl == 0 {
		e = time.Unix(1<<63-1, 0)
	} else {
		e = time.Now().UTC().Add(ttl)
	}

	c := cacheElement{
		expiresAt: e,
		dat:       val,
	}

	m.Dat[key] = c
}
