package cache

import (
	"strings"
	"sync"
	"time"
)

type MemoryCache struct {
	Dat      [26]map[string]cacheElement
	mu       [26]sync.RWMutex
	KeyCount uint64
}

type cacheElement struct {
	expiresAt time.Time
	dat       []byte
}

func newMemoryCache() *MemoryCache {
	var m MemoryCache
	for i := 0; i < 26; i++ {
		d := map[string]cacheElement{}
		m.Dat[i] = d
	}
	return &m
}

func (m *MemoryCache) Read(key string) ([]byte, bool) {
	idx := keyToShard(key)

	m.mu[idx].RLock()
	el, exists := m.Dat[idx][key]
	m.mu[idx].RUnlock()
	if exists && time.Now().UTC().Before(el.expiresAt) {
		return el.dat, true
	} else if exists {
		// evict key since it exists and it's expired
		m.mu[idx].Lock()
		delete(m.Dat[idx], key)
		m.KeyCount--
		m.mu[idx].Unlock()
	}
	return []byte{}, false
}

func (m *MemoryCache) Write(key string, val []byte, ttl time.Duration) {
	idx := keyToShard(key)
	var e time.Time

	if ttl == 0 {
		// for a 0 TTL, store the max value of a time struct, so it essentially never expires
		e = time.Unix(1<<63-1, 0)
	} else {
		e = time.Now().UTC().Add(ttl)
	}

	c := cacheElement{
		expiresAt: e,
		dat:       val,
	}

	m.mu[idx].Lock()
	m.Dat[idx][key] = c
	m.KeyCount++
	m.mu[idx].Unlock()
}

func keyToShard(key string) int {
	i := int(strings.ToLower(key)[0])

	// if we're not in the char range (97-122), we'll stick it in the z bucket
	if i < 97 || i > 122 {
		i = 122
	}

	return i - 97
}
