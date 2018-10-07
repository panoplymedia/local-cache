package cache

import (
	"errors"
	"sync"
	"time"
)

// LocalCache is an interface implementing CacheMiss that is called
// to hydrate the cache when fetching data via `Fetch` results in a miss
type LocalCache interface {
	CacheMiss(key string) ([]byte, error)
}

// BadgerCache wraps an instance of a badger database with automatic garbage collection
// a TTL of 0 does not expire keys
type BadgerCache struct {
	db  *MemoryCache
	TTL time.Duration
	mu  sync.Mutex
}

// BadgerStats displays stats about badger
type BadgerStats struct {
	LSMSize  int64
	VLogSize int64
}

// NewCache creates a new MemoryCache
func NewCache(t time.Duration) (*BadgerCache, error) {
	if t < time.Second && t > 0 {
		return &BadgerCache{}, errors.New("TTL must be >= 1 second. Badger uses Unix timestamps for expiries which operate in second resolution")
	}
	var c BadgerCache
	c.db = newMemoryCache()
	c.TTL = t
	return &c, nil
}

// Close closes the badger database
func (c *BadgerCache) Close() error {
	return nil
}

// Fetch gets data from the cache for the specified key
// If the data is missing, the result from LocalCache.CacheMiss is returned and stored to the key
// uses the LocalCache TTL by default
func (c *BadgerCache) Fetch(k []byte, l LocalCache) ([]byte, error) {
	return c.FetchWithTTL(k, l, c.TTL)
}

// FetchWithTTL is the same as Fetch, but with an explicit TTL
// a TTL of 0 does not expire keys
func (c *BadgerCache) FetchWithTTL(k []byte, l LocalCache, ttl time.Duration) ([]byte, error) {
	var ret []byte

	val, exists := c.db.Read(string(k))
	if exists {
		return val, nil
	}

	ret, err := l.CacheMiss(string(k))
	if err != nil {
		return ret, err
	}
	c.db.Write(string(k), ret, ttl)

	return ret, nil
}

// Set writes data to the cache with the default cache TTL
func (c *BadgerCache) Set(k, v []byte) error {
	return c.SetWithTTL(k, v, c.TTL)
}

// SetWithTTL writes data to the cache with an explicit TTL
// a TTL of 0 does not expire keys
func (c *BadgerCache) SetWithTTL(k, v []byte, ttl time.Duration) error {
	c.db.Write(string(k), v, ttl)
	return nil
}

// SetBatch writes data to the cache with the default cache TTL
// this is more performant than calling `Set` in a loop if data to store is already known
func (c *BadgerCache) SetBatch(k, v [][]byte) error {
	return c.SetBatchWithTTL(k, v, c.TTL)
}

// SetBatchWithTTL writes data to the cache with an explicit TTL
// a TTL of 0 does not expire keys
func (c *BadgerCache) SetBatchWithTTL(k, v [][]byte, ttl time.Duration) error {
	for i := range k {
		c.db.Write(string(k[i]), v[i], ttl)
	}

	return nil
}

// Incr increments the key by the specified uint64 value and returns the current value
// due to transaction conflicts this is eventually consistent
func (c *BadgerCache) Incr(k []byte, v uint64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var newVal uint64

	val, exists := c.db.Read(string(k))
	if exists {
		newVal = bytesToUint64(val) + v
	} else {
		newVal = v
	}

	c.db.Write(string(k), uint64ToBytes(newVal), c.TTL)
	return newVal, nil
}

// Get retrieves data for a key from the cache
func (c *BadgerCache) Get(k []byte) ([]byte, error) {
	val, exists := c.db.Read(string(k))
	if exists {
		return val, nil
	}

	return []byte{}, errors.New("key not found")
}

func (c *BadgerCache) setWithTTL(k, v []byte, ttl time.Duration) error {
	c.db.Write(string(k), v, ttl)
	return nil
}

// Stats provides stats about the Badger database
func (c *BadgerCache) Stats() BadgerStats {
	return BadgerStats{
		LSMSize:  int64(c.db.KeyCount),
		VLogSize: int64(c.db.KeyCount),
	}
}
