package localcache

import (
	"time"

	"github.com/panoplymedia/cache"
)

// BackfillCache is an interface implementing CacheMiss that is called
// to hydrate the cache when fetching data via `Fetch` results in a miss
type BackfillCache interface {
	CacheMiss(key string) ([]byte, error)
}

// LocalCache contains connection to a cache layer
type LocalCache struct {
	Conn cache.Conn
}

// New creates a new LocalCache
func New(c cache.Conn) LocalCache {
	return LocalCache{Conn: c}
}

// Fetch gets data from the cache for the specified key
// If the data is missing, the result from BackfillCache.CacheMiss is returned and stored to the key
func (lc LocalCache) Fetch(k []byte, b BackfillCache) ([]byte, error) {
	ret, err := lc.Conn.Read(k)
	if err != nil {
		ret, err = b.CacheMiss(string(k))
		if err != nil {
			return ret, err
		}
		err = lc.Conn.Write(k, ret)
	}

	return ret, err
}

// FetchWithTTL is the same as Fetch, but with an explicit TTL
func (lc LocalCache) FetchWithTTL(k []byte, b BackfillCache, ttl time.Duration) ([]byte, error) {
	ret, err := lc.Conn.Read(k)
	if err != nil {
		ret, err = b.CacheMiss(string(k))
		if err != nil {
			return ret, err
		}
		err = lc.Conn.WriteTTL(k, ret, ttl)
	}

	return ret, err
}

// Set writes data to the cache
func (lc LocalCache) Set(k, v []byte) error {
	return lc.Conn.Write(k, v)
}

// SetWithTTL writes data to the cache with an explicit TTL
func (lc LocalCache) SetWithTTL(k, v []byte, ttl time.Duration) error {
	return lc.Conn.WriteTTL(k, v, ttl)
}

// Get retrieves data for a key from the cache
func (lc LocalCache) Get(k []byte) ([]byte, error) {
	return lc.Conn.Read(k)
}

// Stats provides stats about the cache connection
func (lc LocalCache) Stats() (map[string]interface{}, error) {
	return lc.Conn.Stats()
}
