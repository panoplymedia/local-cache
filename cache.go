package omnicache

import (
	"time"

	"github.com/panoplymedia/cache"
)

// BackfillCache is an interface implementing CacheMiss that is called
// to hydrate the cache when fetching data via `Fetch` results in a miss
type BackfillCache interface {
	CacheMiss(key string) ([]byte, error)
}

// OmniCache contains connection to a cache layer
type OmniCache struct {
	Conn cache.Conn
}

// New creates a new OmniCache
func New(c cache.Conn) *OmniCache {
	return &OmniCache{Conn: c}
}

// Close closes connection to local cache backend
func (oc *OmniCache) Close() error {
	return oc.Conn.Close()
}

// Fetch gets data from the cache for the specified key
// If the data is missing, the result from BackfillCache.CacheMiss is returned and stored to the key
func (oc *OmniCache) Fetch(k []byte, b BackfillCache) ([]byte, error) {
	ret, err := oc.Conn.Read(k)
	if err != nil {
		ret, err = b.CacheMiss(string(k))
		if err != nil {
			return ret, err
		}
		err = oc.Conn.Write(k, ret)
	}

	return ret, err
}

// FetchWithTTL is the same as Fetch, but with an explicit TTL
func (oc *OmniCache) FetchWithTTL(k []byte, b BackfillCache, ttl time.Duration) ([]byte, error) {
	ret, err := oc.Conn.Read(k)
	if err != nil {
		ret, err = b.CacheMiss(string(k))
		if err != nil {
			return ret, err
		}
		err = oc.Conn.WriteTTL(k, ret, ttl)
	}

	return ret, err
}

// Set writes data to the cache
func (oc *OmniCache) Set(k, v []byte) error {
	return oc.Conn.Write(k, v)
}

// SetWithTTL writes data to the cache with an explicit TTL
func (oc *OmniCache) SetWithTTL(k, v []byte, ttl time.Duration) error {
	return oc.Conn.WriteTTL(k, v, ttl)
}

// Get retrieves data for a key from the cache
func (oc *OmniCache) Get(k []byte) ([]byte, error) {
	return oc.Conn.Read(k)
}

// Stats provides stats about the cache connection
func (oc *OmniCache) Stats() (map[string]interface{}, error) {
	return oc.Conn.Stats()
}
