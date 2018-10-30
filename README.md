# OmniCache

OmniCache is a flexible cache API with pluggable persistence layers (in-memory, Redis, etc). Any persistence layer that implements the `Cache` and `Conn` interfaces from [panoplymedia/cache](https://github.com/panoplymedia/cache) can be used with this package.

## Sample Usage

```go
type MyData struct {
  Value int
}

// called when Fetch doesnt find data in the cache
func (m MyData) CacheMiss(key string) ([]byte, error) {
  return []byte("hydrated"), nil
}

// where `conn` is a cache.Conn
c := localcache.New(conn)
err := c.Set([]byte("key"), []byte("value"))
if err != nil {
  fmt.Println(err)
}
b, err := c.Get([]byte("key"))
if err != nil {
  fmt.Println(err)
}

d := MyData{}
// returns data from the cache or calls `CacheMiss` for `MyData`
b, err = c.Fetch([]byte("miss"), d)
```

## Compatible Persistence Layers

- [MemoryStore](https://github.com/panoplymedia/omni-cache-memorystore)
- [BadgerDB](https://github.com/panoplymedia/omni-cache-badger)
- [Redis](https://github.com/panoplymedia/omni-cache-redis)
