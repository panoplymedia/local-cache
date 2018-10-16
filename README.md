# Local Cache

A local cache to be used with a caching layer that implements the `Cache` and `Conn` interfaces from [panoplymedia/cache](https://github.com/panoplymedia/cache). This enables a consistent local cache API with the ability to use different cache layers.

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
