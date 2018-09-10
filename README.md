# Local Cache

A local cache based on Badger.

### Sample Usage
```
package main

import (
	"fmt"
	"time"
	
	"github.com/panoplymedia/local-cache"
)

// implement the interface
type foo struct {}

// the CacheMiss() function must return a byte array. This is the value that gets cached
// In a typical use-case, this would be a call to a db or some other external call you want to cache
func (f foo) CacheMiss(key string) ([]byte, error){
	result := someLocalDB.query("select title from table where id=?", key)
	return []byte(result), nil
}

func main() {
	// construct a new cache (in this case, we're setting a 1-min TTL)
	c, err := cache.NewCache("foo", time.Minute)
	if err != nil {
		fmt.Println(err)
	}
	defer c.Close()

	// construct a new interface
	f := foo{}
	
	// read the bytes
	b, err := c.Fetch([]byte("key"), f)
	if err != nil {
		fmt.Println(err)
	}

	// display the result
	fmt.Println(string(b))
}

```