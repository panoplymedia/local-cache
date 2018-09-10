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
func (f foo) CacheMiss() ([]byte, error){
	return []byte("abc"), nil
}

func main() {
	// construct a new cache
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