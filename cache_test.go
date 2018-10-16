package localcache

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/panoplymedia/local-cache-memorystore"
	"github.com/stretchr/testify/assert"
)

type doubler struct {
	Value int
}

func (d doubler) CacheMiss(key string) ([]byte, error) {
	d.Value *= 2
	return d.encode()
}

func (d doubler) encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(d)
	return buf.Bytes(), err
}

func decodeDoubler(b []byte) (doubler, error) {
	d := doubler{}
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&d)
	return d, err
}

func createConn() *memorystorecache.Conn {
	memCache, _ := memorystorecache.NewCache(time.Second)
	c, _ := memCache.Open("")
	return c
}

func TestNew(t *testing.T) {
	c := createConn()
	lc := New(c)
	defer lc.Close()
	assert.Equal(t, &LocalCache{Conn: c}, lc)
}

func TestSet(t *testing.T) {
	c := createConn()
	lc := New(c)
	defer lc.Close()

	key := []byte("set")

	// cache miss
	b := []byte{1, 2, 3}
	err := lc.Set(key, b)
	assert.Nil(t, err)

	// cache hit
	b2, err := lc.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, b, b2)

	// default ttl timeout (cache miss)
	time.Sleep(time.Second)
	_, err = lc.Get(key)
	assert.Errorf(t, err, "Key not found")
}

func TestSetWithTTL(t *testing.T) {
	c := createConn()
	lc := New(c)
	defer lc.Close()

	key := []byte("set")

	// cache miss
	b := []byte{1, 2, 3}
	err := lc.SetWithTTL(key, b, time.Second)
	assert.Nil(t, err)

	// cache hit
	b2, err := lc.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, b, b2)

	// default ttl timeout (cache miss)
	time.Sleep(time.Second)
	_, err = lc.Get(key)
	assert.Errorf(t, err, "Key not found")
}

func TestGet(t *testing.T) {
	c := createConn()
	lc := New(c)
	defer lc.Close()

	// creates initial key
	key := []byte("my-key")
	// cache miss
	b, err := lc.Get(key)
	assert.Errorf(t, err, "Key not found")

	// cache hit
	v := []byte{1, 2}
	err = lc.Set(key, v)
	assert.Nil(t, err)
	b, err = lc.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, v, b)
}

func TestFetch(t *testing.T) {
	c := createConn()
	lc := New(c)
	defer lc.Close()

	key := []byte("fetch")

	// cache miss
	d := doubler{Value: 2}
	b, err := lc.Fetch(key, d)
	assert.Nil(t, err)
	newD, err := decodeDoubler(b)
	assert.Equal(t, 4, newD.Value)

	// cache hit
	b, err = lc.Fetch(key, newD)
	assert.Nil(t, err)
	newD, err = decodeDoubler(b)
	assert.Equal(t, 4, newD.Value)

	// default timeout (cache miss again)
	time.Sleep(time.Second)
	b, err = lc.Fetch(key, newD)
	assert.Nil(t, err)
	newD, err = decodeDoubler(b)
	assert.Equal(t, 8, newD.Value)
}

func TestFetchWithTTL(t *testing.T) {
	c := createConn()
	lc := New(c)
	defer lc.Close()

	key := []byte("fetch")

	// cache miss
	d := doubler{Value: 2}
	b, err := lc.FetchWithTTL(key, d, time.Second)
	assert.Nil(t, err)
	newD, err := decodeDoubler(b)
	assert.Equal(t, 4, newD.Value)

	// cache hit
	b, err = lc.FetchWithTTL(key, newD, time.Second)
	assert.Nil(t, err)
	newD, err = decodeDoubler(b)
	assert.Equal(t, 4, newD.Value)

	// ttl timeout (cache miss again)
	time.Sleep(time.Second)
	b, err = lc.FetchWithTTL(key, newD, time.Second)
	assert.Nil(t, err)
	newD, err = decodeDoubler(b)
	assert.Equal(t, 8, newD.Value)
}

func TestStats(t *testing.T) {
	c := createConn()
	lc := New(c)
	defer lc.Close()

	// write a key
	key := []byte("my-key")
	v := []byte{1, 2}
	err := lc.Set(key, v)
	assert.Nil(t, err)

	s, err := lc.Stats()
	assert.Nil(t, err)
	assert.Equal(t, map[string]interface{}{"KeyCount": uint64(1)}, s)
}
