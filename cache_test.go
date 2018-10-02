package cache

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger"
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

func TestMain(m *testing.M) {
	code := m.Run()
	files, _ := filepath.Glob("test-cache-*")
	for _, f := range files {
		os.RemoveAll(f)
	}
	os.Exit(code)
}

func TestNewCache(t *testing.T) {
	opts := badger.DefaultOptions
	c, err := NewCache("test-cache-new", time.Second, &opts, &DefaultGCOptions)
	assert.Nil(t, err)
	defer c.Close()

	assert.Equal(t, "test-cache-new", c.name)
	assert.Equal(t, time.Second, c.TTL)
}

func TestFetch(t *testing.T) {
	c, err := NewCache("test-cache-fetch", time.Second, nil, nil)
	assert.Nil(t, err)
	defer c.Close()

	key := []byte("fetch")

	// cache miss
	d := doubler{Value: 2}
	b, err := c.Fetch(key, d)
	assert.Nil(t, err)
	newD, err := decodeDoubler(b)
	assert.Equal(t, 4, newD.Value)

	// cache hit
	b, err = c.Fetch(key, newD)
	assert.Nil(t, err)
	newD, err = decodeDoubler(b)
	assert.Equal(t, 4, newD.Value)

	// default timeout (cache miss again)
	time.Sleep(time.Second)
	b, err = c.Fetch(key, newD)
	assert.Nil(t, err)
	newD, err = decodeDoubler(b)
	assert.Equal(t, 8, newD.Value)
}

func TestFetchWithTTL(t *testing.T) {
	c, err := NewCache("test-cache-fetch-ttl", 2*time.Second, nil, nil)
	assert.Nil(t, err)
	defer c.Close()

	key := []byte("fetch")

	// cache miss
	d := doubler{Value: 2}
	b, err := c.FetchWithTTL(key, d, time.Second)
	assert.Nil(t, err)
	newD, err := decodeDoubler(b)
	assert.Equal(t, 4, newD.Value)

	// cache hit
	b, err = c.FetchWithTTL(key, newD, time.Second)
	assert.Nil(t, err)
	newD, err = decodeDoubler(b)
	assert.Equal(t, 4, newD.Value)

	// ttl timeout (cache miss again)
	time.Sleep(time.Second)
	b, err = c.FetchWithTTL(key, newD, time.Second)
	assert.Nil(t, err)
	newD, err = decodeDoubler(b)
	assert.Equal(t, 8, newD.Value)
}

func TestSet(t *testing.T) {
	c, err := NewCache("test-cache-set", time.Second, nil, nil)
	assert.Nil(t, err)
	defer c.Close()

	key := []byte("set")

	// cache miss
	b := []byte{1, 2, 3}
	err = c.Set(key, b)
	assert.Nil(t, err)

	// cache hit
	b2, err := c.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, b, b2)

	// default ttl timeout (cache miss)
	time.Sleep(time.Second)
	_, err = c.Get(key)
	assert.Errorf(t, err, "Key not found")
}

func TestSetWithTTL(t *testing.T) {
	c, err := NewCache("test-cache-set-ttl", 2*time.Second, nil, nil)
	assert.Nil(t, err)
	defer c.Close()

	key := []byte("set")

	// cache miss
	b := []byte{1, 2, 3}
	err = c.SetWithTTL(key, b, time.Second)
	assert.Nil(t, err)

	// cache hit
	b2, err := c.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, b, b2)

	// default ttl timeout (cache miss)
	time.Sleep(time.Second)
	_, err = c.Get(key)
	assert.Errorf(t, err, "Key not found")
}

func TestIncr(t *testing.T) {
	c, err := NewCache("test-cache-incr", time.Second, nil, nil)
	assert.Nil(t, err)
	defer c.Close()

	// creates initial key
	key := []byte("my-key")
	c.Incr(key, 2)
	b, err := c.Get(key)
	assert.Nil(t, err, "Key should exist")
	assert.Equal(t, uint64(2), bytesToUint64(b), fmt.Sprintf("Expected %v and got %v", 2, bytesToUint64(b)))

	// increments
	c.Incr(key, 2)
	c.Incr(key, 4)
	c.Incr(key, 3)
	b, err = c.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, uint64(11), bytesToUint64(b), fmt.Sprintf("Expected %v and got %v", 11, bytesToUint64(b)))

	// parallel increments
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Incr(key, 5)
		}()
	}
	wg.Wait()

	b, err = c.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, uint64(511), bytesToUint64(b), fmt.Sprintf("Expected %v and got %v", 511, bytesToUint64(b)))
}

func TestGet(t *testing.T) {
	c, err := NewCache("test-cache-get", time.Second, nil, nil)
	assert.Nil(t, err)
	defer c.Close()

	// creates initial key
	key := []byte("my-key")
	// cache miss
	b, err := c.Get(key)
	assert.Errorf(t, err, "Key not fou")

	// cache hit
	v := []byte{1, 2}
	err = c.Set(key, v)
	assert.Nil(t, err)
	b, err = c.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, v, b)
}

func TestBackup(t *testing.T) {
	c, err := NewCache("test-cache-backup", time.Second, nil, nil)
	assert.Nil(t, err)
	defer c.Close()

	c.Set([]byte{1, 2, 3}, []byte{4, 5, 6})
	var b bytes.Buffer
	w := io.Writer(&b)
	upto, err := c.Backup(w, uint64(time.Now().Add(-1*time.Minute).Unix()))
	assert.Nil(t, err)
	assert.True(t, upto > 0)
}

func TestLoad(t *testing.T) {
	c, err := NewCache("test-cache-load", time.Second, nil, nil)
	assert.Nil(t, err)
	defer c.Close()

	c.Set([]byte{1, 2, 3}, []byte{4, 5, 6})
	var b bytes.Buffer
	w := io.Writer(&b)
	upto, err := c.Backup(w, uint64(time.Now().Add(-1*time.Minute).Unix()))
	assert.Nil(t, err)
	assert.True(t, upto > 0)

	var readB bytes.Buffer
	r := io.Reader(&readB)
	err = c.Load(r)
	assert.Nil(t, err)
	assert.Equal(t, b.Bytes(), readB.Bytes())
}

func TestStats(t *testing.T) {
	opts := badger.DefaultOptions
	c, err := NewCache("test-cache-stats", time.Second, &opts, nil)
	assert.Nil(t, err)
	defer c.Close()

	s := c.Stats()
	assert.Equal(t, BadgerStats{0, 0}, s)
}
