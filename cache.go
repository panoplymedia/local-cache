package cache

import (
	"errors"
	"io"
	"time"

	"github.com/dgraph-io/badger"
)

// LocalCache is an interface implementing CacheMiss that is called
// to hydrate the cache when fetching data via `Fetch` results in a miss
type LocalCache interface {
	CacheMiss(key string) ([]byte, error)
}

// BadgerCache wraps an instance of a badger database with automatic garbage collection
// a TTL of 0 does not expire keys
type BadgerCache struct {
	name   string
	db     *badger.DB
	TTL    time.Duration
	ticker *time.Ticker // for GC loop
}

// GarbageCollectionOptions specifies settings for Badger garbage collection
type GarbageCollectionOptions struct {
	Frequency    time.Duration
	DiscardRatio float64
}

// DefaultGCOptions are the default GarbageCollectionOptions
var DefaultGCOptions = GarbageCollectionOptions{
	Frequency:    time.Minute,
	DiscardRatio: 0.5,
}

// NewCache creates a new BadgerCache
func NewCache(n string, t time.Duration, opts *badger.Options, gcOpts *GarbageCollectionOptions) (*BadgerCache, error) {
	if t < time.Second && t > 0 {
		return &BadgerCache{}, errors.New("TTL must be >= 1 second. Badger uses Unix timestamps for expiries which operate in second resolution")
	}
	if opts == nil {
		opts = &badger.DefaultOptions
	}
	opts.Dir = n
	opts.ValueDir = n
	db, err := badger.Open(*opts)
	if err != nil {
		return &BadgerCache{}, err
	}

	if gcOpts == nil {
		gcOpts = &DefaultGCOptions
	}
	// start a GC loop
	ticker := time.NewTicker(gcOpts.Frequency)
	go func(t *time.Ticker, d *badger.DB) {
		for range t.C {
		again:
			err := d.RunValueLogGC(gcOpts.DiscardRatio)
			if err == nil {
				goto again
			}
		}
	}(ticker, db)

	c := BadgerCache{n, db, t, ticker}
	return &c, nil
}

// Close closes the badger database
func (c *BadgerCache) Close() error {
	c.ticker.Stop()
	return c.db.Close()
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
	cacheMiss := false
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		// key either does not exist or was expired
		if err == badger.ErrKeyNotFound {
			cacheMiss = true
			return nil
		} else if err != nil {
			return err
		}
		ret, err = item.Value()
		return err
	})
	if cacheMiss {
		// pull the new value
		ret, err = l.CacheMiss(string(k))
		if err != nil {
			return ret, err
		}

		c.db.Update(func(txn *badger.Txn) error {
			err := setWithTTL(txn, k, ret, ttl)
			return err
		})
	}
	return ret, err
}

// Set writes data to the cache with the default cache TTL
func (c *BadgerCache) Set(k, v []byte) error {
	return c.SetWithTTL(k, v, c.TTL)
}

// SetWithTTL writes data to the cache with an explicit TTL
// a TTL of 0 does not expire keys
func (c *BadgerCache) SetWithTTL(k, v []byte, ttl time.Duration) error {
	return c.db.Update(func(txn *badger.Txn) error {
		return setWithTTL(txn, k, v, ttl)
	})
}

// SetBatch writes data to the cache with the default cache TTL
// this is more performant than calling `Set` in a loop if data to store is already known
func (c *BadgerCache) SetBatch(k, v [][]byte) error {
	return c.SetBatchWithTTL(k, v, c.TTL)
}

// SetBatchWithTTL writes data to the cache with an explicit TTL
// a TTL of 0 does not expire keys
func (c *BadgerCache) SetBatchWithTTL(k, v [][]byte, ttl time.Duration) error {
	return c.db.Update(func(txn *badger.Txn) error {
		for i := range k {
			return setWithTTL(txn, k[i], v[i], ttl)
		}
		return nil
	})
}

// Incr increments the key by the specified uint64 value and returns the current value
// due to transaction conflicts this is eventually consistent
func (c *BadgerCache) Incr(k []byte, v uint64) (uint64, error) {
	b, err := c.Get(k)
	if err == badger.ErrKeyNotFound {
		err = c.SetWithTTL(k, uint64ToBytes(v), 0)
		if err != nil {
			return 0, err
		}
		return v, nil
	} else if err != nil {
		return bytesToUint64(b), err
	}

	m := c.db.GetMergeOperator(k, add, 50*time.Millisecond)
	defer m.Stop()
	err = m.Add(uint64ToBytes(v))
	if err != nil {
		return 0, err
	}
	b, err = m.Get()
	if err != nil {
		return 0, err
	}
	return bytesToUint64(b), nil
}

// Get retrieves data for a key from the cache
func (c *BadgerCache) Get(k []byte) ([]byte, error) {
	ret := []byte{}
	err := c.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		ret, err = item.Value()
		return err
	})
	return ret, err
}

func setWithTTL(txn *badger.Txn, k, v []byte, ttl time.Duration) error {
	// set the new value with TTL
	if ttl < time.Second && ttl > 0 {
		return errors.New("TTL must be >= 1 second. Badger uses Unix timestamps for expiries which operate in second resolution")
	} else if ttl > 0 {
		err := txn.SetWithTTL(k, v, ttl)
		if err != nil {
			return err
		}
	} else {
		err := txn.Set(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

// Backup dumps a protobuf-encoded list of all entries in the database into the
// given writer, that are newer than the specified version. It returns a
// timestamp indicating when the entries were dumped which can be passed into a
// later invocation to generate an incremental dump, of entries that have been
// added/modified since the last invocation of DB.Backup()
//
// This can be used to backup the data in a database at a given point in time.
func (c *BadgerCache) Backup(w io.Writer, since uint64) (upto uint64, err error) {
	return c.db.Backup(w, since)
}

// Load reads a protobuf-encoded list of all entries from a reader and writes
// them to the database. This can be used to restore the database from a backup
// made by calling DB.Backup().
//
// DB.Load() should be called on a database that is not running any other
// concurrent transactions while it is running.
func (c *BadgerCache) Load(r io.Reader) error {
	return c.db.Load(r)
}
