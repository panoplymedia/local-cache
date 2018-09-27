package cache

import (
	"time"

	"github.com/dgraph-io/badger"
)

type LocalCache interface {
	CacheMiss(key string) ([]byte, error)
}

type BadgerCache struct {
	name   string
	db     *badger.DB
	TTL    time.Duration
	ticker *time.Ticker // for GC loop
}

func NewCache(n string, t time.Duration) (*BadgerCache, error) {
	opts := badger.DefaultOptions
	opts.Dir = n
	opts.ValueDir = n
	db, err := badger.Open(opts)
	if err != nil {
		return &BadgerCache{}, err
	}

	// start a GC loop
	ticker := time.NewTicker(30 * time.Minute)
	go func(t *time.Ticker, d *badger.DB) {
		for range t.C {
		again:
			err := d.RunValueLogGC(0.7)
			if err == nil {
				goto again
			}
		}
	}(ticker, db)

	c := BadgerCache{n, db, t, ticker}
	return &c, nil
}

func (c *BadgerCache) Close() error {
	c.ticker.Stop()
	return c.db.Close()
}

func (c *BadgerCache) Fetch(k []byte, l LocalCache) ([]byte, error) {
	return c.FetchWithTTL(k, l, c.TTL)
}

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

		err = c.db.Update(func(txn *badger.Txn) error {
			return setWithTTL(txn, k, ret, ttl)
		})
	}
	return ret, err
}

func (c *BadgerCache) Set(k, v []byte) error {
	return c.SetWithTTL(k, v, c.TTL)
}

func (c *BadgerCache) SetWithTTL(k, v []byte, ttl time.Duration) error {
	return c.db.Update(func(txn *badger.Txn) error {
		return setWithTTL(txn, k, v, ttl)
	})
}

func (c *BadgerCache) SetBatch(k, v [][]byte) error {
	return c.SetBatchWithTTL(k, v, c.TTL)
}

func (c *BadgerCache) SetBatchWithTTL(k, v [][]byte, ttl time.Duration) error {
	return c.db.Update(func(txn *badger.Txn) error {
		for i := range k {
			return setWithTTL(txn, k[i], v[i], ttl)
		}
		return nil
	})
}

func (c *BadgerCache) Incr(k []byte, v uint64) (uint64, error) {
	return c.IncrWithTTL(k, v, c.TTL)
}

func (c *BadgerCache) IncrWithTTL(k []byte, v uint64, ttl time.Duration) (uint64, error) {
	var ret uint64
	err := c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err == badger.ErrKeyNotFound {
			ret = v
		} else if err != nil {
			return err
		} else {
			val, err := item.Value()
			if err != nil {
				return err
			}
			ret = bytesToUint64(val) + v
		}

		return setWithTTL(txn, k, uint64ToBytes(ret), ttl)
	})

	return ret, err
}

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
	if ttl > 0 {
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
