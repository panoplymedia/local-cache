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
	err := c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		// key either does not exist or was expired
		if err == badger.ErrKeyNotFound {
			// pull the new value
			dat, err := l.CacheMiss(string(k))
			if err != nil {
				return err
			}

			// set the new value with TTL
			if ttl > 0 {
				err = txn.SetWithTTL(k, dat, ttl)
				if err != nil {
					return err
				}
			} else {
				err = txn.Set(k, dat)
				if err != nil {
					return err
				}
			}
			ret = dat
			return nil
		} else if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}
		ret = val
		return nil
	})

	if err != nil {
		return ret, err
	}

	return ret, nil
}

func (c *BadgerCache) Set(k, v []byte) error {
	return c.SetWithTTL(k, v, c.TTL)
}

func (c *BadgerCache) SetWithTTL(k, v []byte, ttl time.Duration) error {
	return c.db.Update(func(txn *badger.Txn) error {
		err := txn.SetWithTTL(k, v, ttl)
		if err != nil {
			return err
		}
		return nil
	})
}

func (c *BadgerCache) SetBatch(k, v [][]byte) error {
	return c.SetBatchWithTTL(k, v, c.TTL)
}

func (c *BadgerCache) SetBatchWithTTL(k, v [][]byte, ttl time.Duration) error {
	return c.db.Update(func(txn *badger.Txn) error {
		for i := range k {
			if ttl > 0 {
				err := txn.SetWithTTL(k[i], v[i], ttl)
				if err != nil {
					return err
				}
			} else {
				err := txn.Set(k[i], v[i])
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (c *BadgerCache) Incr(k []byte, v uint64) (uint64, error) {
	m := c.db.GetMergeOperator(k, add, 200*time.Millisecond)
	defer m.Stop()
	err := m.Add(uint64ToBytes(v))
	if err != nil {
		return 0, err
	}
	b, err := m.Get()
	if err != nil {
		return 0, err
	}
	return bytesToUint64(b), nil
}

func (c *BadgerCache) Get(k []byte) ([]byte, error) {
	ret := []byte{}
	err := c.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(k)
		if err != nil {
			return err
		}
		ret, err = item.Value()
		return err
	})
	return ret, err
}
