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
	var ret []byte
	err := c.db.Update(func(txn *badger.Txn) error {
		item, err2 := txn.Get(k)
		// key either does not exist or was expired
		if err2 == badger.ErrKeyNotFound {
			// pull the new value
			dat, err3 := l.CacheMiss(string(k))
			if err3 != nil {
				return err3
			}

			// set the new value with TTL
			err4 := txn.SetWithTTL(k, dat, c.TTL)
			if err4 != nil {
				return err4
			}
			ret = dat
			return nil
		} else if err2 != nil {
			return err2
		}
		val, err5 := item.Value()
		if err5 != nil {
			return err5
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
	return c.db.Update(func(txn *badger.Txn) error {
		err := txn.SetWithTTL(k, v, c.TTL)
		if err != nil {
			return err
		}
		return nil
	})
}

func (c *BadgerCache) SetBatch(k, v [][]byte) error {
	return c.db.Update(func(txn *badger.Txn) error {
		for i, _ := range k {
			err := txn.SetWithTTL(k[i], v[i], c.TTL)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
