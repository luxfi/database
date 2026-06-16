// Copyright (C) 2020-2026, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"bytes"
	"time"

	"github.com/luxfi/database"
	zdb "github.com/luxfi/zapdb"
)

// txnWrap adapts a zdb.Txn to database.Txn.
type txnWrap struct {
	tx       *zdb.Txn
	readOnly bool
}

// keyFor maps a user-visible key to the on-disk key, applying the empty-key
// placeholder so badger (which rejects empty keys) sees a non-empty value.
func keyFor(k []byte) []byte {
	if len(k) == 0 {
		return emptyKeyPlaceholder
	}
	return k
}

// userKey reverses keyFor for iteration output.
func userKey(k []byte) []byte {
	if bytes.Equal(k, emptyKeyPlaceholder) {
		return []byte{}
	}
	return k
}

func (t *txnWrap) Has(key []byte) (bool, error) {
	_, err := t.tx.Get(keyFor(key))
	if err == zdb.ErrKeyNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (t *txnWrap) Get(key []byte) ([]byte, error) {
	item, err := t.tx.Get(keyFor(key))
	if err == zdb.ErrKeyNotFound {
		return nil, database.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (t *txnWrap) Put(key, value []byte) error {
	if t.readOnly {
		return database.ErrReadOnlyTxn
	}
	return t.tx.Set(keyFor(key), value)
}

func (t *txnWrap) PutWithTTL(key, value []byte, ttl time.Duration) error {
	if t.readOnly {
		return database.ErrReadOnlyTxn
	}
	e := zdb.NewEntry(keyFor(key), value)
	if ttl > 0 {
		e = e.WithTTL(ttl)
	}
	return t.tx.SetEntry(e)
}

func (t *txnWrap) Delete(key []byte) error {
	if t.readOnly {
		return database.ErrReadOnlyTxn
	}
	return t.tx.Delete(keyFor(key))
}

func (t *txnWrap) NewIterator() database.Iterator {
	return t.newIterator(nil, nil)
}

func (t *txnWrap) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return t.newIterator(nil, prefix)
}

func (t *txnWrap) NewIteratorWithStart(start []byte) database.Iterator {
	return t.newIterator(start, nil)
}

func (t *txnWrap) newIterator(start, prefix []byte) database.Iterator {
	opts := zdb.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := t.tx.NewIterator(opts)
	iter := &iterator{
		txn:    t.tx, // shared txn; iterator must NOT close it
		iter:   it,
		prefix: prefix,
		start:  start,
		closed: false,
		shared: true,
	}
	if len(start) > 0 {
		it.Seek(start)
	} else if len(prefix) > 0 {
		it.Seek(prefix)
	} else {
		it.Rewind()
	}
	if len(prefix) > 0 && it.Valid() && !bytes.HasPrefix(it.Item().Key(), prefix) {
		iter.Next()
	}
	return iter
}

// Update runs fn in a writable badger transaction. On non-nil error from fn
// the transaction is rolled back; on nil the transaction is committed.
// badger handles conflict retries internally via View/Update — we surface
// any commit error to the caller.
func (d *Database) Update(fn func(txn database.Txn) error) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	tx := d.db.NewTransaction(true)
	defer tx.Discard()

	wrap := &txnWrap{tx: tx, readOnly: false}
	if err := fn(wrap); err != nil {
		return err
	}
	return tx.Commit()
}

// View runs fn in a read-only badger transaction.
func (d *Database) View(fn func(txn database.Txn) error) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	tx := d.db.NewTransaction(false)
	defer tx.Discard()

	wrap := &txnWrap{tx: tx, readOnly: true}
	return fn(wrap)
}

// PutWithTTL implements database.TTLWriter outside of an explicit transaction.
// It opens a write txn, sets the entry with the TTL, and commits.
func (d *Database) PutWithTTL(key, value []byte, ttl time.Duration) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	return d.db.Update(func(tx *zdb.Txn) error {
		e := zdb.NewEntry(keyFor(key), value)
		if ttl > 0 {
			e = e.WithTTL(ttl)
		}
		return tx.SetEntry(e)
	})
}

// Compile-time interface checks.
var (
	_ database.Transactional = (*Database)(nil)
	_ database.TTLWriter     = (*Database)(nil)
	_ database.Txn           = (*txnWrap)(nil)
)
