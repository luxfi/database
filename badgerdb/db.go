// Copyright (C) 2019-2025, Lux Partners Limited. All rights reserved.
// See the file LICENSE for licensing terms.

package badgerdb

import (
	"bytes"
	"errors"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/luxfi/db"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	_ db.Database = (*Database)(nil)

	// Errors
	errClosed = errors.New("database closed")
)

// Database is a badgerdb backed database
type Database struct {
	dbPath  string
	db      *badger.DB
	closed  bool
	closeMu sync.RWMutex
}

// New returns a new badgerdb-backed database
func New(file string, configBytes []byte, namespace string, metrics prometheus.Registerer) (*Database, error) {
	// Configure BadgerDB options
	opts := badger.DefaultOptions(file)
	opts.Logger = nil // TODO: wrap our logger

	// Open the database
	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &Database{
		dbPath: file,
		db:     badgerDB,
	}, nil
}

// Close implements the Database interface
func (d *Database) Close() error {
	d.closeMu.Lock()
	defer d.closeMu.Unlock()

	if d.closed {
		return db.ErrClosed
	}
	d.closed = true
	return d.db.Close()
}

// HealthCheck returns nil if the database is healthy, non-nil otherwise.
func (d *Database) HealthCheck() error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return db.ErrClosed
	}
	// BadgerDB doesn't have a direct health check, but we can try a simple operation
	return d.db.View(func(txn *badger.Txn) error {
		return nil
	})
}

// Has implements the Database interface
func (d *Database) Has(key []byte) (bool, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return false, db.ErrClosed
	}

	var exists bool
	err := d.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			exists = true
			return nil
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			exists = false
			return nil
		}
		return err
	})
	return exists, err
}

// Get implements the Database interface
func (d *Database) Get(key []byte) ([]byte, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return nil, db.ErrClosed
	}

	var value []byte
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return db.ErrNotFound
			}
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// Put implements the Database interface
func (d *Database) Put(key []byte, value []byte) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return db.ErrClosed
	}

	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete implements the Database interface
func (d *Database) Delete(key []byte) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return db.ErrClosed
	}

	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// NewBatch implements the Database interface
func (d *Database) NewBatch() db.Batch {
	return &batch{
		db:      d,
		ops:     make([]batchOp, 0),
		size:    0,
		closeMu: &d.closeMu,
	}
}

// NewIterator implements the Database interface
func (d *Database) NewIterator() db.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, nil)
}

// NewIteratorWithStart implements the Database interface
func (d *Database) NewIteratorWithStart(start []byte) db.Iterator {
	return d.NewIteratorWithStartAndPrefix(start, nil)
}

// NewIteratorWithPrefix implements the Database interface
func (d *Database) NewIteratorWithPrefix(prefix []byte) db.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, prefix)
}

// NewIteratorWithStartAndPrefix implements the Database interface
func (d *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) db.Iterator {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return &iterator{
			db:       d,
			err:      db.ErrClosed,
			closedMu: &d.closeMu,
		}
	}

	txn := d.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.Prefix = prefix

	it := txn.NewIterator(opts)
	if start != nil {
		it.Seek(start)
	} else if prefix != nil {
		it.Seek(prefix)
	} else {
		it.Rewind()
	}

	return &iterator{
		db:       d,
		txn:      txn,
		iter:     it,
		prefix:   prefix,
		closedMu: &d.closeMu,
	}
}

// Compact implements the Database interface
func (d *Database) Compact(start, limit []byte) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return db.ErrClosed
	}

	// BadgerDB handles compaction automatically via its value log GC
	// We can trigger a manual GC run
	for {
		err := d.db.RunValueLogGC(0.5)
		if err == badger.ErrNoRewrite {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// batchOp is a batch operation
type batchOp struct {
	delete bool
	key    []byte
	value  []byte
}

// batch is a badgerdb batch
type batch struct {
	db      *Database
	ops     []batchOp
	size    int
	closeMu *sync.RWMutex
}

// Put implements the Batch interface
func (b *batch) Put(key, value []byte) error {
	b.ops = append(b.ops, batchOp{
		delete: false,
		key:    append([]byte{}, key...),
		value:  append([]byte{}, value...),
	})
	b.size += len(key) + len(value)
	return nil
}

// Delete implements the Batch interface
func (b *batch) Delete(key []byte) error {
	b.ops = append(b.ops, batchOp{
		delete: true,
		key:    append([]byte{}, key...),
	})
	b.size += len(key)
	return nil
}

// Size implements the Batch interface
func (b *batch) Size() int {
	return b.size
}

// Write implements the Batch interface
func (b *batch) Write() error {
	b.closeMu.RLock()
	defer b.closeMu.RUnlock()

	if b.db.closed {
		return db.ErrClosed
	}

	return b.db.db.Update(func(txn *badger.Txn) error {
		for _, op := range b.ops {
			if op.delete {
				if err := txn.Delete(op.key); err != nil {
					return err
				}
			} else {
				if err := txn.Set(op.key, op.value); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// Reset implements the Batch interface
func (b *batch) Reset() {
	b.ops = b.ops[:0]
	b.size = 0
}

// Replay implements the Batch interface
func (b *batch) Replay(w db.KeyValueWriterDeleter) error {
	for _, op := range b.ops {
		if op.delete {
			if err := w.Delete(op.key); err != nil {
				return err
			}
		} else {
			if err := w.Put(op.key, op.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// Inner returns the inner batch, if applicable
func (b *batch) Inner() db.Batch {
	return nil
}

// iterator is a badgerdb iterator
type iterator struct {
	db       *Database
	txn      *badger.Txn
	iter     *badger.Iterator
	prefix   []byte
	err      error
	closedMu *sync.RWMutex
}

// Next implements the Iterator interface
func (it *iterator) Next() bool {
	if it.iter == nil || it.err != nil {
		return false
	}

	it.iter.Next()
	if !it.iter.Valid() {
		return false
	}

	// Check if we're still within the prefix
	if it.prefix != nil {
		key := it.iter.Item().Key()
		if !bytes.HasPrefix(key, it.prefix) {
			return false
		}
	}

	return true
}

// Error implements the Iterator interface
func (it *iterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return nil
}

// Key implements the Iterator interface
func (it *iterator) Key() []byte {
	if it.iter == nil || !it.iter.Valid() {
		return nil
	}
	return it.iter.Item().KeyCopy(nil)
}

// Value implements the Iterator interface
func (it *iterator) Value() []byte {
	if it.iter == nil || !it.iter.Valid() {
		return nil
	}
	val, err := it.iter.Item().ValueCopy(nil)
	if err != nil {
		it.err = err
		return nil
	}
	return val
}

// Release implements the Iterator interface
func (it *iterator) Release() {
	if it.iter != nil {
		it.iter.Close()
		it.iter = nil
	}
	if it.txn != nil {
		it.txn.Discard()
		it.txn = nil
	}
}