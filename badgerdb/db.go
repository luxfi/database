// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package badgerdb

import (
	"context"
	"bytes"
	"errors"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/luxfi/database"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	_ database.Database = (*Database)(nil)

	// emptyKeyPlaceholder is used internally to store empty keys since BadgerDB doesn't support them
	emptyKeyPlaceholder = []byte{0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF}
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
	// BadgerDB requires a valid directory path
	if file == "" {
		return nil, errors.New("badgerdb: database path required")
	}
	
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
	if d == nil {
		return nil
	}
	
	d.closeMu.Lock()
	defer d.closeMu.Unlock()

	if d.closed {
		return database.ErrClosed
	}
	d.closed = true
	
	if d.db == nil {
		return nil
	}
	return d.db.Close()
}

// HealthCheck returns nil if the database is healthy, non-nil otherwise.
func (d *Database) HealthCheck(ctx context.Context) (interface{}, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return nil, database.ErrClosed
	}
	// BadgerDB doesn't have a direct health check, but we can try a simple operation
	return nil, d.db.View(func(txn *badger.Txn) error {
		return nil
	})
}

// Has implements the Database interface
func (d *Database) Has(key []byte) (bool, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return false, database.ErrClosed
	}

	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
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
		return nil, database.ErrClosed
	}

	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	var value []byte
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return database.ErrNotFound
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
		return database.ErrClosed
	}

	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
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
		return database.ErrClosed
	}

	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// NewBatch implements the Database interface
func (d *Database) NewBatch() database.Batch {
	return &batch{
		db:    d,
		ops:   make([]batchOp, 0, 16),
		size:  0,
		reset: true,
	}
}

// NewIterator implements the Database interface
func (d *Database) NewIterator() database.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, nil)
}

// NewIteratorWithStart implements the Database interface
func (d *Database) NewIteratorWithStart(start []byte) database.Iterator {
	return d.NewIteratorWithStartAndPrefix(start, nil)
}

// NewIteratorWithPrefix implements the Database interface
func (d *Database) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, prefix)
}

// NewIteratorWithStartAndPrefix implements the Database interface
func (d *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return &nopIterator{err: database.ErrClosed}
	}

	txn := d.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10

	it := txn.NewIterator(opts)
	iter := &iterator{
		txn:    txn,
		iter:   it,
		prefix: prefix,
		start:  start,
		closed: false,
		db:     d,
	}

	// Initialize iterator position
	if len(start) > 0 {
		it.Seek(start)
	} else if len(prefix) > 0 {
		it.Seek(prefix)
	} else {
		it.Rewind()
	}

	// If using prefix, ensure we're at a valid position
	if len(prefix) > 0 && it.Valid() && !bytes.HasPrefix(it.Item().Key(), prefix) {
		// Move to next valid item or mark as exhausted
		iter.Next()
	}

	return iter
}

// Compact implements the Database interface
func (d *Database) Compact(start []byte, limit []byte) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	// BadgerDB handles compaction automatically in the background
	// We can trigger a manual compaction if needed
	// The start and limit parameters are ignored as BadgerDB doesn't support range compaction
	err := d.db.RunValueLogGC(0.5)
	// BadgerDB returns an error if GC didn't result in any cleanup, but that's not really an error
	if err != nil && err.Error() == "Value log GC attempt didn't result in any cleanup" {
		return nil
	}
	return err
}

// GetSnapshot implements the database.Database interface
func (d *Database) GetSnapshot() (database.Database, error) {
	// BadgerDB doesn't support snapshots in the same way
	// For now, return the same database instance
	// TODO: Implement proper snapshot support if needed
	return d, nil
}

// Empty returns true if the database doesn't contain any keys (but not deleted keys)
func (d *Database) Empty() (bool, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return false, database.ErrClosed
	}

	empty := true
	err := d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if it.Valid() {
			empty = false
		}
		return nil
	})
	return empty, err
}

// Len returns the number of keys in the database
func (d *Database) Len() (int, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return 0, database.ErrClosed
	}

	count := 0
	err := d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// batch represents a batch of database operations
type batch struct {
	db    *Database
	ops   []batchOp
	size  int
	reset bool
}

type batchOp struct {
	key    []byte
	value  []byte
	delete bool
}

// Put implements the Batch interface
func (b *batch) Put(key, value []byte) error {
	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	b.ops = append(b.ops, batchOp{
		key:   append([]byte{}, key...),
		value: append([]byte{}, value...),
	})
	b.size += len(key) + len(value)
	return nil
}

// Delete implements the Batch interface
func (b *batch) Delete(key []byte) error {
	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	b.ops = append(b.ops, batchOp{
		key:    append([]byte{}, key...),
		delete: true,
	})
	b.size += len(key)
	return nil
}

// Write implements the Batch interface
func (b *batch) Write() error {
	if b.db.closed {
		return database.ErrClosed
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

// WriteSync implements the Batch interface
func (b *batch) WriteSync() error {
	// BadgerDB syncs by default
	return b.Write()
}

// Reset implements the Batch interface
func (b *batch) Reset() {
	if b.reset {
		b.ops = b.ops[:0]
		b.size = 0
	}
}

// Replay implements the Batch interface
func (b *batch) Replay(w database.KeyValueWriterDeleter) error {
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

// SetResetDisabled implements the Batch interface
func (b *batch) SetResetDisabled(disabled bool) {
	b.reset = !disabled
}

// GetResetDisabled implements the Batch interface
func (b *batch) GetResetDisabled() bool {
	return !b.reset
}

// Inner implements the Batch interface
func (b *batch) Inner() database.Batch {
	return b
}

// Size implements the Batch interface
func (b *batch) Size() int {
	return b.size
}

// iterator wraps a BadgerDB iterator
type iterator struct {
	txn      *badger.Txn
	iter     *badger.Iterator
	prefix   []byte
	start    []byte
	closed   bool
	err      error
	mu       sync.RWMutex
	started  bool // Track if we've started iteration
	key      []byte
	value    []byte
	db       *Database
}

// Next implements the Iterator interface
func (i *iterator) Next() bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Check if iterator is closed
	if i.closed {
		i.err = database.ErrClosed
		i.key = nil
		i.value = nil
		return false
	}

	// Check if database is closed
	if i.db != nil {
		i.db.closeMu.RLock()
		dbClosed := i.db.closed
		i.db.closeMu.RUnlock()
		
		if dbClosed {
			i.err = database.ErrClosed
			i.key = nil
			i.value = nil
			return false
		}
	}

	// If this is the first call to Next() and we're already at a valid position
	if !i.started {
		i.started = true
		if i.iter.Valid() {
			// Check prefix constraint
			if len(i.prefix) > 0 && !bytes.HasPrefix(i.iter.Item().Key(), i.prefix) {
				i.key = nil
				i.value = nil
				return false
			}
			// Store current key/value
			i.key = i.getKey()
			i.value = i.getValue()
			return true
		}
	}

	i.iter.Next()
	
	// Check if we're still valid and within prefix bounds
	if !i.iter.Valid() {
		i.key = nil
		i.value = nil
		return false
	}
	
	if len(i.prefix) > 0 && !bytes.HasPrefix(i.iter.Item().Key(), i.prefix) {
		i.key = nil
		i.value = nil
		return false
	}
	
	// Store current key/value
	i.key = i.getKey()
	i.value = i.getValue()
	return true
}

// Error implements the Iterator interface
func (i *iterator) Error() error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.err
}

// Key implements the Iterator interface
func (i *iterator) Key() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.key
}

// getKey returns a copy of the current iterator key
func (i *iterator) getKey() []byte {
	if i.closed || i.iter == nil || !i.iter.Valid() {
		return nil
	}
	
	key := i.iter.Item().Key()
	// Check if this is our empty key placeholder
	if bytes.Equal(key, emptyKeyPlaceholder) {
		return []byte{}
	}
	return append([]byte{}, key...)
}

// Value implements the Iterator interface
func (i *iterator) Value() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.value
}

// getValue returns a copy of the current iterator value
func (i *iterator) getValue() []byte {
	if i.closed || i.iter == nil || !i.iter.Valid() {
		return nil
	}

	value, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		return nil
	}
	return value
}

// Release implements the Iterator interface
func (i *iterator) Release() {
	i.mu.Lock()
	defer i.mu.Unlock()

	if !i.closed {
		i.closed = true
		i.key = nil
		i.value = nil
		if i.iter != nil {
			i.iter.Close()
		}
		i.txn.Discard()
	}
}

// nopIterator is a no-op iterator that returns an error
type nopIterator struct {
	err error
}

func (n *nopIterator) Next() bool       { return false }
func (n *nopIterator) Error() error     { return n.err }
func (n *nopIterator) Key() []byte      { return nil }
func (n *nopIterator) Value() []byte    { return nil }
func (n *nopIterator) Release()         {}