// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package memdb

import (
	"context"
	"sync"

	database "github.com/luxfi/database"
	"golang.org/x/exp/maps"
)

const (
	// DefaultSize is the default initial size of the database.
	DefaultSize = 1 << 10
)

// IteratorError is a wrapper for iterator errors
type IteratorError struct {
	Err error
}

func (i *IteratorError) Next() bool    { return false }
func (i *IteratorError) Error() error  { return i.Err }
func (i *IteratorError) Key() []byte   { return nil }
func (i *IteratorError) Value() []byte { return nil }
func (i *IteratorError) Release()      {}

// Database is an in-memory implementation of db.Database.
type Database struct {
	lock sync.RWMutex
	db   map[string][]byte
}

// New returns a new in-memory database.
func New() *Database {
	return NewWithSize(DefaultSize)
}

// NewWithSize returns a new in-memory database with the specified initial size.
func NewWithSize(size int) *Database {
	return &Database{
		db: make(map[string][]byte, size),
	}
}

// Close implements db.Database.
func (db *Database) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return database.ErrClosed
	}
	db.db = nil
	return nil
}

// Sync implements db.Database. For memdb, this is a no-op since all data is in memory.
func (db *Database) Sync() error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if db.db == nil {
		return database.ErrClosed
	}
	return nil
}

// HealthCheck implements db.Database.
func (db *Database) HealthCheck(ctx context.Context) (interface{}, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if db.db == nil {
		return nil, database.ErrClosed
	}
	return nil, nil
}

// Has implements db.Database.
func (db *Database) Has(key []byte) (bool, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if db.db == nil {
		return false, database.ErrClosed
	}
	_, ok := db.db[string(key)]
	return ok, nil
}

// Get implements db.Database.
func (db *Database) Get(key []byte) ([]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if db.db == nil {
		return nil, database.ErrClosed
	}
	value, ok := db.db[string(key)]
	if !ok {
		return nil, database.ErrNotFound
	}
	return value, nil
}

// Put implements db.Database.
func (db *Database) Put(key []byte, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return database.ErrClosed
	}
	// Clone the value to ensure memory safety
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	db.db[string(key)] = valueCopy
	return nil
}

// Delete implements db.Database.
func (db *Database) Delete(key []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return database.ErrClosed
	}
	delete(db.db, string(key))
	return nil
}

// NewBatch implements db.Database.
func (db *Database) NewBatch() database.Batch {
	return &batch{
		db:  db,
		ops: make([]op, 0),
	}
}

// NewIterator implements db.Database.
func (db *Database) NewIterator() database.Iterator {
	return db.NewIteratorWithStartAndPrefix(nil, nil)
}

// NewIteratorWithStart implements db.Database.
func (db *Database) NewIteratorWithStart(start []byte) database.Iterator {
	return db.NewIteratorWithStartAndPrefix(start, nil)
}

// NewIteratorWithPrefix implements db.Database.
func (db *Database) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return db.NewIteratorWithStartAndPrefix(nil, prefix)
}

// NewIteratorWithStartAndPrefix implements db.Database.
func (db *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if db.db == nil {
		return &IteratorError{Err: database.ErrClosed}
	}

	return newIterator(db, db.db, start, prefix)
}

// Compact implements db.Database.
func (db *Database) Compact(start []byte, limit []byte) error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if db.db == nil {
		return database.ErrClosed
	}
	// No-op for in-memory database
	return nil
}

// GetDatabase returns the underlying database map.
// This should only be used for testing.
func (db *Database) GetDatabase() map[string][]byte {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return maps.Clone(db.db)
}

// SetDatabase sets the underlying database map.
// This should only be used for testing.
func (db *Database) SetDatabase(m map[string][]byte) {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.db = m
}

// NewFromMap returns a new in-memory database initialized with the provided map.
func NewFromMap(m map[string][]byte) *Database {
	return &Database{
		db: m,
	}
}

// batch is a batch of operations to be written atomically.
type batch struct {
	db   *Database
	ops  []op
	size int
}

type op struct {
	key    string
	value  []byte
	delete bool
}

// Put implements db.Batch.
func (b *batch) Put(key, value []byte) error {
	// Clone the value to ensure memory safety
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	b.ops = append(b.ops, op{
		key:   string(key),
		value: valueCopy,
	})
	b.size += len(key) + len(value)
	return nil
}

// Delete implements db.Batch.
func (b *batch) Delete(key []byte) error {
	b.ops = append(b.ops, op{
		key:    string(key),
		delete: true,
	})
	b.size += len(key)
	return nil
}

// Size implements db.Batch.
func (b *batch) Size() int {
	return b.size
}

// Write implements db.Batch.
func (b *batch) Write() error {
	b.db.lock.Lock()
	defer b.db.lock.Unlock()

	if b.db.db == nil {
		return database.ErrClosed
	}

	for _, op := range b.ops {
		if op.delete {
			delete(b.db.db, op.key)
		} else {
			b.db.db[op.key] = op.value
		}
	}
	return nil
}

// Reset implements db.Batch.
func (b *batch) Reset() {
	b.ops = b.ops[:0]
	b.size = 0
}

// Replay implements db.Batch.
func (b *batch) Replay(w database.KeyValueWriterDeleter) error {
	for _, op := range b.ops {
		if op.delete {
			if err := w.Delete([]byte(op.key)); err != nil {
				return err
			}
		} else {
			if err := w.Put([]byte(op.key), op.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// Inner implements db.Batch.
func (b *batch) Inner() database.Batch {
	return b
}

// AtomicClear clears the database atomically.
func (db *Database) AtomicClear(newSize int) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return database.ErrClosed
	}
	db.db = make(map[string][]byte, newSize)
	return nil
}

// AtomicWrite writes all the key-value pairs atomically.
func (db *Database) AtomicWrite(ctx context.Context, ops map[string][]byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.db == nil {
		return database.ErrClosed
	}

	// Check context before making changes
	if err := ctx.Err(); err != nil {
		return err
	}

	for k, v := range ops {
		if v == nil {
			delete(db.db, k)
		} else {
			db.db[k] = v
		}
	}
	return nil
}
