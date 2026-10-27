// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package prefixdb

import (
	"context"
	"io"
	"sync"

	"github.com/luxfi/database"
)

// Database partitions a database into a sub-database by prefixing all keys with
// a unique value.
type Database struct {
	lock   sync.RWMutex
	db     database.Database
	prefix []byte
	closed bool

	bufferPool sync.Pool
}

// New returns a new prefixed database.
func New(prefix []byte, db database.Database) *Database {
	return &Database{
		db:     db,
		prefix: prefix,
		bufferPool: sync.Pool{
			New: func() any {
				return make([]byte, 0, len(prefix)+256)
			},
		},
	}
}

// Deprecated: Use [New] instead.
func NewNested(prefix []byte, db database.Database) *Database {
	return New(prefix, db)
}

// Close implements the database.Database interface.
// Closing a prefixed view releases only the view; the parent store's owner is
// responsible for closing the underlying database.
func (p *Database) Close() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return database.ErrClosed
	}
	p.closed = true
	return nil
}

func (p *Database) isClosed() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.closed
}

// HealthCheck implements the database.Database interface.
func (p *Database) HealthCheck(ctx context.Context) (any, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if p.closed {
		return nil, database.ErrClosed
	}
	return p.db.HealthCheck(ctx)
}

func (p *Database) Backup(w io.Writer, since uint64) (uint64, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if p.closed {
		return 0, database.ErrClosed
	}
	return p.db.Backup(w, since)
}

func (p *Database) Load(r io.Reader) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return database.ErrClosed
	}
	return p.db.Load(r)
}

// Has implements the database.Database interface.
func (p *Database) Has(key []byte) (bool, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if p.closed {
		return false, database.ErrClosed
	}
	return p.db.Has(p.prefixKey(key))
}

// Get implements the database.Database interface.
func (p *Database) Get(key []byte) ([]byte, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if p.closed {
		return nil, database.ErrClosed
	}
	return p.db.Get(p.prefixKey(key))
}

// Put implements the database.Database interface.
func (p *Database) Put(key, value []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return database.ErrClosed
	}
	return p.db.Put(p.prefixKey(key), value)
}

// Delete implements the database.Database interface.
func (p *Database) Delete(key []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return database.ErrClosed
	}
	return p.db.Delete(p.prefixKey(key))
}

// NewBatch implements the database.Database interface.
func (p *Database) NewBatch() database.Batch {
	return &batch{
		Batch:  p.db.NewBatch(),
		prefix: p,
	}
}

// NewIterator implements the database.Database interface.
func (p *Database) NewIterator() database.Iterator {
	return p.NewIteratorWithStartAndPrefix(nil, nil)
}

// NewIteratorWithStart implements the database.Database interface.
func (p *Database) NewIteratorWithStart(start []byte) database.Iterator {
	return p.NewIteratorWithStartAndPrefix(start, nil)
}

// NewIteratorWithPrefix implements the database.Database interface.
func (p *Database) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return p.NewIteratorWithStartAndPrefix(nil, prefix)
}

// NewIteratorWithStartAndPrefix implements the database.Database interface.
//
// prefixedStart MUST be derived only when the caller supplied a start: a nil
// start means "begin at the prefix", and the underlying store seeks to the
// prefix itself. Synthesizing start = p.prefix for a nil start would seek the
// underlying iterator to the FRONT of this partition (p.prefix), which sorts
// BEFORE p.prefix+prefix; with intervening keys under other sub-prefixes a
// store that stops at the first non-matching key after a start-seek (zapdb)
// then returns zero rows for a prefix that does exist. Each prefixed key is
// also built in its OWN buffer (prefixKey) so the two never alias p.prefix's
// backing array.
func (p *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	p.lock.RLock()
	defer p.lock.RUnlock()

	if p.closed {
		return &database.IteratorError{
			Err: database.ErrClosed,
		}
	}

	var prefixedStart []byte
	if len(start) > 0 {
		prefixedStart = p.prefixKey(start)
	}
	prefixedPrefix := p.prefixKey(prefix)
	return &iterator{
		Iterator: p.db.NewIteratorWithStartAndPrefix(prefixedStart, prefixedPrefix),
		prefix:   p,
	}
}

// Compact implements the database.Database interface.
func (p *Database) Compact(start, limit []byte) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return database.ErrClosed
	}
	if start != nil {
		start = p.prefixKey(start)
	}
	if limit != nil {
		limit = p.prefixKey(limit)
	}
	return p.db.Compact(start, limit)
}

// Sync implements the database.Syncer interface by delegating to the underlying database.
func (p *Database) Sync() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.closed {
		return database.ErrClosed
	}
	return p.db.Sync()
}

// prefixKey returns a key with the prefix appended.
func (p *Database) prefixKey(key []byte) []byte {
	buf := p.bufferPool.Get().([]byte)[:0]
	buf = append(buf, p.prefix...)
	buf = append(buf, key...)

	// Create a new slice to avoid keeping reference to the buffer
	result := make([]byte, len(buf))
	copy(result, buf)

	p.bufferPool.Put(buf)
	return result
}

// removePrefix returns a key with the prefix removed.
// The returned key shares memory with the input key.
func (p *Database) removePrefix(key []byte) []byte {
	if len(key) < len(p.prefix) {
		return nil
	}
	return key[len(p.prefix):]
}

// batch wraps a database.Batch to add a prefix to all keys.
type batch struct {
	database.Batch
	prefix *Database
}

// Put implements the database.Batch interface.
func (b *batch) Put(key, value []byte) error {
	return b.Batch.Put(b.prefix.prefixKey(key), value)
}

// Delete implements the database.Batch interface.
func (b *batch) Delete(key []byte) error {
	return b.Batch.Delete(b.prefix.prefixKey(key))
}

// Write implements the database.Batch interface.
func (b *batch) Write() error {
	b.prefix.lock.RLock()
	defer b.prefix.lock.RUnlock()

	if b.prefix.closed {
		return database.ErrClosed
	}
	return b.Batch.Write()
}

// Inner implements the database.Batch interface.
func (b *batch) Inner() database.Batch {
	return b.Batch
}

// iterator wraps a database.Iterator to remove the prefix from all keys.
type iterator struct {
	database.Iterator
	prefix *Database

	key, val []byte
	err      error
}

// Next moves the iterator to the next key/value pair.
func (it *iterator) Next() bool {
	if it.prefix.isClosed() {
		it.key = nil
		it.val = nil
		it.err = database.ErrClosed
		return false
	}

	next := it.Iterator.Next()
	if next {
		if rawKey := it.Iterator.Key(); rawKey != nil {
			it.key = it.prefix.removePrefix(rawKey)
		} else {
			it.key = nil
		}
		it.val = it.Iterator.Value()
	} else {
		it.key = nil
		it.val = nil
	}
	return next
}

// Key implements the database.Iterator interface.
func (it *iterator) Key() []byte {
	return it.key
}

// Value implements the database.Iterator interface.
func (it *iterator) Value() []byte {
	return it.val
}

// Error implements the database.Iterator interface.
func (it *iterator) Error() error {
	if it.err != nil {
		return it.err
	}
	return it.Iterator.Error()
}

// Release implements the database.Iterator interface.
func (it *iterator) Release() {
	it.key = nil
	it.val = nil
	it.Iterator.Release()
}
