// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package prefixdb

import (
	"sync"

	"github.com/luxfi/database"
)

// Database partitions a database into a sub-database by prefixing all keys with
// a unique value.
type Database struct {
	db     database.Database
	prefix []byte

	bufferPool sync.Pool
}

// New returns a new prefixed database.
func New(prefix []byte, db database.Database) *Database {
	return &Database{
		db:     db,
		prefix: prefix,
		bufferPool: sync.Pool{
			New: func() interface{} {
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
func (p *Database) Close() error {
	return p.db.Close()
}

// HealthCheck implements the database.Database interface.
func (p *Database) HealthCheck() error {
	return p.db.HealthCheck()
}

// Has implements the database.Database interface.
func (p *Database) Has(key []byte) (bool, error) {
	return p.db.Has(p.prefixKey(key))
}

// Get implements the database.Database interface.
func (p *Database) Get(key []byte) ([]byte, error) {
	return p.db.Get(p.prefixKey(key))
}

// Put implements the database.Database interface.
func (p *Database) Put(key, value []byte) error {
	return p.db.Put(p.prefixKey(key), value)
}

// Delete implements the database.Database interface.
func (p *Database) Delete(key []byte) error {
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
func (p *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	prefixedStart := append(p.prefix, start...)
	prefixedPrefix := append(p.prefix, prefix...)
	return &iterator{
		Iterator: p.db.NewIteratorWithStartAndPrefix(prefixedStart, prefixedPrefix),
		prefix:   p,
	}
}

// Compact implements the database.Database interface.
func (p *Database) Compact(start, limit []byte) error {
	if start != nil {
		start = p.prefixKey(start)
	}
	if limit != nil {
		limit = p.prefixKey(limit)
	}
	return p.db.Compact(start, limit)
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

// Inner implements the database.Batch interface.
func (b *batch) Inner() database.Batch {
	return b.Batch
}

// iterator wraps a database.Iterator to remove the prefix from all keys.
type iterator struct {
	database.Iterator
	prefix *Database
}

// Key implements the database.Iterator interface.
func (it *iterator) Key() []byte {
	key := it.Iterator.Key()
	if key == nil {
		return nil
	}
	return it.prefix.removePrefix(key)
}

// Error implements the database.Iterator interface.
func (it *iterator) Error() error {
	return it.Iterator.Error()
}
