// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !rocksdb
// +build !rocksdb

package leveldb

import (
	"bytes"
	"errors"

	"github.com/luxfi/database"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

const (
	// minBlockCacheSize is the minimum size of the block cache in bytes.
	minBlockCacheSize = 12 * opt.MiB

	// minWriteBufferSize is the minimum size of the write buffer in bytes.
	minWriteBufferSize = 4 * opt.MiB

	// minHandleCap is the minimum number of file handles.
	minHandleCap = 64
)

// Database is a persistent key-value store using LevelDB.
type Database struct {
	db *leveldb.DB
}

// New returns a new LevelDB database.
func New(path string, blockCacheSize int, writeCacheSize int, handleCap int) (*Database, error) {
	// Enforce minimums
	if blockCacheSize < minBlockCacheSize {
		blockCacheSize = minBlockCacheSize
	}
	if writeCacheSize < minWriteBufferSize {
		writeCacheSize = minWriteBufferSize
	}
	if handleCap < minHandleCap {
		handleCap = minHandleCap
	}

	opts := &opt.Options{
		BlockCacheCapacity:              blockCacheSize,
		WriteBuffer:                     writeCacheSize,
		OpenFilesCacheCapacity:          handleCap,
		CompactionTableSize:             4 * opt.MiB,
		CompactionTableSizeMultiplier:   2.0,
		CompactionL0Trigger:             8,
		DisableSeeksCompaction:          true,
	}

	ldb, err := leveldb.OpenFile(path, opts)
	if err != nil {
		return nil, err
	}

	return &Database{db: ldb}, nil
}

// Close implements database.Database.
func (d *Database) Close() error {
	return d.db.Close()
}

// HealthCheck implements database.Database.
func (d *Database) HealthCheck() error {
	_, err := d.db.GetProperty("leveldb.stats")
	return err
}

// Has implements database.Database.
func (d *Database) Has(key []byte) (bool, error) {
	_, err := d.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return false, nil
	}
	return err == nil, err
}

// Get implements database.Database.
func (d *Database) Get(key []byte) ([]byte, error) {
	value, err := d.db.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, database.ErrNotFound
	}
	return value, err
}

// Put implements database.Database.
func (d *Database) Put(key []byte, value []byte) error {
	return d.db.Put(key, value, nil)
}

// Delete implements database.Database.
func (d *Database) Delete(key []byte) error {
	return d.db.Delete(key, nil)
}

// NewBatch implements database.Database.
func (d *Database) NewBatch() database.Batch {
	return &batch{
		b: new(leveldb.Batch),
		d: d,
	}
}

// NewIterator implements database.Database.
func (d *Database) NewIterator() database.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, nil)
}

// NewIteratorWithStart implements database.Database.
func (d *Database) NewIteratorWithStart(start []byte) database.Iterator {
	return d.NewIteratorWithStartAndPrefix(start, nil)
}

// NewIteratorWithPrefix implements database.Database.
func (d *Database) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, prefix)
}

// NewIteratorWithStartAndPrefix implements database.Database.
func (d *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	var iter iterator.Iterator
	if len(prefix) == 0 {
		iter = d.db.NewIterator(nil, nil)
	} else {
		iter = d.db.NewIterator(util.BytesPrefix(prefix), nil)
	}

	if len(start) > 0 {
		iter.Seek(start)
	}

	return &dbIterator{
		Iterator: iter,
		start:    start,
	}
}

// Compact implements database.Database.
func (d *Database) Compact(start []byte, limit []byte) error {
	return d.db.CompactRange(util.Range{Start: start, Limit: limit})
}

// batch is a batch of operations to be written atomically.
type batch struct {
	b *leveldb.Batch
	d *Database
}

// Put implements database.Batch.
func (b *batch) Put(key, value []byte) error {
	b.b.Put(key, value)
	return nil
}

// Delete implements database.Batch.
func (b *batch) Delete(key []byte) error {
	b.b.Delete(key)
	return nil
}

// Size implements database.Batch.
func (b *batch) Size() int {
	return b.b.Len()
}

// Write implements database.Batch.
func (b *batch) Write() error {
	return b.d.db.Write(b.b, nil)
}

// Reset implements database.Batch.
func (b *batch) Reset() {
	b.b.Reset()
}

// Replay implements database.Batch.
func (b *batch) Replay(w database.KeyValueWriterDeleter) error {
	return b.b.Replay(&replayer{w: w})
}

// Inner implements database.Batch.
func (b *batch) Inner() database.Batch {
	return b
}

// replayer is a helper to replay a batch.
type replayer struct {
	w database.KeyValueWriterDeleter
}

func (r *replayer) Put(key, value []byte) {
	r.w.Put(key, value)
}

func (r *replayer) Delete(key []byte) {
	r.w.Delete(key)
}

// dbIterator is an iterator over a LevelDB database.
type dbIterator struct {
	iterator.Iterator
	start []byte
}

// Next implements database.Iterator.
func (it *dbIterator) Next() bool {
	// If we haven't started iterating yet and we have a start key,
	// check if we're already at a valid position
	if it.start != nil && !it.Valid() {
		return false
	}

	// If we have a start key and haven't moved yet, check current position
	if it.start != nil {
		if it.Valid() && bytes.Compare(it.Key(), it.start) >= 0 {
			// We're already at or past the start key
			it.start = nil // Clear start so we know we've started
			return true
		}
		it.start = nil // Clear start since we've now started iteration
	}

	return it.Iterator.Next()
}

// Error implements database.Iterator.
func (it *dbIterator) Error() error {
	return it.Iterator.Error()
}

// Key implements database.Iterator.
func (it *dbIterator) Key() []byte {
	return it.Iterator.Key()
}

// Value implements database.Iterator.
func (it *dbIterator) Value() []byte {
	return it.Iterator.Value()
}

// Release implements database.Iterator.
func (it *dbIterator) Release() {
	it.Iterator.Release()
}