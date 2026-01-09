// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build leveldb

package leveldb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"

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
	db     *leveldb.DB
	closed atomic.Bool
	mu     sync.RWMutex
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
		BlockCacheCapacity:            blockCacheSize,
		WriteBuffer:                   writeCacheSize,
		OpenFilesCacheCapacity:        handleCap,
		CompactionTableSize:           4 * opt.MiB,
		CompactionTableSizeMultiplier: 2.0,
		CompactionL0Trigger:           8,
		DisableSeeksCompaction:        true,
	}

	ldb, err := leveldb.OpenFile(path, opts)
	if err != nil {
		return nil, err
	}

	return &Database{db: ldb}, nil
}

// Close implements database.Database.
func (d *Database) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed.Load() {
		return database.ErrClosed
	}

	d.closed.Store(true)
	return updateError(d.db.Close())
}

// HealthCheck implements database.Database.
func (d *Database) HealthCheck(ctx context.Context) (interface{}, error) {
	if d.closed.Load() {
		return nil, database.ErrClosed
	}
	_, err := d.db.GetProperty("leveldb.stats")
	return nil, updateError(err)
}

func (d *Database) Backup(io.Writer, uint64) (uint64, error) {
	return 0, errors.New("backup not supported for leveldb")
}

func (d *Database) Load(io.Reader) error {
	return errors.New("load not supported for leveldb")
}

// Has implements database.Database.
func (d *Database) Has(key []byte) (bool, error) {
	if d.closed.Load() {
		return false, database.ErrClosed
	}
	_, err := d.db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return false, nil
	}
	return err == nil, updateError(err)
}

// Get implements database.Database.
func (d *Database) Get(key []byte) ([]byte, error) {
	if d.closed.Load() {
		return nil, database.ErrClosed
	}
	value, err := d.db.Get(key, nil)
	return value, updateError(err)
}

// Put implements database.Database.
func (d *Database) Put(key []byte, value []byte) error {
	if d.closed.Load() {
		return database.ErrClosed
	}
	return updateError(d.db.Put(key, value, nil))
}

// Delete implements database.Database.
func (d *Database) Delete(key []byte) error {
	if d.closed.Load() {
		return database.ErrClosed
	}
	return updateError(d.db.Delete(key, nil))
}

// NewBatch implements database.Database.
func (d *Database) NewBatch() database.Batch {
	if d.closed.Load() {
		// Return a batch that will error on write
		return &batch{
			b: new(leveldb.Batch),
			d: d,
		}
	}
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
	if d.closed.Load() {
		return &dbIterator{
			Iterator: nil,
			start:    start,
			db:       d,
			err:      database.ErrClosed,
		}
	}

	var iter iterator.Iterator
	if len(prefix) == 0 {
		iter = d.db.NewIterator(nil, nil)
	} else {
		iter = d.db.NewIterator(util.BytesPrefix(prefix), nil)
	}

	if len(start) > 0 {
		iter.Seek(start)
	}

	dbIter := &dbIterator{
		Iterator: iter,
		start:    start,
		db:       d,
	}

	// If we're already at a valid position after seeking, store the current key/value
	if iter.Valid() {
		dbIter.key = slices.Clone(iter.Key())
		dbIter.value = slices.Clone(iter.Value())
	}

	return dbIter
}

// Compact implements database.Database.
func (d *Database) Compact(start []byte, limit []byte) error {
	if d.closed.Load() {
		return database.ErrClosed
	}
	return updateError(d.db.CompactRange(util.Range{Start: start, Limit: limit}))
}

// Sync implements the database.Syncer interface
// It flushes all buffered writes to persistent storage.
func (d *Database) Sync() error {
	if d.closed.Load() {
		return database.ErrClosed
	}
	// LevelDB doesn't have a direct Sync method.
	// We use an empty batch write with Sync option to force a WAL sync.
	return updateError(d.db.Write(new(leveldb.Batch), &opt.WriteOptions{Sync: true}))
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
	if b.d.closed.Load() {
		return database.ErrClosed
	}
	return updateError(b.d.db.Write(b.b, nil))
}

// Reset implements database.Batch.
func (b *batch) Reset() {
	b.b.Reset()
}

// Replay implements database.Batch.
func (b *batch) Replay(w database.KeyValueWriterDeleter) error {
	replay := &replayer{w: w}
	if err := b.b.Replay(replay); err != nil {
		// leveldb's Replay doesn't return errors
		return err
	}
	return replay.err
}

// Inner implements database.Batch.
func (b *batch) Inner() database.Batch {
	return b
}

// replayer is a helper to replay a batch.
type replayer struct {
	w   database.KeyValueWriterDeleter
	err error
}

func (r *replayer) Put(key, value []byte) {
	if r.err != nil {
		return
	}
	r.err = r.w.Put(key, value)
}

func (r *replayer) Delete(key []byte) {
	if r.err != nil {
		return
	}
	r.err = r.w.Delete(key)
}

// dbIterator is an iterator over a LevelDB database.
type dbIterator struct {
	iterator.Iterator
	start []byte
	db    *Database
	err   error
	key   []byte
	value []byte
}

// Next implements database.Iterator.
func (it *dbIterator) Next() bool {
	// Check if iterator was created with an error or database is closed
	if it.err != nil || it.Iterator == nil {
		it.key = nil
		it.value = nil
		return false
	}

	// Check if database is closed
	if it.db != nil && it.db.closed.Load() {
		it.key = nil
		it.value = nil
		it.err = database.ErrClosed
		return false
	}

	// If we haven't started iterating yet and we have a start key,
	// check if we're already at a valid position
	if it.start != nil && !it.Valid() {
		it.key = nil
		it.value = nil
		return false
	}

	// If we have a start key and haven't moved yet, check current position
	if it.start != nil {
		if it.Valid() && bytes.Compare(it.Iterator.Key(), it.start) >= 0 {
			// We're already at or past the start key
			it.start = nil // Clear start so we know we've started
			// Store current key/value
			it.key = slices.Clone(it.Iterator.Key())
			it.value = slices.Clone(it.Iterator.Value())
			return true
		}
		it.start = nil // Clear start since we've now started iteration
	}

	hasNext := it.Iterator.Next()
	if hasNext {
		it.key = slices.Clone(it.Iterator.Key())
		it.value = slices.Clone(it.Iterator.Value())
	} else {
		it.key = nil
		it.value = nil
	}
	return hasNext
}

// Error implements database.Iterator.
func (it *dbIterator) Error() error {
	if it.err != nil {
		return it.err
	}
	if it.Iterator == nil {
		return nil
	}
	return it.Iterator.Error()
}

// Key implements database.Iterator.
func (it *dbIterator) Key() []byte {
	return it.key
}

// Value implements database.Iterator.
func (it *dbIterator) Value() []byte {
	return it.value
}

// Release implements database.Iterator.
func (it *dbIterator) Release() {
	if it.Iterator != nil {
		it.Iterator.Release()
	}
}
