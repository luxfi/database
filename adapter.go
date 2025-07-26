// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package database

import (
	"context"
	"io"

	"github.com/luxfi/database"
)

// Ensure luxfi/database types implement our interfaces
var (
	_ Database = (*dbAdapter)(nil)
	_ Batch    = (*batchAdapter)(nil)
	_ Iterator = (*iteratorAdapter)(nil)
)

// dbAdapter wraps a luxfi/database.Database to implement the node Database interface
type dbAdapter struct {
	db db.Database
}

// NewAdapter creates a new database adapter
func NewAdapter(database db.Database) Database {
	return &dbAdapter{db: database}
}

// Has implements KeyValueReader
func (d *dbAdapter) Has(key []byte) (bool, error) {
	return d.db.Has(key)
}

// Get implements KeyValueReader
func (d *dbAdapter) Get(key []byte) ([]byte, error) {
	return d.db.Get(key)
}

// Put implements KeyValueWriter
func (d *dbAdapter) Put(key []byte, value []byte) error {
	return d.db.Put(key, value)
}

// Delete implements KeyValueDeleter
func (d *dbAdapter) Delete(key []byte) error {
	return d.db.Delete(key)
}

// NewBatch implements Batcher
func (d *dbAdapter) NewBatch() Batch {
	return &batchAdapter{batch: d.db.NewBatch()}
}

// NewIterator implements Iteratee
func (d *dbAdapter) NewIterator() Iterator {
	return &iteratorAdapter{iter: d.db.NewIterator()}
}

// NewIteratorWithStart implements Iteratee
func (d *dbAdapter) NewIteratorWithStart(start []byte) Iterator {
	return &iteratorAdapter{iter: d.db.NewIteratorWithStart(start)}
}

// NewIteratorWithPrefix implements Iteratee
func (d *dbAdapter) NewIteratorWithPrefix(prefix []byte) Iterator {
	return &iteratorAdapter{iter: d.db.NewIteratorWithPrefix(prefix)}
}

// NewIteratorWithStartAndPrefix implements Iteratee
func (d *dbAdapter) NewIteratorWithStartAndPrefix(start, prefix []byte) Iterator {
	return &iteratorAdapter{iter: d.db.NewIteratorWithStartAndPrefix(start, prefix)}
}

// Compact implements Compacter
func (d *dbAdapter) Compact(start []byte, limit []byte) error {
	return d.db.Compact(start, limit)
}

// Close implements io.Closer
func (d *dbAdapter) Close() error {
	return d.db.Close()
}

// HealthCheck implements health.Checker
func (d *dbAdapter) HealthCheck(ctx context.Context) (interface{}, error) {
	// Call the luxfi/database HealthCheck (which doesn't take context)
	err := d.db.HealthCheck()
	if err != nil {
		return nil, err
	}
	return map[string]string{"status": "healthy"}, nil
}

// batchAdapter wraps a luxfi/database.Batch
type batchAdapter struct {
	batch db.Batch
}

// Put implements Batch
func (b *batchAdapter) Put(key, value []byte) error {
	return b.batch.Put(key, value)
}

// Delete implements Batch
func (b *batchAdapter) Delete(key []byte) error {
	return b.batch.Delete(key)
}

// Size implements Batch
func (b *batchAdapter) Size() int {
	return b.batch.Size()
}

// Write implements Batch
func (b *batchAdapter) Write() error {
	return b.batch.Write()
}

// Reset implements Batch
func (b *batchAdapter) Reset() {
	b.batch.Reset()
}

// Replay implements Batch
func (b *batchAdapter) Replay(w KeyValueWriterDeleter) error {
	// Create an adapter for the writer
	adapter := &writerDeleterAdapter{w: w}
	return b.batch.Replay(adapter)
}

// Inner implements Batch
func (b *batchAdapter) Inner() Batch {
	return b
}

// iteratorAdapter wraps a luxfi/database.Iterator
type iteratorAdapter struct {
	iter db.Iterator
}

// Next implements Iterator
func (i *iteratorAdapter) Next() bool {
	return i.iter.Next()
}

// Error implements Iterator
func (i *iteratorAdapter) Error() error {
	return i.iter.Error()
}

// Key implements Iterator
func (i *iteratorAdapter) Key() []byte {
	return i.iter.Key()
}

// Value implements Iterator
func (i *iteratorAdapter) Value() []byte {
	return i.iter.Value()
}

// Release implements Iterator
func (i *iteratorAdapter) Release() {
	i.iter.Release()
}

// writerDeleterAdapter adapts our KeyValueWriterDeleter to db.KeyValueWriterDeleter
type writerDeleterAdapter struct {
	w KeyValueWriterDeleter
}

func (w *writerDeleterAdapter) Put(key, value []byte) error {
	return w.w.Put(key, value)
}

func (w *writerDeleterAdapter) Delete(key []byte) error {
	return w.w.Delete(key)
}

// Factory functions to create databases from luxfi/database implementations

// NewMemDB creates a new in-memory database
func NewMemDB() Database {
	return NewAdapter(db.New())
}

// NewLevelDB creates a new LevelDB database
func NewLevelDB(path string) (Database, error) {
	leveldb, err := db.NewLevelDB(path)
	if err != nil {
		return nil, err
	}
	return NewAdapter(leveldb), nil
}