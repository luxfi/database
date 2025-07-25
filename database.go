// Copyright (C) 2019-2025, Lux Partners Limited. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import (
	"io"
	"sync"
)

// Database is a key-value store that supports batch writes and iteration.
type Database interface {
	KeyValueReader
	KeyValueWriterDeleter
	Batcher
	Iteratee
	Compacter
	io.Closer

	// HealthCheck returns nil if the database is healthy, non-nil otherwise.
	HealthCheck() error
}

// KeyValueReader is the interface for reading key-value pairs.
type KeyValueReader interface {
	// Has returns if the database has a key with the provided value.
	Has(key []byte) (bool, error)

	// Get returns the value associated with the key.
	// If the key doesn't exist, ErrNotFound is returned.
	Get(key []byte) ([]byte, error)
}

// KeyValueWriter is the interface for writing key-value pairs.
type KeyValueWriter interface {
	// Put stores the key value pair in the database.
	// If the key already exists, it will be overwritten.
	Put(key []byte, value []byte) error
}

// KeyValueDeleter is the interface for deleting key-value pairs.
type KeyValueDeleter interface {
	// Delete removes the key from the database.
	Delete(key []byte) error
}

// KeyValueWriterDeleter is the interface for writing and deleting key-value pairs.
type KeyValueWriterDeleter interface {
	KeyValueWriter
	KeyValueDeleter
}

// KeyValueReaderWriterDeleter is the interface for reading, writing, and deleting key-value pairs.
type KeyValueReaderWriterDeleter interface {
	KeyValueReader
	KeyValueWriter
	KeyValueDeleter
}

// Batcher is the interface for batching database operations.
type Batcher interface {
	// NewBatch creates a new batch that can be used to perform multiple
	// operations atomically.
	NewBatch() Batch
}

// Iteratee is the interface for iterating over key-value pairs.
type Iteratee interface {
	// NewIterator returns an iterator that will iterate over the key-value
	// pairs in the database in lexicographical order by key.
	NewIterator() Iterator

	// NewIteratorWithStart returns an iterator that will iterate over the
	// key-value pairs in the database in lexicographical order by key,
	// starting at the provided key (inclusive).
	NewIteratorWithStart(start []byte) Iterator

	// NewIteratorWithPrefix returns an iterator that will iterate over the
	// key-value pairs in the database in lexicographical order by key,
	// where each key has the provided prefix.
	NewIteratorWithPrefix(prefix []byte) Iterator

	// NewIteratorWithStartAndPrefix returns an iterator that will iterate over
	// the key-value pairs in the database in lexicographical order by key,
	// starting at the provided key (inclusive) and where each key has the
	// provided prefix.
	NewIteratorWithStartAndPrefix(start, prefix []byte) Iterator
}

// Compacter is the interface for compacting a database.
type Compacter interface {
	// Compact flushes all writes to disk and performs any necessary cleanup.
	Compact(start []byte, limit []byte) error
}

// Batch is a set of operations that can be atomically committed to the database.
type Batch interface {
	KeyValueWriterDeleter

	// Size returns the current size of the batch in bytes.
	Size() int

	// Write flushes the batch to the database.
	Write() error

	// Reset resets the batch to be empty.
	Reset()

	// Replay replays the operations in the batch on the provided writer.
	Replay(w KeyValueWriterDeleter) error

	// Inner returns the inner batch, if one exists.
	Inner() Batch
}

// Iterator is an interface for iterating over key-value pairs.
type Iterator interface {
	// Next moves the iterator to the next key-value pair.
	// It returns false if the iterator is exhausted.
	Next() bool

	// Error returns any accumulated error. Exhausting all the key-value pairs
	// is not considered to be an error.
	Error() error

	// Key returns the key of the current key-value pair, or nil if done.
	Key() []byte

	// Value returns the value of the current key-value pair, or nil if done.
	Value() []byte

	// Release releases associated resources. Release should always succeed and
	// can be called multiple times without causing error.
	Release()
}

// IteratorError is a mock iterator that always returns an error.
type IteratorError struct {
	Err error
}

func (i *IteratorError) Next() bool        { return false }
func (i *IteratorError) Error() error      { return i.Err }
func (i *IteratorError) Key() []byte       { return nil }
func (i *IteratorError) Value() []byte     { return nil }
func (i *IteratorError) Release()          {}

// Pool is a collection of databases that can be reused.
type Pool interface {
	// Get returns a database from the pool.
	// If there are no databases in the pool, a new one is created.
	Get() Database

	// Put returns a database to the pool.
	Put(Database)
}

// pool implements Pool
type pool struct {
	databases chan Database
	generator func() Database
}

// NewPool creates a new database pool.
func NewPool(size int, generator func() Database) Pool {
	return &pool{
		databases: make(chan Database, size),
		generator: generator,
	}
}

func (p *pool) Get() Database {
	select {
	case db := <-p.databases:
		return db
	default:
		return p.generator()
	}
}

func (p *pool) Put(db Database) {
	select {
	case p.databases <- db:
	default:
		db.Close()
	}
}

// AtomicBatch is a batch that writes to the database atomically.
type AtomicBatch struct {
	lock  sync.Mutex
	batch Batch
}

// NewAtomicBatch creates a new atomic batch.
func NewAtomicBatch(db Database) *AtomicBatch {
	return &AtomicBatch{
		batch: db.NewBatch(),
	}
}

func (a *AtomicBatch) Put(key, value []byte) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.batch.Put(key, value)
}

func (a *AtomicBatch) Delete(key []byte) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.batch.Delete(key)
}

func (a *AtomicBatch) Size() int {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.batch.Size()
}

func (a *AtomicBatch) Write() error {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.batch.Write()
}

func (a *AtomicBatch) Reset() {
	a.lock.Lock()
	defer a.lock.Unlock()
	a.batch.Reset()
}

func (a *AtomicBatch) Replay(w KeyValueWriterDeleter) error {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.batch.Replay(w)
}

func (a *AtomicBatch) Inner() Batch {
	a.lock.Lock()
	defer a.lock.Unlock()
	return a.batch.Inner()
}