// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

// For ease of implementation, our database's interface matches Ethereum's
// database implementation. This was to allow us to use Geth code as is for the
// EVM chain.

package database

import (
	"context"
	"io"
)

const (
	// MaxExcessCapacityFactor is the factor above which capacity is considered excessive
	MaxExcessCapacityFactor = 4
	// CapacityReductionFactor is the factor by which to reduce capacity when it's excessive
	CapacityReductionFactor = 2
)

// KeyValueReader wraps the Has and Get method of a backing data store.
type KeyValueReader interface {
	// Has retrieves if a key is present in the key-value data store.
	//
	// Note: [key] is safe to modify and read after calling Has.
	Has(key []byte) (bool, error)

	// Get retrieves the given key if it's present in the key-value data store.
	// Returns ErrNotFound if the key is not present in the key-value data store.
	//
	// Note: [key] is safe to modify and read after calling Get.
	// The returned byte slice is safe to read, but cannot be modified.
	Get(key []byte) ([]byte, error)
}

// KeyValueWriter wraps the Put method of a backing data store.
type KeyValueWriter interface {
	// Put inserts the given value into the key-value data store.
	//
	// Note: [key] and [value] are safe to modify and read after calling Put.
	//
	// If [value] is nil or an empty slice, then when it's retrieved
	// it may be nil or an empty slice.
	//
	// Similarly, a nil [key] is treated the same as an empty slice.
	Put(key []byte, value []byte) error
}

// KeyValueDeleter wraps the Delete method of a backing data store.
type KeyValueDeleter interface {
	// Delete removes the key from the key-value data store.
	//
	// Note: [key] is safe to modify and read after calling Delete.
	Delete(key []byte) error
}

// KeyValueReaderWriter allows read/write access to a backing data store.
type KeyValueReaderWriter interface {
	KeyValueReader
	KeyValueWriter
}

// KeyValueWriterDeleter allows write/delete access to a backing data store.
type KeyValueWriterDeleter interface {
	KeyValueWriter
	KeyValueDeleter
}

// KeyValueReaderWriterDeleter allows read/write/delete access to a backing data store.
type KeyValueReaderWriterDeleter interface {
	KeyValueReader
	KeyValueWriter
	KeyValueDeleter
}

// Compacter wraps the Compact method of a backing data store.
type Compacter interface {
	// Compact the underlying DB for the given key range.
	// Specifically, deleted and overwritten versions are discarded,
	// and the data is rearranged to reduce the cost of operations
	// needed to access the data. This operation should typically only
	// be invoked by users who understand the underlying implementation.
	//
	// A nil start is treated as a key before all keys in the DB.
	// And a nil limit is treated as a key after all keys in the DB.
	// Therefore if both are nil then it will compact entire DB.
	//
	// Note: [start] and [limit] are safe to modify and read after calling Compact.
	Compact(start []byte, limit []byte) error
}

// Syncer wraps the Sync method of a backing data store.
type Syncer interface {
	// Sync flushes all buffered writes to persistent storage.
	// This ensures that data written before Sync is called will
	// survive a crash or power failure. This is important for
	// databases configured with async writes for performance.
	//
	// Implementations should ensure all in-memory buffers (memtables,
	// write-ahead logs, etc.) are flushed to disk.
	Sync() error
}

// Database contains all the methods required to allow handling different
// key-value data stores backing the database.
type Database interface {
	KeyValueReaderWriterDeleter
	Batcher
	Iteratee
	Compacter
	Syncer
	io.Closer
	HealthCheck(context.Context) (interface{}, error)

	// Backup performs a backup of the database to the provided writer.
	// This should be a consistent snapshot of the database state.
	// since: 0 for full backup, or the version of the last backup for incremental.
	// returns: the version (timestamp/sequence) of this backup, to be used for the next incremental.
	Backup(w io.Writer, since uint64) (uint64, error)

	// Load restores the database from the provided reader.
	// This will overwrite the current database state with the backup.
	// Note: The database should generally be empty or fresh when calling Load.
	Load(r io.Reader) error
}

// HeightIndex provides height-based key-value storage for blockchain state.
// Heights are stored as big-endian uint64 keys for efficient range queries.
type HeightIndex interface {
	// Put inserts the value at the given height.
	//
	// Note: [value] is safe to modify and read after calling Put.
	//
	// If [value] is nil or an empty slice, then when it's retrieved
	// it may be nil or an empty slice.
	Put(height uint64, value []byte) error

	// Get retrieves the value at the given height.
	// Returns ErrNotFound if the height does not exist.
	//
	// The returned byte slice is safe to read, but cannot be modified.
	Get(height uint64) ([]byte, error)

	// Has retrieves if a value exists at the given height.
	Has(height uint64) (bool, error)

	// Delete removes the value at the given height.
	Delete(height uint64) error

	// Close releases any resources associated with the index.
	Close() error
}
