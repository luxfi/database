// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package memdb

import (
	"sync"

	"github.com/luxfi/database"
)

var _ database.HeightIndex = (*Database)(nil)

// Database is an in-memory implementation of HeightIndex.
// It provides thread-safe height-based key-value storage for testing
// and high-performance paths.
type Database struct {
	lock   sync.RWMutex
	data   map[uint64][]byte
	closed bool
}

// New creates a new in-memory HeightIndex database.
func New() *Database {
	return &Database{
		data: make(map[uint64][]byte),
	}
}

// Put inserts the value at the given height.
// The value is copied to prevent external mutation.
func (db *Database) Put(height uint64, value []byte) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return database.ErrClosed
	}

	// Clone the value to ensure callers can safely modify after Put
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	db.data[height] = valueCopy
	return nil
}

// Get retrieves the value at the given height.
// Returns database.ErrNotFound if the height does not exist.
// The returned byte slice is safe to read, but cannot be modified.
func (db *Database) Get(height uint64) ([]byte, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if db.closed {
		return nil, database.ErrClosed
	}

	value, exists := db.data[height]
	if !exists {
		return nil, database.ErrNotFound
	}
	return value, nil
}

// Has returns whether a value exists at the given height.
func (db *Database) Has(height uint64) (bool, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	if db.closed {
		return false, database.ErrClosed
	}

	_, exists := db.data[height]
	return exists, nil
}

// Delete removes the value at the given height.
// Does not return an error if the height does not exist.
func (db *Database) Delete(height uint64) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return database.ErrClosed
	}

	delete(db.data, height)
	return nil
}

// Close releases resources and clears all stored data.
// Subsequent operations will return database.ErrClosed.
func (db *Database) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.closed {
		return database.ErrClosed
	}

	db.closed = true
	// Clear the map to release memory
	db.data = nil
	return nil
}
