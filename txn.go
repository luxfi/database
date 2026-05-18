// Copyright (C) 2020-2026, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package database

import "time"

// Txn is a single read/write transaction over a Transactional database.
// Operations within a Txn are atomic: either all are committed on nil
// return from the callback, or all are rolled back if the callback
// returns an error.
//
// A Txn is not safe for concurrent use across goroutines.
type Txn interface {
	// Has reports whether key is present in the transaction's view.
	Has(key []byte) (bool, error)

	// Get returns the value for key in the transaction's view.
	// Returns ErrNotFound if the key is not present.
	Get(key []byte) ([]byte, error)

	// Put writes (key, value) within the transaction. Visible only after
	// the enclosing Update commits.
	Put(key, value []byte) error

	// PutWithTTL writes (key, value) with an expiration. A ttl <= 0 means
	// no TTL (equivalent to Put). The exact eviction semantics depend on
	// the backend; readers may see the key briefly after expiry.
	PutWithTTL(key, value []byte, ttl time.Duration) error

	// Delete removes key from the transaction's view.
	Delete(key []byte) error

	// NewIterator returns an iterator over the transaction's view.
	NewIterator() Iterator

	// NewIteratorWithPrefix returns an iterator over keys with the given prefix.
	NewIteratorWithPrefix(prefix []byte) Iterator

	// NewIteratorWithStart returns an iterator starting at start.
	NewIteratorWithStart(start []byte) Iterator
}

// Transactional is an optional interface implemented by databases that
// support callback-style atomic transactions. Callers detect support via
// type assertion or UnwrapTo[Transactional].
//
// Update runs fn in a writable transaction. If fn returns nil, the
// transaction is committed; if fn returns an error, the transaction is
// rolled back and the error is returned. Implementations may retry fn on
// transient conflicts, so fn MUST be idempotent.
//
// View runs fn in a read-only transaction. Writes inside fn return
// ErrReadOnlyTxn.
type Transactional interface {
	Update(fn func(txn Txn) error) error
	View(fn func(txn Txn) error) error
}

// TTLWriter is an optional interface for databases that support per-key
// TTL outside of a transaction. Callers detect support via type assertion
// or UnwrapTo[TTLWriter].
//
// A ttl <= 0 is equivalent to Put (no expiration).
type TTLWriter interface {
	PutWithTTL(key, value []byte, ttl time.Duration) error
}
