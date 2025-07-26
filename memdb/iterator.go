// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package memdb

import (
	"bytes"
	"slices"

	database "github.com/luxfi/db"
	"golang.org/x/exp/maps"
)

// iterator is an iterator over the in-memory database.
type iterator struct {
	db     *Database
	keys   []string
	values map[string][]byte

	idx int
	err error
}

// newIterator returns a new iterator over the in-memory database.
func newIterator(
	dbInstance *Database,
	db map[string][]byte,
	start []byte,
	prefix []byte,
) database.Iterator {
	if db == nil {
		return &IteratorError{
			Err: database.ErrClosed,
		}
	}
	if prefix == nil {
		prefix = []byte{}
	}
	if start == nil {
		start = prefix
	}

	keys := maps.Keys(db)
	slices.Sort(keys)

	// Remove all keys that don't have the prefix
	i := 0
	for _, key := range keys {
		keyBytes := []byte(key)
		if bytes.HasPrefix(keyBytes, prefix) {
			keys[i] = key
			i++
		}
	}
	keys = keys[:i]

	// Binary search for the first key >= start
	idx := 0
	if len(start) > 0 {
		idx = slices.IndexFunc(keys, func(key string) bool {
			return bytes.Compare([]byte(key), start) >= 0
		})
		if idx == -1 {
			idx = len(keys)
		}
	}

	return &iterator{
		db:     dbInstance,
		keys:   keys,
		values: db,
		idx:    idx - 1, // -1 because Next() increments before returning
	}
}

// Next implements db.Iterator.
func (it *iterator) Next() bool {
	if it.err != nil {
		return false
	}
	
	// Check if database is closed
	it.db.lock.RLock()
	closed := it.db.db == nil
	it.db.lock.RUnlock()
	
	if closed {
		it.err = database.ErrClosed
		return false
	}
	
	it.idx++
	return it.idx < len(it.keys)
}

// Error implements db.Iterator.
func (it *iterator) Error() error {
	return it.err
}

// Key implements db.Iterator.
func (it *iterator) Key() []byte {
	if it.idx < 0 || it.idx >= len(it.keys) || it.err != nil {
		return nil
	}
	return []byte(it.keys[it.idx])
}

// Value implements db.Iterator.
func (it *iterator) Value() []byte {
	if it.idx < 0 || it.idx >= len(it.keys) || it.err != nil {
		return nil
	}
	return it.values[it.keys[it.idx]]
}

// Release implements db.Iterator.
func (it *iterator) Release() {
	it.keys = nil
	it.values = nil
}