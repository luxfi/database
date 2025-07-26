// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package versiondb

import (
	"errors"
	"sync"

	"github.com/luxfi/database"
)

// Database wraps a database to allow version management.
type Database struct {
	lock sync.RWMutex

	baseDB    db.Database
	versions  map[string]*version
	currentID string
}

// version represents a specific version of the database.
type version struct {
	parentID string
	db       db.Database
}

// New returns a new versioned database.
func New(baseDB db.Database) *Database {
	mainVersion := &version{
		db: baseDB,
	}
	return &Database{
		baseDB:    baseDB,
		versions:  map[string]*version{"main": mainVersion},
		currentID: "main",
	}
}

// Close implements the db.Database interface.
func (db *Database) Close() error {
	db.lock.Lock()
	defer db.lock.Unlock()

	return db.baseDB.Close()
}

// HealthCheck implements the db.Database interface.
func (db *Database) HealthCheck() error {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.baseDB.HealthCheck()
}

// Current returns the current version database.
func (db *Database) Current() db.Database {
	db.lock.RLock()
	defer db.lock.RUnlock()

	return db.versions[db.currentID].db
}

// SetDatabase sets the underlying database for the current version.
func (db *Database) SetDatabase(newDB db.Database) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.versions[db.currentID].db = newDB
	return nil
}

// NewVersion creates a new version with the given ID.
func (db *Database) NewVersion(versionID string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if _, exists := db.versions[versionID]; exists {
		return errors.New("version already exists")
	}

	currentVersion := db.versions[db.currentID]
	newVersion := &version{
		parentID: db.currentID,
		db:       currentVersion.db, // Share the same database reference
	}

	db.versions[versionID] = newVersion
	return nil
}

// SetVersion sets the current version to the specified ID.
func (db *Database) SetVersion(versionID string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if _, exists := db.versions[versionID]; !exists {
		return errors.New("version does not exist")
	}

	db.currentID = versionID
	return nil
}

// Has implements the db.Database interface.
func (db *Database) Has(key []byte) (bool, error) {
	return db.Current().Has(key)
}

// Get implements the db.Database interface.
func (db *Database) Get(key []byte) ([]byte, error) {
	return db.Current().Get(key)
}

// Put implements the db.Database interface.
func (db *Database) Put(key []byte, value []byte) error {
	return db.Current().Put(key, value)
}

// Delete implements the db.Database interface.
func (db *Database) Delete(key []byte) error {
	return db.Current().Delete(key)
}

// NewBatch implements the db.Database interface.
func (db *Database) NewBatch() db.Batch {
	return db.Current().NewBatch()
}

// NewIterator implements the db.Database interface.
func (db *Database) NewIterator() db.Iterator {
	return db.Current().NewIterator()
}

// NewIteratorWithStart implements the db.Database interface.
func (db *Database) NewIteratorWithStart(start []byte) db.Iterator {
	return db.Current().NewIteratorWithStart(start)
}

// NewIteratorWithPrefix implements the db.Database interface.
func (db *Database) NewIteratorWithPrefix(prefix []byte) db.Iterator {
	return db.Current().NewIteratorWithPrefix(prefix)
}

// NewIteratorWithStartAndPrefix implements the db.Database interface.
func (db *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) db.Iterator {
	return db.Current().NewIteratorWithStartAndPrefix(start, prefix)
}

// Compact implements the db.Database interface.
func (db *Database) Compact(start []byte, limit []byte) error {
	return db.Current().Compact(start, limit)
}