// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

// Package hookdb wraps a database to fire async callbacks on write operations.
// This enables real-time indexing without polling.
package hookdb

import (
	"bytes"
	"context"
	"sync"

	"github.com/luxfi/database"
)

var (
	_ database.Database = (*Database)(nil)
	_ database.Batch    = (*batch)(nil)
)

// Event represents a database write event
type Event struct {
	Type   EventType
	Key    []byte
	Value  []byte
	Prefix []byte // Which prefix triggered this event
}

// EventType indicates the type of database operation
type EventType int

const (
	EventPut EventType = iota
	EventDelete
	EventBatchWrite
)

// Handler is called when a write event occurs
// Handlers are called asynchronously and should not block
type Handler func(Event)

// PrefixHandler registers a handler for a specific key prefix
type PrefixHandler struct {
	Prefix  []byte
	Handler Handler
}

// Database wraps another database to fire callbacks on writes
type Database struct {
	db database.Database

	mu       sync.RWMutex
	handlers []PrefixHandler

	// Async event channel
	events chan Event
	wg     sync.WaitGroup
	closed bool
}

// Config for the hook database
type Config struct {
	// BufferSize for the async event channel (default: 10000)
	BufferSize int
	// Workers is the number of async workers (default: 4)
	Workers int
}

// DefaultConfig returns sensible defaults
func DefaultConfig() Config {
	return Config{
		BufferSize: 10000,
		Workers:    4,
	}
}

// New wraps a database with hook capabilities
func New(db database.Database, cfg Config) *Database {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = 10000
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}

	hdb := &Database{
		db:     db,
		events: make(chan Event, cfg.BufferSize),
	}

	// Start async workers
	for i := 0; i < cfg.Workers; i++ {
		hdb.wg.Add(1)
		go hdb.worker()
	}

	return hdb
}

// RegisterHandler adds a handler for a specific key prefix
// Pass nil prefix to receive all events
func (db *Database) RegisterHandler(prefix []byte, handler Handler) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.handlers = append(db.handlers, PrefixHandler{
		Prefix:  prefix,
		Handler: handler,
	})
}

// worker processes events asynchronously
func (db *Database) worker() {
	defer db.wg.Done()
	for event := range db.events {
		db.mu.RLock()
		for _, ph := range db.handlers {
			if ph.Prefix == nil || bytes.HasPrefix(event.Key, ph.Prefix) {
				event.Prefix = ph.Prefix
				ph.Handler(event)
			}
		}
		db.mu.RUnlock()
	}
}

// emit sends an event to the async workers
func (db *Database) emit(event Event) {
	select {
	case db.events <- event:
	default:
		// Channel full, drop event (could add metrics here)
	}
}

func (db *Database) Has(key []byte) (bool, error) {
	return db.db.Has(key)
}

func (db *Database) Get(key []byte) ([]byte, error) {
	return db.db.Get(key)
}

func (db *Database) Put(key, value []byte) error {
	err := db.db.Put(key, value)
	if err == nil {
		db.emit(Event{Type: EventPut, Key: key, Value: value})
	}
	return err
}

func (db *Database) Delete(key []byte) error {
	err := db.db.Delete(key)
	if err == nil {
		db.emit(Event{Type: EventDelete, Key: key})
	}
	return err
}

func (db *Database) NewBatch() database.Batch {
	return &batch{
		Batch: db.db.NewBatch(),
		db:    db,
	}
}

func (db *Database) NewIterator() database.Iterator {
	return db.db.NewIterator()
}

func (db *Database) NewIteratorWithStart(start []byte) database.Iterator {
	return db.db.NewIteratorWithStart(start)
}

func (db *Database) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return db.db.NewIteratorWithPrefix(prefix)
}

func (db *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	return db.db.NewIteratorWithStartAndPrefix(start, prefix)
}

func (db *Database) Compact(start, limit []byte) error {
	return db.db.Compact(start, limit)
}

func (db *Database) Sync() error {
	return db.db.Sync()
}

func (db *Database) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	close(db.events)
	db.wg.Wait()
	return db.db.Close()
}

func (db *Database) HealthCheck(ctx context.Context) (interface{}, error) {
	return db.db.HealthCheck(ctx)
}

// batch wraps a database batch to track writes
type batch struct {
	database.Batch
	db     *Database
	events []Event
}

func (b *batch) Put(key, value []byte) error {
	err := b.Batch.Put(key, value)
	if err == nil {
		// Copy key/value since they may be reused
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)
		b.events = append(b.events, Event{Type: EventPut, Key: keyCopy, Value: valueCopy})
	}
	return err
}

func (b *batch) Delete(key []byte) error {
	err := b.Batch.Delete(key)
	if err == nil {
		keyCopy := make([]byte, len(key))
		copy(keyCopy, key)
		b.events = append(b.events, Event{Type: EventDelete, Key: keyCopy})
	}
	return err
}

func (b *batch) Write() error {
	err := b.Batch.Write()
	if err == nil {
		// Emit all events after successful batch write
		for _, event := range b.events {
			b.db.emit(event)
		}
	}
	return err
}

func (b *batch) Reset() {
	b.Batch.Reset()
	b.events = b.events[:0]
}

func (b *batch) Inner() database.Batch {
	return b.Batch
}
