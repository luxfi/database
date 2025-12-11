// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package badgerdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	"github.com/luxfi/database"
	"github.com/luxfi/log"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	_ database.Database = (*Database)(nil)

	// emptyKeyPlaceholder is used internally to store empty keys since BadgerDB doesn't support them
	emptyKeyPlaceholder = []byte{0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF}
)

// Database is a badgerdb backed database
type Database struct {
	dbPath  string
	db      *badger.DB
	closed  bool
	closeMu sync.RWMutex
}

// New returns a new badgerdb-backed database
func New(file string, configBytes []byte, namespace string, metrics prometheus.Registerer) (*Database, error) {
	// BadgerDB requires a valid directory path
	if file == "" {
		return nil, errors.New("badgerdb: database path required")
	}

	// Configure BadgerDB options with optimizations
	opts := badger.DefaultOptions(file)
	// Wrap our logger for BadgerDB
	if configBytes != nil {
		opts.Logger = &badgerLogger{namespace: namespace}
	} else {
		opts.Logger = nil // Silent mode by default
	}

	// Performance optimizations for blockchain + Verkle tree workloads
	opts.SyncWrites = false           // Async writes for better performance
	opts.NumCompactors = 4            // More compactors for faster background compaction
	opts.NumLevelZeroTables = 10      // More L0 tables before stalling
	opts.NumLevelZeroTablesStall = 15 // Stall writes if L0 tables exceed this
	opts.NumMemtables = 5             // More memtables for better write throughput
	opts.MemTableSize = 64 << 20      // 64 MB memtable size
	opts.ValueLogFileSize = 1 << 30   // 1 GB value log files
	opts.ValueLogMaxEntries = 1000000 // Max entries per value log file
	opts.BlockCacheSize = 256 << 20   // 256 MB block cache
	opts.IndexCacheSize = 100 << 20   // 100 MB index cache
	opts.BloomFalsePositive = 0.01    // 1% false positive rate for bloom filter
	opts.BaseTableSize = 64 << 20     // 64 MB base table size
	opts.BaseLevelSize = 256 << 20    // 256 MB base level size
	opts.TableSizeMultiplier = 2      // Table size multiplier for each level
	opts.LevelSizeMultiplier = 10     // Standard LSM tree multiplier
	opts.MaxLevels = 7                // Standard number of levels

	// Verkle-optimized: Store anything > 256 bytes in value log
	// Verkle keys are ~32 bytes, small values stay in LSM for fast access
	// Verkle proofs (often > 256 bytes) go to value log
	opts.ValueThreshold = 256

	opts.NumVersionsToKeep = 1   // Only keep 1 version for blockchain data
	opts.CompactL0OnClose = true // Compact L0 tables on close
	// Note: KeepBlocksInCache and KeepBlockIndicesInCache are no longer in BadgerDB v4
	// Cache management is automatic
	opts.Compression = options.Snappy // Use Snappy compression
	opts.ZSTDCompressionLevel = 1     // Fast ZSTD compression if used
	opts.BlockSize = 4 * 1024         // 4KB block size
	opts.DetectConflicts = false      // No conflict detection for single writer

	// Note: LoadingMode (MemoryMap) is no longer in BadgerDB v4
	// Memory management is automatic

	// Parse custom config if provided
	if len(configBytes) > 0 {
		if cfg, err := parseConfig(configBytes); err == nil {
			applyConfig(&opts, cfg)
		}
	}

	// Open the database
	badgerDB, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	// Start garbage collection in background
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			lsm, vlog := badgerDB.Size()
			if vlog > 1<<30 { // If value log > 1GB, run GC
				badgerDB.RunValueLogGC(0.5)
			}
			_ = lsm // Avoid unused variable
		}
	}()

	return &Database{
		dbPath: file,
		db:     badgerDB,
	}, nil
}

// Close implements the Database interface
func (d *Database) Close() error {
	if d == nil {
		return nil
	}

	d.closeMu.Lock()
	defer d.closeMu.Unlock()

	if d.closed {
		return database.ErrClosed
	}
	d.closed = true

	if d.db == nil {
		return nil
	}
	return d.db.Close()
}

// HealthCheck returns nil if the database is healthy, non-nil otherwise.
func (d *Database) HealthCheck(ctx context.Context) (interface{}, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return nil, database.ErrClosed
	}
	// BadgerDB doesn't have a direct health check, but we can try a simple operation
	return nil, d.db.View(func(txn *badger.Txn) error {
		return nil
	})
}

// Has implements the Database interface
func (d *Database) Has(key []byte) (bool, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return false, database.ErrClosed
	}

	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	var exists bool
	err := d.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			exists = true
			return nil
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			exists = false
			return nil
		}
		return err
	})
	return exists, err
}

// Get implements the Database interface
func (d *Database) Get(key []byte) ([]byte, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return nil, database.ErrClosed
	}

	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	var value []byte
	err := d.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, badger.ErrKeyNotFound) {
				return database.ErrNotFound
			}
			return err
		}
		value, err = item.ValueCopy(nil)
		return err
	})
	return value, err
}

// Put implements the Database interface
func (d *Database) Put(key []byte, value []byte) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete implements the Database interface
func (d *Database) Delete(key []byte) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	return d.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// NewBatch implements the Database interface
func (d *Database) NewBatch() database.Batch {
	return &batch{
		db:    d,
		ops:   make([]batchOp, 0, 16),
		size:  0,
		reset: true,
	}
}

// NewIterator implements the Database interface
func (d *Database) NewIterator() database.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, nil)
}

// NewIteratorWithStart implements the Database interface
func (d *Database) NewIteratorWithStart(start []byte) database.Iterator {
	return d.NewIteratorWithStartAndPrefix(start, nil)
}

// NewIteratorWithPrefix implements the Database interface
func (d *Database) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, prefix)
}

// NewIteratorWithStartAndPrefix implements the Database interface
func (d *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return &nopIterator{err: database.ErrClosed}
	}

	txn := d.db.NewTransaction(false)
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10

	it := txn.NewIterator(opts)
	iter := &iterator{
		txn:    txn,
		iter:   it,
		prefix: prefix,
		start:  start,
		closed: false,
		db:     d,
	}

	// Initialize iterator position
	if len(start) > 0 {
		it.Seek(start)
	} else if len(prefix) > 0 {
		it.Seek(prefix)
	} else {
		it.Rewind()
	}

	// If using prefix, ensure we're at a valid position
	if len(prefix) > 0 && it.Valid() && !bytes.HasPrefix(it.Item().Key(), prefix) {
		// Move to next valid item or mark as exhausted
		iter.Next()
	}

	return iter
}

// Compact implements the Database interface
func (d *Database) Compact(start []byte, limit []byte) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	// BadgerDB handles compaction automatically in the background
	// We can trigger a manual compaction if needed
	// The start and limit parameters are ignored as BadgerDB doesn't support range compaction
	err := d.db.RunValueLogGC(0.5)
	// BadgerDB returns an error if GC didn't result in any cleanup, but that's not really an error
	if err != nil && err.Error() == "Value log GC attempt didn't result in any cleanup" {
		return nil
	}
	return err
}

// GetSnapshot implements the database.Database interface
func (d *Database) GetSnapshot() (database.Database, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return nil, database.ErrClosed
	}

	// Create a new read-only transaction that acts as a snapshot
	// This provides a consistent view of the database at this point in time
	return &snapshotDB{
		db:   d,
		txn:  d.db.NewTransaction(false),
	}, nil
}

// Empty returns true if the database doesn't contain any keys (but not deleted keys)
func (d *Database) Empty() (bool, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return false, database.ErrClosed
	}

	empty := true
	err := d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if it.Valid() {
			empty = false
		}
		return nil
	})
	return empty, err
}

// Len returns the number of keys in the database
func (d *Database) Len() (int, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return 0, database.ErrClosed
	}

	count := 0
	err := d.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			count++
		}
		return nil
	})
	return count, err
}

// batch represents a batch of database operations
type batch struct {
	db    *Database
	ops   []batchOp
	size  int
	reset bool
}

type batchOp struct {
	key    []byte
	value  []byte
	delete bool
}

// Put implements the Batch interface
func (b *batch) Put(key, value []byte) error {
	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	b.ops = append(b.ops, batchOp{
		key:   append([]byte{}, key...),
		value: append([]byte{}, value...),
	})
	b.size += len(key) + len(value)
	return nil
}

// Delete implements the Batch interface
func (b *batch) Delete(key []byte) error {
	// Handle empty keys using placeholder
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	b.ops = append(b.ops, batchOp{
		key:    append([]byte{}, key...),
		delete: true,
	})
	b.size += len(key)
	return nil
}

// Write implements the Batch interface
func (b *batch) Write() error {
	if b.db.closed {
		return database.ErrClosed
	}

	return b.db.db.Update(func(txn *badger.Txn) error {
		for _, op := range b.ops {
			if op.delete {
				if err := txn.Delete(op.key); err != nil {
					return err
				}
			} else {
				if err := txn.Set(op.key, op.value); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// WriteSync implements the Batch interface
func (b *batch) WriteSync() error {
	// BadgerDB syncs by default
	return b.Write()
}

// Reset implements the Batch interface
func (b *batch) Reset() {
	if b.reset {
		b.ops = b.ops[:0]
		b.size = 0
	}
}

// Replay implements the Batch interface
func (b *batch) Replay(w database.KeyValueWriterDeleter) error {
	for _, op := range b.ops {
		if op.delete {
			if err := w.Delete(op.key); err != nil {
				return err
			}
		} else {
			if err := w.Put(op.key, op.value); err != nil {
				return err
			}
		}
	}
	return nil
}

// SetResetDisabled implements the Batch interface
func (b *batch) SetResetDisabled(disabled bool) {
	b.reset = !disabled
}

// GetResetDisabled implements the Batch interface
func (b *batch) GetResetDisabled() bool {
	return !b.reset
}

// Inner implements the Batch interface
func (b *batch) Inner() database.Batch {
	return b
}

// Size implements the Batch interface
func (b *batch) Size() int {
	return b.size
}

// iterator wraps a BadgerDB iterator
type iterator struct {
	txn     *badger.Txn
	iter    *badger.Iterator
	prefix  []byte
	start   []byte
	closed  bool
	err     error
	mu      sync.RWMutex
	started bool // Track if we've started iteration
	key     []byte
	value   []byte
	db      *Database
}

// Next implements the Iterator interface
func (i *iterator) Next() bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Check if iterator is closed
	if i.closed {
		i.err = database.ErrClosed
		i.key = nil
		i.value = nil
		return false
	}

	// Check if underlying iterator is nil (shouldn't happen but be defensive)
	if i.iter == nil {
		i.err = database.ErrClosed
		i.key = nil
		i.value = nil
		return false
	}

	// Check if database is closed
	if i.db != nil {
		i.db.closeMu.RLock()
		dbClosed := i.db.closed
		i.db.closeMu.RUnlock()

		if dbClosed {
			i.err = database.ErrClosed
			i.key = nil
			i.value = nil
			return false
		}
	}

	// If this is the first call to Next() and we're already at a valid position
	if !i.started {
		i.started = true
		if i.iter.Valid() {
			// Check prefix constraint
			if len(i.prefix) > 0 && !bytes.HasPrefix(i.iter.Item().Key(), i.prefix) {
				i.key = nil
				i.value = nil
				return false
			}
			// Store current key/value
			i.key = i.getKey()
			i.value = i.getValue()
			return true
		}
		// Iterator was not valid on first call - nothing to iterate
		i.key = nil
		i.value = nil
		return false
	}

	// Only call Next() if the iterator is currently valid
	// (Badger's Next() panics if item is nil, which happens when not valid)
	if !i.iter.Valid() {
		i.key = nil
		i.value = nil
		return false
	}

	i.iter.Next()

	// Check if we're still valid and within prefix bounds
	if !i.iter.Valid() {
		i.key = nil
		i.value = nil
		return false
	}

	if len(i.prefix) > 0 && !bytes.HasPrefix(i.iter.Item().Key(), i.prefix) {
		i.key = nil
		i.value = nil
		return false
	}

	// Store current key/value
	i.key = i.getKey()
	i.value = i.getValue()
	return true
}

// Error implements the Iterator interface
func (i *iterator) Error() error {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.err
}

// Key implements the Iterator interface
func (i *iterator) Key() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.key
}

// getKey returns a copy of the current iterator key
func (i *iterator) getKey() []byte {
	if i.closed || i.iter == nil || !i.iter.Valid() {
		return nil
	}

	key := i.iter.Item().Key()
	// Check if this is our empty key placeholder
	if bytes.Equal(key, emptyKeyPlaceholder) {
		return []byte{}
	}
	return append([]byte{}, key...)
}

// Value implements the Iterator interface
func (i *iterator) Value() []byte {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.value
}

// getValue returns a copy of the current iterator value
func (i *iterator) getValue() []byte {
	if i.closed || i.iter == nil || !i.iter.Valid() {
		return nil
	}

	value, err := i.iter.Item().ValueCopy(nil)
	if err != nil {
		return nil
	}
	return value
}

// Release implements the Iterator interface
func (i *iterator) Release() {
	i.mu.Lock()
	defer i.mu.Unlock()

	if !i.closed {
		i.closed = true
		i.key = nil
		i.value = nil
		if i.iter != nil {
			i.iter.Close()
		}
		i.txn.Discard()
	}
}

// nopIterator is a no-op iterator that returns an error
type nopIterator struct {
	err error
}

func (n *nopIterator) Next() bool    { return false }
func (n *nopIterator) Error() error  { return n.err }
func (n *nopIterator) Key() []byte   { return nil }
func (n *nopIterator) Value() []byte { return nil }
func (n *nopIterator) Release()      {}

// Config represents BadgerDB configuration
type Config struct {
	SyncWrites          bool   `json:"syncWrites"`
	NumCompactors       int    `json:"numCompactors"`
	NumMemtables        int    `json:"numMemtables"`
	MemTableSize        int64  `json:"memTableSize"`
	ValueLogFileSize    int64  `json:"valueLogFileSize"`
	ValueLogMaxEntries  uint32 `json:"valueLogMaxEntries"`
	BlockCacheSize      int64  `json:"blockCacheSize"`
	IndexCacheSize      int64  `json:"indexCacheSize"`
	BloomFalsePositive  float64 `json:"bloomFalsePositive"`
	ValueThreshold      int    `json:"valueThreshold"`
	Compression         string `json:"compression"`
	ZSTDCompressionLevel int    `json:"zstdCompressionLevel"`
}

// parseConfig parses JSON configuration for BadgerDB
func parseConfig(configBytes []byte) (*Config, error) {
	var cfg Config
	if err := json.Unmarshal(configBytes, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse BadgerDB config: %w", err)
	}
	return &cfg, nil
}

// applyConfig applies parsed configuration to BadgerDB options
func applyConfig(opts *badger.Options, cfg *Config) {
	if cfg.SyncWrites {
		opts.SyncWrites = true
	}
	if cfg.NumCompactors > 0 {
		opts.NumCompactors = cfg.NumCompactors
	}
	if cfg.NumMemtables > 0 {
		opts.NumMemtables = cfg.NumMemtables
	}
	if cfg.MemTableSize > 0 {
		opts.MemTableSize = cfg.MemTableSize
	}
	if cfg.ValueLogFileSize > 0 {
		opts.ValueLogFileSize = cfg.ValueLogFileSize
	}
	if cfg.ValueLogMaxEntries > 0 {
		opts.ValueLogMaxEntries = cfg.ValueLogMaxEntries
	}
	if cfg.BlockCacheSize > 0 {
		opts.BlockCacheSize = cfg.BlockCacheSize
	}
	if cfg.IndexCacheSize > 0 {
		opts.IndexCacheSize = cfg.IndexCacheSize
	}
	if cfg.BloomFalsePositive > 0 {
		opts.BloomFalsePositive = cfg.BloomFalsePositive
	}
	if cfg.ValueThreshold > 0 {
		opts.ValueThreshold = int64(cfg.ValueThreshold)
	}
	if cfg.Compression != "" {
		switch cfg.Compression {
		case "snappy":
			opts.Compression = options.Snappy
		case "zstd":
			opts.Compression = options.ZSTD
		case "none":
			opts.Compression = options.None
		}
	}
	if cfg.ZSTDCompressionLevel > 0 {
		opts.ZSTDCompressionLevel = cfg.ZSTDCompressionLevel
	}
}

// badgerLogger wraps our logger for BadgerDB
type badgerLogger struct {
	namespace string
}

// Errorf logs an error message
func (l *badgerLogger) Errorf(format string, args ...interface{}) {
	// Log to stderr for now
	log.Error(fmt.Sprintf("[%s] %s", l.namespace, fmt.Sprintf(format, args...)))
}

// Warningf logs a warning message
func (l *badgerLogger) Warningf(format string, args ...interface{}) {
	log.Warn(fmt.Sprintf("[%s] %s", l.namespace, fmt.Sprintf(format, args...)))
}

// Infof logs an info message
func (l *badgerLogger) Infof(format string, args ...interface{}) {
	log.Info(fmt.Sprintf("[%s] %s", l.namespace, fmt.Sprintf(format, args...)))
}

// Debugf logs a debug message
func (l *badgerLogger) Debugf(format string, args ...interface{}) {
	log.Debug(fmt.Sprintf("[%s] %s", l.namespace, fmt.Sprintf(format, args...)))
}

// snapshotDB represents a snapshot of the database at a point in time
type snapshotDB struct {
	db     *Database
	txn    *badger.Txn
	closed bool
	mu     sync.RWMutex
}

// Has implements the Database interface for snapshot
func (s *snapshotDB) Has(key []byte) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, database.ErrClosed
	}

	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	_, err := s.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	return err == nil, err
}

// Get implements the Database interface for snapshot
func (s *snapshotDB) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, database.ErrClosed
	}

	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	item, err := s.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return nil, database.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return item.ValueCopy(nil)
}

// Put is not supported for snapshots
func (s *snapshotDB) Put(key []byte, value []byte) error {
	return errors.New("cannot modify snapshot")
}

// Delete is not supported for snapshots
func (s *snapshotDB) Delete(key []byte) error {
	return errors.New("cannot modify snapshot")
}

// NewBatch is not supported for snapshots
func (s *snapshotDB) NewBatch() database.Batch {
	return &nopBatch{err: errors.New("cannot create batch on snapshot")}
}

// NewIterator creates an iterator for the snapshot
func (s *snapshotDB) NewIterator() database.Iterator {
	return s.NewIteratorWithStartAndPrefix(nil, nil)
}

// NewIteratorWithStart creates an iterator for the snapshot
func (s *snapshotDB) NewIteratorWithStart(start []byte) database.Iterator {
	return s.NewIteratorWithStartAndPrefix(start, nil)
}

// NewIteratorWithPrefix creates an iterator for the snapshot
func (s *snapshotDB) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return s.NewIteratorWithStartAndPrefix(nil, prefix)
}

// NewIteratorWithStartAndPrefix creates an iterator for the snapshot
func (s *snapshotDB) NewIteratorWithStartAndPrefix(start []byte, prefix []byte) database.Iterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return &nopIterator{err: database.ErrClosed}
	}

	opts := badger.DefaultIteratorOptions
	if len(prefix) > 0 {
		opts.Prefix = prefix
	}

	it := s.txn.NewIterator(opts)
	iter := &iterator{
		txn:    s.txn,
		iter:   it,
		prefix: prefix,
		start:  start,
		db:     s.db,
	}

	if len(start) > 0 {
		it.Seek(start)
	} else if len(prefix) > 0 {
		it.Seek(prefix)
	} else {
		it.Rewind()
	}

	return iter
}

// Close releases the snapshot
func (s *snapshotDB) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.closed {
		s.closed = true
		s.txn.Discard()
	}
	return nil
}

// HealthCheck implements the Database interface for snapshot
func (s *snapshotDB) HealthCheck(ctx context.Context) (interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, database.ErrClosed
	}
	// Snapshot is healthy if not closed
	return nil, nil
}

// Compact is not supported for snapshots
func (s *snapshotDB) Compact(start []byte, limit []byte) error {
	return errors.New("cannot compact snapshot")
}

// GetSnapshot is not supported for snapshots
func (s *snapshotDB) GetSnapshot() (database.Database, error) {
	return nil, errors.New("cannot create snapshot of snapshot")
}

// Empty checks if the snapshot is empty
func (s *snapshotDB) Empty() (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, database.ErrClosed
	}

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := s.txn.NewIterator(opts)
	defer it.Close()

	it.Rewind()
	return !it.Valid(), nil
}

// Len returns the number of keys in the snapshot
func (s *snapshotDB) Len() (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return 0, database.ErrClosed
	}

	count := 0
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := s.txn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	return count, nil
}

// nopBatch is a no-op batch that returns errors
type nopBatch struct {
	err error
}

func (n *nopBatch) Put([]byte, []byte) error { return n.err }
func (n *nopBatch) Delete([]byte) error      { return n.err }
func (n *nopBatch) Size() int                { return 0 }
func (n *nopBatch) Write() error             { return n.err }
func (n *nopBatch) Reset()                   {}
func (n *nopBatch) Replay(database.KeyValueWriterDeleter) error { return n.err }
func (n *nopBatch) Inner() database.Batch    { return n }
