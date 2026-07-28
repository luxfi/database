// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/luxfi/age"
	"github.com/luxfi/database"
	log "github.com/luxfi/log"
	"github.com/luxfi/metric"
	zdb "github.com/luxfi/zapdb"
	"github.com/luxfi/zapdb/options"
)

var (
	_ database.Database = (*Database)(nil)

	// emptyKeyPlaceholder is used internally to store empty keys since BadgerDB doesn't support them
	emptyKeyPlaceholder = []byte{0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF}
)

// Database is a badgerdb backed database
type Database struct {
	dbPath      string
	db          *zdb.DB
	repl        *vfsReplicator // non-nil once StartReplicator activates
	replStarted bool           // guards against double-activation (node + factory both call StartReplicator)
	closed      bool
	closeMu     sync.RWMutex
}

// New returns a new badgerdb-backed database
func New(file string, configBytes []byte, namespace string, metrics metric.Registerer) (*Database, error) {
	// BadgerDB requires a valid directory path
	if file == "" {
		return nil, errors.New("badgerdb: database path required")
	}

	// Configure BadgerDB options with optimizations
	opts := zdb.DefaultOptions(file)
	// Wrap our logger for BadgerDB
	if configBytes != nil {
		opts.Logger = &badgerLogger{namespace: namespace}
	} else {
		opts.Logger = nil // Silent mode by default
	}

	// Performance optimizations for blockchain + Verkle tree workloads.
	//
	// SyncWrites=true fsyncs every commit. That is the safe default and stays
	// the default, but it is the dominant cost on network-attached disks: on a
	// cloud block volume it is worth roughly 10 ms per write, which a
	// metadata-heavy caller pays on every single operation. Callers that can
	// accept losing the last writes on a machine crash may set
	// "syncWrites": false in the config; see applyConfig.
	opts.SyncWrites = true
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

	// If physical replication is on and this is a fresh dir, hydrate it from the
	// latest physical snapshot in S3 BEFORE Badger opens — so Badger recovers
	// straight onto the restored LSM files (logical incrementals replay later in
	// StartReplicator). No-op unless configured + dir empty + restore-on-boot.
	maybeHydratePhysicalSnapshot(file)

	// Open the database, cleaning stale .mem files on retry.
	// BadgerDB leaves .mem files on unclean shutdown; a subsequent Open
	// fails with "File …/.mem already exists" because it tries to create
	// a memtable at an ID that already has an orphaned file on disk.
	badgerDB, err := zdb.Open(opts)
	if err != nil && strings.Contains(err.Error(), ".mem already exists") {
		log.Warn(fmt.Sprintf("[zapdb] stale .mem files detected in %s, cleaning up and retrying", file))
		// BadgerDB may store memtable files in the data dir itself or a "db" subdir.
		for _, dir := range []string{file, filepath.Join(file, "db")} {
			if cleanErr := removeStaleMemFiles(dir); cleanErr != nil {
				return nil, fmt.Errorf("zapdb: failed to clean stale .mem files in %s: %w (original: %v)", dir, cleanErr, err)
			}
		}
		badgerDB, err = zdb.Open(opts)
	}
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

	d := &Database{
		dbPath: file,
		db:     badgerDB,
	}
	// Activate per-DB replication when REPLICATE_AUTO_ON_OPEN is set, so a
	// subprocess VM plugin opening its own chain DB backs it up too (the node's
	// base-DB replicator can't see across the plugin process boundary).
	d.maybeAutoReplicate()
	return d, nil
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

	// Stop the replication loop (if running) before closing the DB so an
	// in-flight backup doesn't race the close.
	if d.repl != nil {
		d.repl.Stop()
	}

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
	return nil, d.db.View(func(txn *zdb.Txn) error {
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
	err := d.db.View(func(txn *zdb.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			exists = true
			return nil
		}
		if errors.Is(err, zdb.ErrKeyNotFound) {
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
	err := d.db.View(func(txn *zdb.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if errors.Is(err, zdb.ErrKeyNotFound) {
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

	return d.db.Update(func(txn *zdb.Txn) error {
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

	return d.db.Update(func(txn *zdb.Txn) error {
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
	opts := zdb.DefaultIteratorOptions
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

// Sync implements the database.Syncer interface
// It flushes all buffered writes to persistent storage.
func (d *Database) Sync() error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	// BadgerDB's Sync() flushes all pending writes to disk
	return d.db.Sync()
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
		db:  d,
		txn: d.db.NewTransaction(false),
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
	err := d.db.View(func(txn *zdb.Txn) error {
		opts := zdb.DefaultIteratorOptions
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
	err := d.db.View(func(txn *zdb.Txn) error {
		opts := zdb.DefaultIteratorOptions
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

// Backup implements the database.Database interface
func (d *Database) Backup(w io.Writer, since uint64) (uint64, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return 0, database.ErrClosed
	}

	return d.db.Backup(w, since)
}

// Load implements the database.Database interface
func (d *Database) Load(r io.Reader) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	// 16 is a reasonable default for maxPendingWrites
	return d.db.Load(r, 16)
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

	return b.db.db.Update(func(txn *zdb.Txn) error {
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
	txn     *zdb.Txn
	iter    *zdb.Iterator
	prefix  []byte
	start   []byte
	closed  bool
	err     error
	mu      sync.RWMutex
	started bool // Track if we've started iteration
	key     []byte
	value   []byte
	db      *Database
	// shared is true when the iterator was created against a caller-owned
	// transaction (e.g. from Update/View). In that case Release() must NOT
	// Discard the txn — the caller owns its lifetime.
	shared bool
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
		if !i.shared {
			i.txn.Discard()
		}
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

// removeStaleMemFiles removes orphaned .mem files left by an unclean shutdown.
// These are empty memtable files that prevent BadgerDB from opening.
func removeStaleMemFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".mem") {
			path := filepath.Join(dir, e.Name())
			log.Warn(fmt.Sprintf("[zapdb] removing stale memtable file: %s", path))
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// Config represents BadgerDB configuration
type Config struct {
	// SyncWrites is a POINTER so the config can express three states: absent
	// (nil, keep the built-in default), explicitly true, and explicitly false.
	// As a plain bool the zero value was indistinguishable from "unset", so
	// applyConfig could only ever turn syncing ON and `"syncWrites": false`
	// was silently ignored.
	SyncWrites           *bool   `json:"syncWrites"`
	NumCompactors        int     `json:"numCompactors"`
	NumMemtables         int     `json:"numMemtables"`
	MemTableSize         int64   `json:"memTableSize"`
	ValueLogFileSize     int64   `json:"valueLogFileSize"`
	ValueLogMaxEntries   uint32  `json:"valueLogMaxEntries"`
	BlockCacheSize       int64   `json:"blockCacheSize"`
	IndexCacheSize       int64   `json:"indexCacheSize"`
	BloomFalsePositive   float64 `json:"bloomFalsePositive"`
	ValueThreshold       int     `json:"valueThreshold"`
	Compression          string  `json:"compression"`
	ZSTDCompressionLevel int     `json:"zstdCompressionLevel"`
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
func applyConfig(opts *zdb.Options, cfg *Config) {
	if cfg.SyncWrites != nil {
		opts.SyncWrites = *cfg.SyncWrites
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
	txn    *zdb.Txn
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
	if err == zdb.ErrKeyNotFound {
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
	if err == zdb.ErrKeyNotFound {
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

	opts := zdb.DefaultIteratorOptions
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

// Sync is not supported for snapshots (they are read-only)
func (s *snapshotDB) Sync() error {
	return nil // Snapshots are read-only, nothing to sync
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

	opts := zdb.DefaultIteratorOptions
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
	opts := zdb.DefaultIteratorOptions
	opts.PrefetchValues = false
	it := s.txn.NewIterator(opts)
	defer it.Close()

	for it.Rewind(); it.Valid(); it.Next() {
		count++
	}
	return count, nil
}

// Backup is not supported for snapshots
func (s *snapshotDB) Backup(w io.Writer, since uint64) (uint64, error) {
	return 0, errors.New("cannot backup snapshot")
}

// Load is not supported for snapshots
func (s *snapshotDB) Load(r io.Reader) error {
	return errors.New("cannot load into snapshot")
}

// nopBatch is a no-op batch that returns errors
type nopBatch struct {
	err error
}

func (n *nopBatch) Put([]byte, []byte) error                    { return n.err }
func (n *nopBatch) Delete([]byte) error                         { return n.err }
func (n *nopBatch) Size() int                                   { return 0 }
func (n *nopBatch) Write() error                                { return n.err }
func (n *nopBatch) Reset()                                      {}
func (n *nopBatch) Replay(database.KeyValueWriterDeleter) error { return n.err }
func (n *nopBatch) Inner() database.Batch                       { return n }

// StartReplicator starts encrypted streaming replication to S3 if configured.
// Reads config from REPLICATE_* env vars. No-op if REPLICATE_S3_ENDPOINT is not set.
// Implements database.Replicatable.
//
// On boot, if the database is empty and a snapshot exists in S3, it is restored
// (latest full snapshot + newer incrementals) synchronously before this returns,
// so a fresh node comes up on the latest replicated state instead of importing or
// replaying per-node. The continuous backup loop (full snapshot hourly, incremental
// every second) then runs in the background for the life of ctx. This call does NOT
// block. Set REPLICATE_RESTORE_ON_BOOT=false to skip restore (backup-only).
func (d *Database) StartReplicator(ctx context.Context) error {
	endpoint := os.Getenv("REPLICATE_S3_ENDPOINT")
	if endpoint == "" {
		return nil // replication not configured
	}

	// Idempotent: the node (node.initDatabase) and the database factory both call
	// StartReplicator on the base DB. Only the first wins — a second activation
	// would run a duplicate backup loop writing the same versions to the same S3
	// path. Claim under the lock so concurrent calls can't both proceed.
	d.closeMu.Lock()
	if d.replStarted {
		d.closeMu.Unlock()
		return nil
	}
	d.replStarted = true
	d.closeMu.Unlock()

	cfg := vfsReplicatorConfig{
		Endpoint:    normalizeS3Endpoint(endpoint, os.Getenv("REPLICATE_S3_USE_SSL") == "true"),
		Bucket:      envOr("REPLICATE_S3_BUCKET", "replicate"),
		Region:      envOr("REPLICATE_S3_REGION", "us-central1"),
		Path:        d.replicationPath(),
		AccessKey:   os.Getenv("REPLICATE_S3_ACCESS_KEY"),
		SecretKey:   os.Getenv("REPLICATE_S3_SECRET_KEY"),
		Compress:    os.Getenv("REPLICATE_COMPRESS") != "false", // zstd on by default
		SnapEvery:   envSeconds("REPLICATE_SNAPSHOT_INTERVAL_SECONDS"),
		IncEvery:    envSeconds("REPLICATE_INCREMENTAL_INTERVAL_SECONDS"),
		SnapMinIncs: envInt("REPLICATE_SNAPSHOT_MIN_INCREMENTS"),
		MaxPending:  16,
		DBPath:      d.dbPath,
		Physical:    os.Getenv("REPLICATE_PHYSICAL_SNAPSHOT") == "true",
		CDC:         os.Getenv("REPLICATE_CDC") == "true",
	}

	// Encryption. Resolve the age identity, then the recipient. Order of
	// precedence for the identity (the private key, needed to DECRYPT on restore):
	//   1. REPLICATE_AGE_IDENTITY (explicit secret — operator-managed),
	//   2. REPLICATE_AGE_KEY_FILE (generate-on-first-use, persist, reload) — the
	//      zero-config "PQ out of the box" path.
	// The recipient (encrypt target) defaults to the identity's own recipient, so
	// one PQ key configures BOTH directions. Backups are post-quantum (ML-KEM-768
	// + X25519 hybrid) by default whenever a key is available — PQ adds ~1.6 KB and
	// ~0.1 ms per object, so there's no reason not to.
	cfg.Identity = resolveReplicationIdentity()
	if recipientStr := os.Getenv("REPLICATE_AGE_RECIPIENT"); recipientStr != "" {
		rcs, err := age.ParseRecipients(strings.NewReader(recipientStr))
		if err != nil {
			return fmt.Errorf("parse age recipient: %w", err)
		}
		if len(rcs) > 0 {
			cfg.Recipient = rcs[0]
		}
	} else if cfg.Identity != nil {
		// Derive the encrypt target from the identity so a single PQ key suffices.
		if r := recipientFor(cfg.Identity); r != nil {
			cfg.Recipient = r
		}
	}

	replicator, err := newVFSReplicator(ctx, d.db, cfg)
	if err != nil {
		return fmt.Errorf("create replicator: %w", err)
	}

	// Restore-on-boot. Normally gated on an empty DB so we never clobber newer
	// local state with an older full snapshot. In PHYSICAL mode the full snapshot
	// was already hydrated into the LSM dir before Badger opened (so the DB is
	// non-empty here by design), and Restore only replays logical incrementals
	// NEWER than the current version — it never clobbers — so we always run it.
	if os.Getenv("REPLICATE_RESTORE_ON_BOOT") != "false" {
		empty, err := d.isEmpty()
		if err != nil {
			return fmt.Errorf("replicate: emptiness check: %w", err)
		}
		if empty || replicator.physical {
			log.Info(fmt.Sprintf("[zapdb] replicate: restoring from s3://%s/%s (empty=%v physical=%v)", cfg.Bucket, cfg.Path, empty, replicator.physical))
			if err := replicator.Restore(ctx); err != nil {
				log.Warn(fmt.Sprintf("[zapdb] replicate: restore-on-boot failed (continuing fresh): %v", err))
			} else {
				log.Info("[zapdb] replicate: restore-on-boot complete")
			}
		} else {
			log.Info("[zapdb] replicate: non-empty DB, skipping restore-on-boot")
		}
	}

	// Backer vs restorer. In a multi-node set exactly one node should WRITE the
	// shared stream; the rest restore-on-boot (above) and stop. A restore-only
	// node has already pulled state, so we just don't start the backup loop.
	d.repl = replicator
	if shouldBackup() {
		go replicator.Start(ctx)
	} else {
		log.Info("[zapdb] replicate: restore-only node (REPLICATE_BACKUP=false / not the source ordinal) — backup loop not started")
	}
	return nil
}

// shouldBackup reports whether THIS node runs the backup loop. Default true.
// REPLICATE_BACKUP=false forces restore-only. REPLICATE_SOURCE_ORDINAL (set by
// the operator from a StatefulSet's sourceNodeIndex) elects a single backer: the
// node backs up only when its own ordinal — the trailing -N of HOSTNAME/POD_NAME
// — matches. Every node still restores-on-boot; only the writer is gated.
func shouldBackup() bool {
	if os.Getenv("REPLICATE_BACKUP") == "false" {
		return false
	}
	src := os.Getenv("REPLICATE_SOURCE_ORDINAL")
	if src == "" {
		return true
	}
	host := os.Getenv("POD_NAME")
	if host == "" {
		host, _ = os.Hostname()
	}
	if i := strings.LastIndexByte(host, '-'); i >= 0 {
		return host[i+1:] == src
	}
	return true
}

// replicationPath returns the S3 key prefix this DB replicates under.
//
// By default (operator/base-DB flow) it is REPLICATE_S3_PATH verbatim — one
// stream per node holding the shared base DB (P/X + every in-process VM, which
// share the base via prefixdb).
//
// When REPLICATE_AUTO_ON_OPEN is set, EACH zapdb instance replicates to its own
// sub-prefix keyed by its on-disk location. This is required because the ZAP
// plugin transport cannot proxy a database across processes: every subprocess
// VM (e.g. the C-Chain EVM/coreth) opens its OWN zapdb under
// <chainData>/<chainID>/db, so its trie never lands in the node's base DB.
// Auto-on-open lets the plugin process activate replication of that per-chain DB
// to s3://<bucket>/<REPLICATE_S3_PATH>/<chainID> — so ALL chains, in-process and
// plugin, are backed up incrementally with one stream per physical zapdb (the
// most granular layout the multi-process architecture allows).
func (d *Database) replicationPath() string {
	base := envOr("REPLICATE_S3_PATH", d.dbPath)
	if os.Getenv("REPLICATE_AUTO_ON_OPEN") == "" {
		return base
	}
	return filepath.Join(base, dbReplicationKey(d.dbPath))
}

// dbReplicationKey derives a stable, human-readable S3 sub-key from a zapdb's
// on-disk path. Subprocess VM databases live at <chainData>/<network>/<chainID>/db
// — for those it returns the chainID. The node base DB (…/db/<network>) keys to
// "base". Anything else falls back to a sanitized tail of the path so two
// different DBs never collide.
func dbReplicationKey(p string) string {
	parts := strings.Split(filepath.Clean(p), string(filepath.Separator))
	for i, seg := range parts {
		// .../chainData/<network>/<chainID>/db
		if seg == "chainData" && i+3 < len(parts) && parts[i+3] == "db" {
			return parts[i+2]
		}
	}
	// Node base DB is opened at "<dataDir>/db/<network>" (or "<dataDir>/db").
	for i, seg := range parts {
		if seg == "db" {
			if i+1 < len(parts) {
				return "base-" + parts[i+1]
			}
			return "base"
		}
	}
	if n := len(parts); n > 0 {
		return strings.NewReplacer("/", "_", ".", "_").Replace(parts[n-1])
	}
	return "base"
}

// maybeAutoReplicate starts replication of this DB when REPLICATE_AUTO_ON_OPEN
// is set and an S3 endpoint is configured. Called from New so that every zapdb
// — including the ones subprocess VM plugins open for themselves — activates
// replication without the opener having to know about it.
//
// This runs SYNCHRONOUSLY: StartReplicator performs restore-on-boot (when the
// DB is empty) before returning, and the caller — a subprocess VM plugin about
// to hand this DB to its VM's Initialize — must observe the restored state, not
// race it. The continuous backup loop StartReplicator launches is itself async
// (`go replicator.Start`), so only the one-shot restore blocks New. A restore
// failure is logged and swallowed: a fresh chain then bootstraps normally.
func (d *Database) maybeAutoReplicate() {
	if os.Getenv("REPLICATE_AUTO_ON_OPEN") == "" || os.Getenv("REPLICATE_S3_ENDPOINT") == "" {
		return
	}
	if err := d.StartReplicator(context.Background()); err != nil {
		log.Warn(fmt.Sprintf("[zapdb] auto-replicate %s: %v", d.dbPath, err))
	}
}

// normalizeS3Endpoint ensures the endpoint carries an http(s) scheme, derived
// from REPLICATE_S3_USE_SSL when absent (the vfs s3 backend wants a full URL).
func normalizeS3Endpoint(endpoint string, useSSL bool) string {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return endpoint
	}
	if useSSL {
		return "https://" + endpoint
	}
	return "http://" + endpoint
}

// isEmpty reports whether the database holds no keys. Used to gate restore-on-boot.
func (d *Database) isEmpty() (bool, error) {
	empty := true
	err := d.db.View(func(txn *zdb.Txn) error {
		opts := zdb.DefaultIteratorOptions
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

// envSeconds parses an integer-seconds env var into a Duration (0 if unset/invalid,
// which lets newVFSReplicator apply its defaults: 1h snapshot, 1s incremental).
func envSeconds(key string) time.Duration {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return 0
}

// maybeHydratePhysicalSnapshot restores a physical (SST-copy) snapshot into a
// fresh LSM dir before Badger opens it. Gated: physical mode on, auto-on-open
// (per-DB streams), restore-on-boot not disabled, S3 configured, and the dir
// empty (never clobber existing data). Best-effort — a miss just starts fresh.
func maybeHydratePhysicalSnapshot(file string) {
	if os.Getenv("REPLICATE_PHYSICAL_SNAPSHOT") != "true" ||
		os.Getenv("REPLICATE_S3_ENDPOINT") == "" ||
		os.Getenv("REPLICATE_AUTO_ON_OPEN") == "" ||
		os.Getenv("REPLICATE_RESTORE_ON_BOOT") == "false" {
		return
	}
	if entries, _ := os.ReadDir(file); len(entries) > 0 {
		return // not empty — don't overwrite live state
	}
	if err := os.MkdirAll(file, 0o755); err != nil {
		return
	}
	cfg := vfsReplicatorConfig{
		Endpoint:  normalizeS3Endpoint(os.Getenv("REPLICATE_S3_ENDPOINT"), os.Getenv("REPLICATE_S3_USE_SSL") == "true"),
		Bucket:    envOr("REPLICATE_S3_BUCKET", "replicate"),
		Region:    envOr("REPLICATE_S3_REGION", "us-central1"),
		Path:      (&Database{dbPath: file}).replicationPath(),
		AccessKey: os.Getenv("REPLICATE_S3_ACCESS_KEY"),
		SecretKey: os.Getenv("REPLICATE_S3_SECRET_KEY"),
	}
	v, err := latestPhysicalInto(context.Background(), cfg, resolveReplicationIdentity(), file)
	if err != nil {
		log.Warn(fmt.Sprintf("[zapdb] replicate: physical hydrate %s: %v", file, err))
		return
	}
	if v > 0 {
		log.Info(fmt.Sprintf("[zapdb] replicate: hydrated physical snapshot version %d into %s", v, file))
	}
}

func envInt(key string) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return 0
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
