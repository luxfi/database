// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build pebbledb

package pebbledb

import (
	"context"
	"runtime"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	database "github.com/luxfi/database"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	// pebbleByteOverHead is the number of bytes of constant overhead that
	// should be added to a batch size per operation.
	pebbleByteOverHead = 8
)

var _ database.Database = (*Database)(nil)

// Database is a persistent key-value store implementing the Database interface.
// Apart from basic data storage functionality it also supports batch writes and
// iterating over the keyspace in binary-alphabetical order.
type Database struct {
	lock          sync.RWMutex
	pebbleDB      *pebble.DB
	closed        bool
	openIterators set[*iter]
	writeOptions  *pebble.WriteOptions

	// Metrics
	compTimeMeter      prometheus.Summary
	compReadMeter      prometheus.Counter
	compWriteMeter     prometheus.Counter
	writeDelayNMeter   prometheus.Summary
	writeDelayMeter    prometheus.Summary
	diskSizeGauge      prometheus.Gauge
	diskReadMeter      prometheus.Counter
	diskWriteMeter     prometheus.Counter
	memCompGauge       prometheus.Gauge
	level0CompGauge    prometheus.Gauge
	nonlevel0CompGauge prometheus.Gauge
	seekCompGauge      prometheus.Gauge
}

// New returns a new PebbleDB database.
func New(path string, cacheSize int, handles int, namespace string, readonly bool) (*Database, error) {
	// Set default options
	opts := &pebble.Options{
		Cache:                       pebble.NewCache(int64(cacheSize * 1024 * 1024)),
		DisableWAL:                  false,
		MaxOpenFiles:                handles,
		MaxConcurrentCompactions:    runtime.NumCPU,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 * 1024 * 1024,  // 64 MB
		MaxManifestFileSize:         128 * 1024 * 1024, // 128 MB
		MemTableSize:                4 * 1024 * 1024,   // 4 MB
		MemTableStopWritesThreshold: 2,
		ReadOnly:                    readonly,
	}

	// Configure bloom filters
	opts.Levels = make([]pebble.LevelOptions, 7)
	for i := 0; i < len(opts.Levels); i++ {
		opts.Levels[i].BlockSize = 32 * 1024       // 32 KB
		opts.Levels[i].IndexBlockSize = 256 * 1024 // 256 KB
		opts.Levels[i].FilterPolicy = bloom.FilterPolicy(10)
		opts.Levels[i].FilterType = pebble.TableFilter
		if i > 0 {
			opts.Levels[i].TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
	}
	opts.Levels[0].TargetFileSize = 2 * 1024 * 1024 // 2 MB

	// Open database
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, err
	}

	// Create database wrapper
	database := &Database{
		pebbleDB:      db,
		openIterators: make(set[*iter]),
		writeOptions:  &pebble.WriteOptions{Sync: true},
	}

	// Configure metrics if namespace is provided
	if namespace != "" {
		database.compTimeMeter = prometheus.NewSummary(prometheus.SummaryOpts{
			Namespace: namespace,
			Subsystem: "pebbledb",
			Name:      "compact_time",
			Help:      "Time spent in compaction",
		})
		database.compReadMeter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "pebbledb",
			Name:      "compact_read",
			Help:      "Bytes read during compaction",
		})
		database.compWriteMeter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "pebbledb",
			Name:      "compact_write",
			Help:      "Bytes written during compaction",
		})
		database.diskSizeGauge = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: "pebbledb",
			Name:      "disk_size",
			Help:      "Size of the database on disk",
		})
		database.diskReadMeter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "pebbledb",
			Name:      "disk_read",
			Help:      "Bytes read from disk",
		})
		database.diskWriteMeter = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: "pebbledb",
			Name:      "disk_write",
			Help:      "Bytes written to disk",
		})
	}

	return database, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (d *Database) Close() error {
	if d == nil {
		return nil
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	if d.closed {
		return database.ErrClosed
	}

	d.closed = true

	for iter := range d.openIterators {
		iter.lock.Lock()
		iter.release()
		iter.lock.Unlock()
	}
	d.openIterators.Clear()

	return updateError(d.pebbleDB.Close())
}

// HealthCheck returns nil if the database is healthy, or an error otherwise.
func (d *Database) HealthCheck(ctx context.Context) (interface{}, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if d.closed {
		return nil, database.ErrClosed
	}
	return nil, nil
}

// Has retrieves if a key is present in the key-value store.
func (d *Database) Has(key []byte) (bool, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if d.closed {
		return false, database.ErrClosed
	}

	_, closer, err := d.pebbleDB.Get(key)
	if err == pebble.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, updateError(err)
	}
	return true, closer.Close()
}

// Get retrieves the given key if it's present in the key-value store.
func (d *Database) Get(key []byte) ([]byte, error) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if d.closed {
		return nil, database.ErrClosed
	}

	data, closer, err := d.pebbleDB.Get(key)
	if err != nil {
		return nil, updateError(err)
	}
	ret := make([]byte, len(data))
	copy(ret, data)
	return ret, closer.Close()
}

// Put inserts the given value into the key-value store.
func (d *Database) Put(key []byte, value []byte) error {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	return updateError(d.pebbleDB.Set(key, value, d.writeOptions))
}

// Delete removes the key from the key-value store.
func (d *Database) Delete(key []byte) error {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	return updateError(d.pebbleDB.Delete(key, d.writeOptions))
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (d *Database) NewIterator() database.Iterator {
	return d.NewIteratorWithPrefix(nil)
}

// NewIteratorWithStart creates a binary-alphabetical iterator over a subset of
// database content starting at a particular initial key (or after, if it does
// not exist).
func (d *Database) NewIteratorWithStart(start []byte) database.Iterator {
	return d.NewIteratorWithStartAndPrefix(start, nil)
}

// NewIteratorWithPrefix creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix.
func (d *Database) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return d.NewIteratorWithStartAndPrefix(nil, prefix)
}

// NewIteratorWithStartAndPrefix creates a binary-alphabetical iterator over a
// subset of database content with a particular key prefix, starting at a
// particular initial key (or after, if it does not exist).
func (d *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.closed {
		return &iter{
			db:     d,
			closed: true,
			err:    database.ErrClosed,
		}
	}

	it, err := d.pebbleDB.NewIter(keyRange(start, prefix))
	if err != nil {
		return &iter{
			db:     d,
			closed: true,
			err:    updateError(err),
		}
	}

	iterator := &iter{
		db:   d,
		iter: it,
	}
	d.openIterators.Add(iterator)
	return iterator
}

func keyRange(start, prefix []byte) *pebble.IterOptions {
	opt := &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixToUpperBound(prefix),
	}
	if pebble.DefaultComparer.Compare(start, prefix) == 1 {
		opt.LowerBound = start
	}
	return opt
}

// Returns an upper bound that stops after all keys with the given [prefix].
// Assumes the Database uses bytes.Compare for key comparison and not a custom
// comparer.
func prefixToUpperBound(prefix []byte) []byte {
	for i := len(prefix) - 1; i >= 0; i-- {
		if prefix[i] != 0xFF {
			upperBound := make([]byte, i+1)
			copy(upperBound, prefix)
			upperBound[i]++
			return upperBound
		}
	}
	return nil
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both are nil then it
// will compact entire data store.
func (d *Database) Compact(start []byte, end []byte) error {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	if end == nil {
		// The database.Database spec treats a nil [limit] as a key after all
		// keys but pebble treats a nil [limit] as a key before all keys in
		// Compact. Use the greatest key in the database as the [limit] to get
		// the desired behavior.
		it, err := d.pebbleDB.NewIter(&pebble.IterOptions{})
		if err != nil {
			return updateError(err)
		}

		if !it.Last() {
			// The database is empty.
			return it.Close()
		}

		end = make([]byte, len(it.Key()))
		copy(end, it.Key())
		if err := it.Close(); err != nil {
			return err
		}
	}

	if pebble.DefaultComparer.Compare(start, end) >= 1 {
		// pebble requires [start] < [end]
		return nil
	}

	return updateError(d.pebbleDB.Compact(start, end, true /* parallelize */))
}

// Sync implements the database.Syncer interface
// It flushes all buffered writes to persistent storage.
func (d *Database) Sync() error {
	d.lock.RLock()
	defer d.lock.RUnlock()

	if d.closed {
		return database.ErrClosed
	}

	// Pebble's LogData with Sync option forces a WAL sync
	// This ensures all written data is persisted to disk
	return updateError(d.pebbleDB.LogData(nil, pebble.Sync))
}
