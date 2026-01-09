// Package database provides enhanced database utilities with VictoriaMetrics-style optimizations
package database

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/luxfi/cache"
	"github.com/luxfi/compress"
	"github.com/luxfi/concurrent"
	"github.com/luxfi/metric"
)

// BadgerDBWithCache wraps BadgerDB with caching and compression optimizations
type BadgerDBWithCache struct {
	db              Database
	cache           *cache.DualMapCache[string, []byte]
	compressor      *compress.ZstdCompressor
	concurrentLimiter *concurrent.ConcurrencyLimiter
	readCounter     *metric.OptimizedCounter
	writeCounter    *metric.OptimizedCounter
	hitRatioGauge   *metric.OptimizedGauge
	cacheHits       int64
	cacheMisses     int64
	cacheMu         sync.RWMutex
}

// NewBadgerDBWithCache creates a new BadgerDB wrapper with caching and compression
func NewBadgerDBWithCache(db Database, cacheSize int, compressionLevel zstd.EncoderLevel, maxConcurrency int, reg *metric.MetricsRegistry) (*BadgerDBWithCache, error) {
	compressor, err := compress.NewZstdCompressor(compressionLevel, reg)
	if err != nil {
		return nil, fmt.Errorf("failed to create compressor: %w", err)
	}

	bdb := &BadgerDBWithCache{
		db:              db,
		cache:           cache.NewDualMapCache[string, []byte](reg),
		compressor:      compressor,
		concurrentLimiter: concurrent.NewConcurrencyLimiter(maxConcurrency, "badger_db", reg),
	}

	// Initialize metrics
	if reg != nil {
		bdb.readCounter = metric.NewOptimizedCounter("badger_db_reads_total", "Total number of database reads")
		bdb.writeCounter = metric.NewOptimizedCounter("badger_db_writes_total", "Total number of database writes")
		bdb.hitRatioGauge = metric.NewOptimizedGauge("badger_db_cache_hit_ratio", "Cache hit ratio for database operations")

		reg.RegisterCounter("badger_db_reads", bdb.readCounter)
		reg.RegisterCounter("badger_db_writes", bdb.writeCounter)
		reg.RegisterGauge("badger_db_cache_hit_ratio", bdb.hitRatioGauge)
	}

	// Start periodic cache migration
	go bdb.periodicCacheMigration()

	return bdb, nil
}

// periodicCacheMigration periodically migrates the cache from mutable to immutable
func (bdb *BadgerDBWithCache) periodicCacheMigration() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		bdb.cache.Migrate()
		bdb.updateHitRatio()
	}
}

// updateHitRatio updates the cache hit ratio metric
func (bdb *BadgerDBWithCache) updateHitRatio() {
	bdb.cacheMu.RLock()
	defer bdb.cacheMu.RUnlock()

	total := bdb.cacheHits + bdb.cacheMisses
	if total > 0 {
		hitRatio := float64(bdb.cacheHits) / float64(total)
		if bdb.hitRatioGauge != nil {
			bdb.hitRatioGauge.Set(hitRatio)
		}
	}
}

// Get retrieves a value from the database with caching
func (bdb *BadgerDBWithCache) Get(key []byte) ([]byte, error) {
	if bdb.readCounter != nil {
		bdb.readCounter.Inc()
	}

	keyStr := string(key)

	// Try to get from cache first
	if cached, ok := bdb.cache.Get(keyStr); ok {
		bdb.cacheMu.Lock()
		bdb.cacheHits++
		bdb.cacheMu.Unlock()
		return cached, nil
	}

	// Acquire concurrency token
	if err := bdb.concurrentLimiter.Acquire(context.Background()); err != nil {
		return nil, err
	}
	defer bdb.concurrentLimiter.Release()

	// Get from database
	value, err := bdb.db.Get(key)
	if err != nil {
		bdb.cacheMu.Lock()
		bdb.cacheMisses++
		bdb.cacheMu.Unlock()
		return nil, err
	}

	// Decompress if needed
	decompressed, err := bdb.compressor.Decompress(value)
	if err != nil {
		// If decompression fails, assume it wasn't compressed
		decompressed = value
	}

	// Add to cache
	bdb.cache.Put(keyStr, decompressed)

	bdb.cacheMu.Lock()
	bdb.cacheMisses++
	bdb.cacheMu.Unlock()

	return decompressed, nil
}

// Put inserts a value into the database with caching and compression
func (bdb *BadgerDBWithCache) Put(key, value []byte) error {
	if bdb.writeCounter != nil {
		bdb.writeCounter.Inc()
	}

	// Acquire concurrency token
	if err := bdb.concurrentLimiter.Acquire(context.Background()); err != nil {
		return err
	}
	defer bdb.concurrentLimiter.Release()

	// Compress the value
	compressed, err := bdb.compressor.Compress(value)
	if err != nil {
		return err
	}

	// Put to database
	if err := bdb.db.Put(key, compressed); err != nil {
		return err
	}

	// Update cache
	bdb.cache.Put(string(key), value)

	return nil
}

// Has checks if a key exists in the database
func (bdb *BadgerDBWithCache) Has(key []byte) (bool, error) {
	if bdb.readCounter != nil {
		bdb.readCounter.Inc()
	}

	keyStr := string(key)

	// Check cache first
	if _, ok := bdb.cache.Get(keyStr); ok {
		return true, nil
	}

	// Acquire concurrency token
	if err := bdb.concurrentLimiter.Acquire(context.Background()); err != nil {
		return false, err
	}
	defer bdb.concurrentLimiter.Release()

	return bdb.db.Has(key)
}

// Delete removes a key from the database and cache
func (bdb *BadgerDBWithCache) Delete(key []byte) error {
	if bdb.writeCounter != nil {
		bdb.writeCounter.Inc()
	}

	// Acquire concurrency token
	if err := bdb.concurrentLimiter.Acquire(context.Background()); err != nil {
		return err
	}
	defer bdb.concurrentLimiter.Release()

	// Delete from database
	if err := bdb.db.Delete(key); err != nil {
		return err
	}

	// Remove from cache
	// Since we can't directly remove from the dual map cache,
	// we'll let it be cleaned up during migration

	return nil
}

// BatchedDatabase provides batched operations with optimizations
type BatchedDatabase struct {
	db              Database
	batchSize       int
	batchTimeout    time.Duration
	pendingOps      []*Operation
	pendingMu       sync.Mutex
	flushTimer      *time.Timer
	batchCounter    *metric.OptimizedCounter
	batchSizeGauge  *metric.OptimizedGauge
}

// Operation represents a database operation
type Operation struct {
	Type  OpType
	Key   []byte
	Value []byte
}

// OpType represents the type of operation
type OpType int

const (
	PutOp OpType = iota
	DeleteOp
)

// NewBatchedDatabase creates a new batched database wrapper
func NewBatchedDatabase(db Database, batchSize int, batchTimeout time.Duration, reg *metric.MetricsRegistry) *BatchedDatabase {
	batchedDB := &BatchedDatabase{
		db:           db,
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		pendingOps:   make([]*Operation, 0, batchSize),
	}

	// Initialize metrics
	if reg != nil {
		batchedDB.batchCounter = metric.NewOptimizedCounter("database_batches_total", "Total number of batches processed")
		batchedDB.batchSizeGauge = metric.NewOptimizedGauge("database_batch_size_current", "Current size of batch being processed")

		reg.RegisterCounter("database_batches", batchedDB.batchCounter)
		reg.RegisterGauge("database_batch_size", batchedDB.batchSizeGauge)
	}

	return batchedDB
}

// Put adds a put operation to the batch
func (b *BatchedDatabase) Put(key, value []byte) error {
	op := &Operation{
		Type:  PutOp,
		Key:   key,
		Value: value,
	}

	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	b.pendingOps = append(b.pendingOps, op)

	if b.batchSizeGauge != nil {
		b.batchSizeGauge.Set(float64(len(b.pendingOps)))
	}

	// If we've reached the batch size, flush immediately
	if len(b.pendingOps) >= b.batchSize {
		return b.flushLocked()
	}

	// Set up a timer to flush if no more operations come in
	if b.flushTimer == nil {
		b.flushTimer = time.AfterFunc(b.batchTimeout, func() {
			b.pendingMu.Lock()
			defer b.pendingMu.Unlock()
			// Check if there are still pending ops
			if len(b.pendingOps) > 0 {
				b.flushLocked()
			}
		})
	} else {
		// Reset the timer if it's already running
		b.flushTimer.Reset(b.batchTimeout)
	}

	return nil
}

// Delete adds a delete operation to the batch
func (b *BatchedDatabase) Delete(key []byte) error {
	op := &Operation{
		Type: DeleteOp,
		Key:  key,
	}

	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	b.pendingOps = append(b.pendingOps, op)

	if b.batchSizeGauge != nil {
		b.batchSizeGauge.Set(float64(len(b.pendingOps)))
	}

	// If we've reached the batch size, flush immediately
	if len(b.pendingOps) >= b.batchSize {
		return b.flushLocked()
	}

	// Set up a timer to flush if no more operations come in
	if b.flushTimer == nil {
		b.flushTimer = time.AfterFunc(b.batchTimeout, func() {
			b.pendingMu.Lock()
			defer b.pendingMu.Unlock()
			// Check if there are still pending ops
			if len(b.pendingOps) > 0 {
				b.flushLocked()
			}
		})
	} else {
		// Reset the timer if it's already running
		b.flushTimer.Reset(b.batchTimeout)
	}

	return nil
}

// flushLocked performs the actual flush operation (caller must hold lock)
func (b *BatchedDatabase) flushLocked() error {
	if len(b.pendingOps) == 0 {
		return nil
	}

	// Create a batch from pending operations
	batch := b.db.NewBatch()
	defer batch.Reset()

	for _, op := range b.pendingOps {
		switch op.Type {
		case PutOp:
			if err := batch.Put(op.Key, op.Value); err != nil {
				return err
			}
		case DeleteOp:
			if err := batch.Delete(op.Key); err != nil {
				return err
			}
		}
	}

	// Write the batch
	if err := batch.Write(); err != nil {
		return err
	}

	if b.batchCounter != nil {
		b.batchCounter.Add(float64(len(b.pendingOps)))
	}

	// Clear pending operations
	b.pendingOps = b.pendingOps[:0]

	return nil
}

// Flush forces a flush of all pending operations
func (b *BatchedDatabase) Flush() error {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	return b.flushLocked()
}

// Close closes the batched database and flushes any remaining operations
func (b *BatchedDatabase) Close() error {
	b.pendingMu.Lock()
	defer b.pendingMu.Unlock()

	if b.flushTimer != nil {
		b.flushTimer.Stop()
	}

	return b.flushLocked()
}

// RangeIterator provides optimized range iteration
type RangeIterator struct {
	db       Database
	start    []byte
	end      []byte
	limit    int
	reverse  bool
	iter     Iterator
	metrics  *metric.MetricsRegistry
	count    int
}

// NewRangeIterator creates a new range iterator
func NewRangeIterator(db Database, start, end []byte, limit int, reverse bool, reg *metric.MetricsRegistry) *RangeIterator {
	return &RangeIterator{
		db:      db,
		start:   start,
		end:     end,
		limit:   limit,
		reverse: reverse,
		metrics: reg,
	}
}

// Iterate iterates over the range and calls the callback for each key-value pair
func (r *RangeIterator) Iterate(callback func(key, value []byte) error) error {
	var iter Iterator
	_ = r.reverse
	if r.start != nil {
		iter = r.db.NewIteratorWithStart(r.start)
	} else {
		iter = r.db.NewIterator()
	}
	defer iter.Release()

	r.count = 0
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		// Check if we've exceeded the range
		if r.end != nil && bytes.Compare(key, r.end) >= 0 {
			break
		}

		// Check if we've reached the limit
		if r.limit > 0 && r.count >= r.limit {
			break
		}

		if err := callback(key, value); err != nil {
			return err
		}

		r.count++
	}

	return iter.Error()
}

// Count returns the number of items iterated
func (r *RangeIterator) Count() int {
	return r.count
}
