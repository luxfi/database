// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metricdb

import (
	"context"
	"time"

	"github.com/luxfi/database"
	"github.com/luxfi/metric"
)

// Database wraps a database and records metrics for each operation.
type Database struct {
	db database.Database

	readDuration  metric.Histogram
	writeDuration metric.Histogram
	readSize      metric.Histogram
	writeSize     metric.Histogram
	readCount     metric.Counter
	writeCount    metric.Counter
	deleteCount   metric.Counter
}

// New returns a new database that records metrics.
func New(namespace string, db database.Database, registerer metric.Registerer) (*Database, error) {
	var metrics metric.Metrics
	if reg, ok := registerer.(metric.Registry); ok {
		metrics = metric.NewWithRegistry(namespace, reg)
	} else {
		metrics = metric.New(namespace)
	}

	readDuration := metrics.NewHistogram("db_read_duration", "Time spent reading from the database", exponentialBuckets(0.0001, 2, 10))
	writeDuration := metrics.NewHistogram("db_write_duration", "Time spent writing to the database", exponentialBuckets(0.0001, 2, 10))
	readSize := metrics.NewHistogram("db_read_size", "Size of values read from the database", exponentialBuckets(1, 2, 20))
	writeSize := metrics.NewHistogram("db_write_size", "Size of values written to the database", exponentialBuckets(1, 2, 20))
	readCount := metrics.NewCounter("db_read_count", "Number of database reads")
	writeCount := metrics.NewCounter("db_write_count", "Number of database writes")
	deleteCount := metrics.NewCounter("db_delete_count", "Number of database deletes")

	return &Database{
		db:            db,
		readDuration:  readDuration,
		writeDuration: writeDuration,
		readSize:      readSize,
		writeSize:     writeSize,
		readCount:     readCount,
		writeCount:    writeCount,
		deleteCount:   deleteCount,
	}, nil
}

func exponentialBuckets(start, factor float64, count int) []float64 {
	if count <= 0 {
		return nil
	}
	buckets := make([]float64, count)
	current := start
	for i := 0; i < count; i++ {
		buckets[i] = current
		current *= factor
	}
	return buckets
}

// Close implements the database.Database interface.
func (mdb *Database) Close() error {
	return mdb.db.Close()
}

// HealthCheck implements the database.Database interface.
func (mdb *Database) HealthCheck(ctx context.Context) (interface{}, error) {
	return mdb.db.HealthCheck(ctx)
}

// Has implements the database.Database interface.
func (mdb *Database) Has(key []byte) (bool, error) {
	start := time.Now()
	has, err := mdb.db.Has(key)
	mdb.readDuration.Observe(time.Since(start).Seconds())
	mdb.readCount.Inc()
	return has, err
}

// Get implements the database.Database interface.
func (mdb *Database) Get(key []byte) ([]byte, error) {
	start := time.Now()
	value, err := mdb.db.Get(key)
	mdb.readDuration.Observe(time.Since(start).Seconds())
	mdb.readCount.Inc()
	if err == nil {
		mdb.readSize.Observe(float64(len(value)))
	}
	return value, err
}

// Put implements the database.Database interface.
func (mdb *Database) Put(key []byte, value []byte) error {
	start := time.Now()
	err := mdb.db.Put(key, value)
	mdb.writeDuration.Observe(time.Since(start).Seconds())
	mdb.writeCount.Inc()
	mdb.writeSize.Observe(float64(len(key) + len(value)))
	return err
}

// Delete implements the database.Database interface.
func (mdb *Database) Delete(key []byte) error {
	start := time.Now()
	err := mdb.db.Delete(key)
	mdb.writeDuration.Observe(time.Since(start).Seconds())
	mdb.deleteCount.Inc()
	return err
}

// NewBatch implements the database.Database interface.
func (mdb *Database) NewBatch() database.Batch {
	return &batch{
		Batch:         mdb.db.NewBatch(),
		writeDuration: mdb.writeDuration,
		writeSize:     mdb.writeSize,
		writeCount:    mdb.writeCount,
		deleteCount:   mdb.deleteCount,
	}
}

// NewIterator implements the database.Database interface.
func (mdb *Database) NewIterator() database.Iterator {
	return mdb.db.NewIterator()
}

// NewIteratorWithStart implements the database.Database interface.
func (mdb *Database) NewIteratorWithStart(start []byte) database.Iterator {
	return mdb.db.NewIteratorWithStart(start)
}

// NewIteratorWithPrefix implements the database.Database interface.
func (mdb *Database) NewIteratorWithPrefix(prefix []byte) database.Iterator {
	return mdb.db.NewIteratorWithPrefix(prefix)
}

// NewIteratorWithStartAndPrefix implements the database.Database interface.
func (mdb *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	return mdb.db.NewIteratorWithStartAndPrefix(start, prefix)
}

// Compact implements the database.Database interface.
func (mdb *Database) Compact(start []byte, limit []byte) error {
	return mdb.db.Compact(start, limit)
}

// Sync implements the database.Syncer interface.
func (mdb *Database) Sync() error {
	return mdb.db.Sync()
}

// batch wraps a database.Batch to record metrics.
type batch struct {
	database.Batch

	writeDuration metric.Histogram
	writeSize     metric.Histogram
	writeCount    metric.Counter
	deleteCount   metric.Counter

	size int
}

// Put implements the database.Batch interface.
func (b *batch) Put(key, value []byte) error {
	b.size += len(key) + len(value)
	b.writeCount.Inc()
	return b.Batch.Put(key, value)
}

// Delete implements the database.Batch interface.
func (b *batch) Delete(key []byte) error {
	b.size += len(key)
	b.deleteCount.Inc()
	return b.Batch.Delete(key)
}

// Write implements the database.Batch interface.
func (b *batch) Write() error {
	start := time.Now()
	err := b.Batch.Write()
	b.writeDuration.Observe(time.Since(start).Seconds())
	b.writeSize.Observe(float64(b.size))
	return err
}

// Inner implements the database.Batch interface.
func (b *batch) Inner() database.Batch {
	return b.Batch
}
