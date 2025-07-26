// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package metricdb

import (
	"time"

	"github.com/luxfi/database"
	"github.com/prometheus/client_golang/prometheus"
)

// Database wraps a database and records metrics for each operation.
type Database struct {
	db db.Database

	readDuration  prometheus.Histogram
	writeDuration prometheus.Histogram
	readSize      prometheus.Histogram
	writeSize     prometheus.Histogram
	readCount     prometheus.Counter
	writeCount    prometheus.Counter
	deleteCount   prometheus.Counter
}

// New returns a new database that records metrics.
func New(namespace string, db db.Database, registerer prometheus.Registerer) (*Database, error) {
	readDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "db_read_duration",
		Help:      "Time spent reading from the database",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 10),
	})
	writeDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "db_write_duration",
		Help:      "Time spent writing to the database",
		Buckets:   prometheus.ExponentialBuckets(0.0001, 2, 10),
	})
	readSize := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "db_read_size",
		Help:      "Size of values read from the database",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	writeSize := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "db_write_size",
		Help:      "Size of values written to the database",
		Buckets:   prometheus.ExponentialBuckets(1, 2, 20),
	})
	readCount := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "db_read_count",
		Help:      "Number of database reads",
	})
	writeCount := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "db_write_count",
		Help:      "Number of database writes",
	})
	deleteCount := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "db_delete_count",
		Help:      "Number of database deletes",
	})

	if registerer != nil {
		if err := registerer.Register(readDuration); err != nil {
			return nil, err
		}
		if err := registerer.Register(writeDuration); err != nil {
			return nil, err
		}
		if err := registerer.Register(readSize); err != nil {
			return nil, err
		}
		if err := registerer.Register(writeSize); err != nil {
			return nil, err
		}
		if err := registerer.Register(readCount); err != nil {
			return nil, err
		}
		if err := registerer.Register(writeCount); err != nil {
			return nil, err
		}
		if err := registerer.Register(deleteCount); err != nil {
			return nil, err
		}
	}

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

// Close implements the db.Database interface.
func (mdb *Database) Close() error {
	return mdb.db.Close()
}

// HealthCheck implements the db.Database interface.
func (mdb *Database) HealthCheck() error {
	return mdb.db.HealthCheck()
}

// Has implements the db.Database interface.
func (mdb *Database) Has(key []byte) (bool, error) {
	start := time.Now()
	has, err := mdb.db.Has(key)
	mdb.readDuration.Observe(time.Since(start).Seconds())
	mdb.readCount.Inc()
	return has, err
}

// Get implements the db.Database interface.
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

// Put implements the db.Database interface.
func (mdb *Database) Put(key []byte, value []byte) error {
	start := time.Now()
	err := mdb.db.Put(key, value)
	mdb.writeDuration.Observe(time.Since(start).Seconds())
	mdb.writeCount.Inc()
	mdb.writeSize.Observe(float64(len(key) + len(value)))
	return err
}

// Delete implements the db.Database interface.
func (mdb *Database) Delete(key []byte) error {
	start := time.Now()
	err := mdb.db.Delete(key)
	mdb.writeDuration.Observe(time.Since(start).Seconds())
	mdb.deleteCount.Inc()
	return err
}

// NewBatch implements the db.Database interface.
func (mdb *Database) NewBatch() db.Batch {
	return &batch{
		Batch:         mdb.db.NewBatch(),
		writeDuration: mdb.writeDuration,
		writeSize:     mdb.writeSize,
		writeCount:    mdb.writeCount,
		deleteCount:   mdb.deleteCount,
	}
}

// NewIterator implements the db.Database interface.
func (mdb *Database) NewIterator() db.Iterator {
	return mdb.db.NewIterator()
}

// NewIteratorWithStart implements the db.Database interface.
func (mdb *Database) NewIteratorWithStart(start []byte) db.Iterator {
	return mdb.db.NewIteratorWithStart(start)
}

// NewIteratorWithPrefix implements the db.Database interface.
func (mdb *Database) NewIteratorWithPrefix(prefix []byte) db.Iterator {
	return mdb.db.NewIteratorWithPrefix(prefix)
}

// NewIteratorWithStartAndPrefix implements the db.Database interface.
func (mdb *Database) NewIteratorWithStartAndPrefix(start, prefix []byte) db.Iterator {
	return mdb.db.NewIteratorWithStartAndPrefix(start, prefix)
}

// Compact implements the db.Database interface.
func (mdb *Database) Compact(start []byte, limit []byte) error {
	return mdb.db.Compact(start, limit)
}

// batch wraps a db.Batch to record metrics.
type batch struct {
	db.Batch

	writeDuration prometheus.Histogram
	writeSize     prometheus.Histogram
	writeCount    prometheus.Counter
	deleteCount   prometheus.Counter

	size int
}

// Put implements the db.Batch interface.
func (b *batch) Put(key, value []byte) error {
	b.size += len(key) + len(value)
	b.writeCount.Inc()
	return b.Batch.Put(key, value)
}

// Delete implements the db.Batch interface.
func (b *batch) Delete(key []byte) error {
	b.size += len(key)
	b.deleteCount.Inc()
	return b.Batch.Delete(key)
}

// Write implements the db.Batch interface.
func (b *batch) Write() error {
	start := time.Now()
	err := b.Batch.Write()
	b.writeDuration.Observe(time.Since(start).Seconds())
	b.writeSize.Observe(float64(b.size))
	return err
}

// Inner implements the db.Batch interface.
func (b *batch) Inner() db.Batch {
	return b.Batch
}