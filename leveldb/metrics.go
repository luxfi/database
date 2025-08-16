// Copyright (C) 2019-2024, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package leveldb

import (
	"github.com/syndtr/goleveldb/leveldb"
	
	metric "github.com/luxfi/metric"
)

var levelLabels = []string{"level"}

type leveldbMetrics struct {
	// total number of writes that have been delayed due to compaction
	writesDelayedCount metric.Counter
	// total amount of time (in ns) that writes that have been delayed due to
	// compaction
	writesDelayedDuration metric.Gauge
	// set to 1 if there is currently at least one write that is being delayed
	// due to compaction
	writeIsDelayed metric.Gauge

	// number of currently alive snapshots
	aliveSnapshots metric.Gauge
	// number of currently alive iterators
	aliveIterators metric.Gauge

	// total amount of data written
	ioWrite metric.Counter
	// total amount of data read
	ioRead metric.Counter

	// total number of bytes of cached data blocks
	blockCacheSize metric.Gauge
	// current number of open tables
	openTables metric.Gauge

	// number of tables per level
	levelTableCount metric.GaugeVec
	// size of each level
	levelSize metric.GaugeVec
	// amount of time spent compacting each level
	levelDuration metric.GaugeVec
	// amount of bytes read while compacting each level
	levelReads metric.CounterVec
	// amount of bytes written while compacting each level
	levelWrites metric.CounterVec

	// total number memory compactions performed
	memCompactions metric.Counter
	// total number of level 0 compactions performed
	level0Compactions metric.Counter
	// total number of non-level 0 compactions performed
	nonLevel0Compactions metric.Counter
	// total number of seek compactions performed
	seekCompactions metric.Counter

	priorStats, currentStats *leveldb.DBStats
}

func newMetrics(reg metric.Metrics) (leveldbMetrics, error) {
	// Convert to using luxfi metrics instead of prometheus directly
	m := leveldbMetrics{
		writesDelayedCount: reg.NewCounter(
			"writes_delayed",
			"number of cumulative writes that have been delayed due to compaction",
		),
		writesDelayedDuration: reg.NewGauge(
			"writes_delayed_duration",
			"amount of time (in ns) that writes have been delayed due to compaction",
		),
		writeIsDelayed: reg.NewGauge(
			"write_delayed",
			"1 if there is currently a write that is being delayed due to compaction",
		),

		aliveSnapshots: reg.NewGauge(
			"alive_snapshots",
			"number of currently alive snapshots",
		),
		aliveIterators: reg.NewGauge(
			"alive_iterators",
			"number of currently alive iterators",
		),

		ioWrite: reg.NewCounter(
			"io_write",
			"amount of data written",
		),
		ioRead: reg.NewCounter(
			"io_read",
			"amount of data read",
		),

		blockCacheSize: reg.NewGauge(
			"block_cache_size",
			"total amount of cached data blocks",
		),
		openTables: reg.NewGauge(
			"open_tables",
			"number of currently open tables",
		),

		levelTableCount: reg.NewGaugeVec(
			"level_table_count",
			"number of tables in a level",
			levelLabels,
		),
		levelSize: reg.NewGaugeVec(
			"level_size",
			"amount of bytes in a level",
			levelLabels,
		),
		levelDuration: reg.NewGaugeVec(
			"level_duration",
			"amount of time spent compacting a level",
			levelLabels,
		),
		levelReads: reg.NewCounterVec(
			"level_reads",
			"amount of bytes read while compacting a level",
			levelLabels,
		),
		levelWrites: reg.NewCounterVec(
			"level_writes",
			"amount of bytes written while compacting a level",
			levelLabels,
		),

		memCompactions: reg.NewCounter(
			"mem_compactions",
			"cumulative number of memory compactions performed",
		),
		level0Compactions: reg.NewCounter(
			"level0_compactions",
			"cumulative number of level 0 compactions performed",
		),
		nonLevel0Compactions: reg.NewCounter(
			"non_level0_compactions",
			"cumulative number of non-level 0 compactions performed",
		),
		seekCompactions: reg.NewCounter(
			"seek_compactions",
			"cumulative number of seek compactions performed",
		),

		priorStats:   &leveldb.DBStats{},
		currentStats: &leveldb.DBStats{},
	}

	// Metrics are already registered when created through the Metrics interface
	// No need to explicitly register them
	return m, nil
}

func (db *Database) updateMetrics() error {
	// TODO: Fix metrics integration with current Database struct
	// The metrics field and DB field are not available in the current Database struct
	return nil
}