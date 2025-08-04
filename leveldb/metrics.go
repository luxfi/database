// Copyright (C) 2019-2024, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package leveldb

import (
	"errors"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/syndtr/goleveldb/leveldb"
)

var levelLabels = []string{"level"}

type metrics struct {
	// total number of writes that have been delayed due to compaction
	writesDelayedCount prometheus.Counter
	// total amount of time (in ns) that writes that have been delayed due to
	// compaction
	writesDelayedDuration prometheus.Gauge
	// set to 1 if there is currently at least one write that is being delayed
	// due to compaction
	writeIsDelayed prometheus.Gauge

	// number of currently alive snapshots
	aliveSnapshots prometheus.Gauge
	// number of currently alive iterators
	aliveIterators prometheus.Gauge

	// total amount of data written
	ioWrite prometheus.Counter
	// total amount of data read
	ioRead prometheus.Counter

	// total number of bytes of cached data blocks
	blockCacheSize prometheus.Gauge
	// current number of open tables
	openTables prometheus.Gauge

	// number of tables per level
	levelTableCount *prometheus.GaugeVec
	// size of each level
	levelSize *prometheus.GaugeVec
	// amount of time spent compacting each level
	levelDuration *prometheus.GaugeVec
	// amount of bytes read while compacting each level
	levelReads *prometheus.CounterVec
	// amount of bytes written while compacting each level
	levelWrites *prometheus.CounterVec

	// total number memory compactions performed
	memCompactions prometheus.Counter
	// total number of level 0 compactions performed
	level0Compactions prometheus.Counter
	// total number of non-level 0 compactions performed
	nonLevel0Compactions prometheus.Counter
	// total number of seek compactions performed
	seekCompactions prometheus.Counter

	priorStats, currentStats *leveldb.DBStats
}

func newMetrics(reg prometheus.Registerer) (metrics, error) {
	m := metrics{
		writesDelayedCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "writes_delayed",
			Help: "number of cumulative writes that have been delayed due to compaction",
		}),
		writesDelayedDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "writes_delayed_duration",
			Help: "amount of time (in ns) that writes have been delayed due to compaction",
		}),
		writeIsDelayed: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "write_delayed",
			Help: "1 if there is currently a write that is being delayed due to compaction",
		}),

		aliveSnapshots: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "alive_snapshots",
			Help: "number of currently alive snapshots",
		}),
		aliveIterators: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "alive_iterators",
			Help: "number of currently alive iterators",
		}),

		ioWrite: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "io_write",
			Help: "cumulative amount of io write during compaction",
		}),
		ioRead: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "io_read",
			Help: "cumulative amount of io read during compaction",
		}),

		blockCacheSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "block_cache_size",
			Help: "total size of cached blocks",
		}),
		openTables: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "open_tables",
			Help: "number of currently opened tables",
		}),

		levelTableCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "table_count",
				Help: "number of tables allocated by level",
			},
			levelLabels,
		),
		levelSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "size",
				Help: "amount of bytes allocated by level",
			},
			levelLabels,
		),
		levelDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "duration",
				Help: "amount of time (in ns) spent in compaction by level",
			},
			levelLabels,
		),
		levelReads: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "reads",
				Help: "amount of bytes read during compaction by level",
			},
			levelLabels,
		),
		levelWrites: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "writes",
				Help: "amount of bytes written during compaction by level",
			},
			levelLabels,
		),

		memCompactions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "mem_comps",
			Help: "total number of memory compactions performed",
		}),
		level0Compactions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "level_0_comps",
			Help: "total number of level 0 compactions performed",
		}),
		nonLevel0Compactions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "non_level_0_comps",
			Help: "total number of non-level 0 compactions performed",
		}),
		seekCompactions: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "seek_comps",
			Help: "total number of seek compactions performed",
		}),

		priorStats:   &leveldb.DBStats{},
		currentStats: &leveldb.DBStats{},
	}

	err := errors.Join(
		reg.Register(m.writesDelayedCount),
		reg.Register(m.writesDelayedDuration),
		reg.Register(m.writeIsDelayed),

		reg.Register(m.aliveSnapshots),
		reg.Register(m.aliveIterators),

		reg.Register(m.ioWrite),
		reg.Register(m.ioRead),

		reg.Register(m.blockCacheSize),
		reg.Register(m.openTables),

		reg.Register(m.levelTableCount),
		reg.Register(m.levelSize),
		reg.Register(m.levelDuration),
		reg.Register(m.levelReads),
		reg.Register(m.levelWrites),

		reg.Register(m.memCompactions),
		reg.Register(m.level0Compactions),
		reg.Register(m.nonLevel0Compactions),
		reg.Register(m.seekCompactions),
	)
	return m, err
}

func (db *Database) updateMetrics() error {
	// TODO: Fix metrics integration with current Database struct
	// The metrics field and DB field are not available in the current Database struct
	return nil
}
