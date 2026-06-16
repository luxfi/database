// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/luxfi/database"
	"github.com/luxfi/database/memdb"
	"github.com/luxfi/database/meterdb"
	"github.com/luxfi/database/versiondb"
	"github.com/luxfi/database/zapdb"
	log "github.com/luxfi/log"
	"github.com/luxfi/metric"
)

// DatabaseFactory is a function that creates a database
type DatabaseFactory func(
	dbPath string,
	config []byte,
	logger log.Logger,
	registerer metric.Registerer,
	metricsPrefix string,
	readOnly bool,
) (database.Database, error)

var (
	factoryMu sync.RWMutex
	factories = make(map[string]DatabaseFactory)

	// replOnce guards base-DB replication so it activates exactly once per process.
	replOnce sync.Once
)

// startReplicationIfBase activates ZapDB→S3 replication for the node's single
// base database: restore-on-boot (a fresh, empty DB pulls the latest snapshot +
// incrementals before any chain opens) plus a background snapshot/incremental
// loop. It is a no-op unless REPLICATE_S3_ENDPOINT is set AND this is the base
// DB — node.initDatabase passes meterDBRegName=="all"; VM-plugin subprocesses
// pass a different name ("meterdb"), so they never replicate and can't collide
// on the S3 path even though they inherit the same REPLICATE_* env. The hook
// runs inside factory.New (before it returns), so restore completes before the
// node reads genesis or opens chains.
func startReplicationIfBase(db database.Database, meterDBRegName string) database.Database {
	if meterDBRegName != "all" || os.Getenv("REPLICATE_S3_ENDPOINT") == "" {
		return db
	}
	replOnce.Do(func() {
		r, ok := database.UnwrapTo[database.Replicatable](db)
		if !ok {
			log.Warn("zapdb: base DB is not Replicatable, skipping replication")
			return
		}
		if err := r.StartReplicator(context.Background()); err != nil {
			log.Warn(fmt.Sprintf("zapdb: failed to start base-DB replication: %v", err))
		}
	})
	return db
}

// RegisterDatabase registers a database factory for a given name
func RegisterDatabase(name string, factory DatabaseFactory) {
	factoryMu.Lock()
	defer factoryMu.Unlock()
	factories[name] = factory
}

// AvailableDatabases returns the list of available database types
func AvailableDatabases() []string {
	factoryMu.RLock()
	defer factoryMu.RUnlock()
	names := make([]string, 0, len(factories)+2)
	names = append(names, zapdb.Name, memdb.Name)
	for name := range factories {
		names = append(names, name)
	}
	return names
}

func init() {
	// zapdb is always available (default database)
	// Other databases (pebbledb, leveldb) available with build tags:
	//   -tags=pebbledb  for PebbleDB
	//   -tags=leveldb   for LevelDB
	factory := func(
		dbPath string,
		config []byte,
		logger log.Logger,
		registerer metric.Registerer,
		metricsPrefix string,
		readOnly bool,
	) (database.Database, error) {
		return zapdb.New(dbPath, config, zapdb.Name, registerer)
	}
	RegisterDatabase(zapdb.Name, factory)
	// Register under legacy name for backwards compatibility
	RegisterDatabase("badgerdb", factory)
}

// New creates a new database with the provided configuration
func New(
	name string,
	dbPath string,
	readOnly bool,
	config []byte,
	gatherer interface{}, // Can be metric.Gatherer or metric.MultiGatherer
	logger log.Logger,
	metricsPrefix string,
	meterDBRegName string,
) (database.Database, error) {
	var db database.Database
	var err error

	// Try to create a metric.Metrics from the gatherer
	var metricsInstance metric.Metrics
	var registerer metric.Registerer

	// Check if it's already metric.Metrics
	if m, ok := gatherer.(metric.Metrics); ok {
		metricsInstance = m
	} else if reg, ok := gatherer.(metric.Registerer); ok {
		registerer = reg
	} else if multiGatherer, ok := gatherer.(interface {
		Register(string, metric.Gatherer) error
	}); ok {
		// Create a registry and register it with the MultiGatherer
		reg := metric.NewRegistry()
		if err := multiGatherer.Register(metricsPrefix, reg); err != nil {
			return nil, fmt.Errorf("couldn't register %q metrics: %w", metricsPrefix, err)
		}
		registerer = reg
	}

	// Handle memdb specially (no factory needed)
	if name == memdb.Name {
		db = memdb.New()
	} else {
		// Look up factory
		factoryMu.RLock()
		factory, ok := factories[name]
		factoryMu.RUnlock()

		if !ok {
			available := AvailableDatabases()
			return nil, fmt.Errorf("unknown database type: %s (available: %v)", name, available)
		}

		db, err = factory(dbPath, config, logger, registerer, metricsPrefix, readOnly)
		if err != nil {
			return nil, err
		}
	}

	// Wrap with versiondb if read-only (except memdb)
	if readOnly && name != memdb.Name {
		db = versiondb.New(db)
	}

	// Wrap with meterdb for metrics
	if metricsInstance != nil {
		meterDB, err := meterdb.New(metricsInstance, db)
		if err != nil {
			return nil, fmt.Errorf("failed to create meterdb: %w", err)
		}
		return startReplicationIfBase(meterDB, meterDBRegName), nil
	} else if registerer != nil {
		if reg, ok := registerer.(metric.Registry); ok {
			metricsInstance = metric.NewWithRegistry(metricsPrefix, reg)
		} else {
			metricsInstance = metric.New(metricsPrefix)
		}
		meterDB, err := meterdb.New(metricsInstance, db)
		if err != nil {
			return nil, fmt.Errorf("failed to create meterdb: %w", err)
		}
		return startReplicationIfBase(meterDB, meterDBRegName), nil
	}

	return startReplicationIfBase(db, meterDBRegName), nil
}
