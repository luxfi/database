// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"fmt"
	"sync"

	"github.com/luxfi/database"
	"github.com/luxfi/database/badgerdb"
	"github.com/luxfi/database/memdb"
	"github.com/luxfi/database/meterdb"
	"github.com/luxfi/database/versiondb"
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
)

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
	names = append(names, badgerdb.Name, memdb.Name)
	for name := range factories {
		names = append(names, name)
	}
	return names
}

func init() {
	// badgerdb is always available (default)
	RegisterDatabase(badgerdb.Name, func(
		dbPath string,
		config []byte,
		logger log.Logger,
		registerer metric.Registerer,
		metricsPrefix string,
		readOnly bool,
	) (database.Database, error) {
		return badgerdb.New(dbPath, config, "badgerdb", registerer)
	})
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
		return meterDB, nil
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
		return meterDB, nil
	}

	return db, nil
}
