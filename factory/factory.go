// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"fmt"

	"github.com/luxfi/database"
	"github.com/luxfi/database/badgerdb"
	"github.com/luxfi/database/leveldb"
	"github.com/luxfi/database/memdb"
	"github.com/luxfi/database/meterdb"
	"github.com/luxfi/database/pebbledb"
	"github.com/luxfi/database/versiondb"
	"github.com/luxfi/log"
	"github.com/prometheus/client_golang/prometheus"
)

// New creates a new database with the provided configuration
func New(
	name string,
	dbPath string,
	readOnly bool,
	config []byte,
	gatherer interface{}, // Can be prometheus.Gatherer or metrics.MultiGatherer
	logger log.Logger,
	metricsPrefix string,
	meterDBRegName string,
) (database.Database, error) {
	var db database.Database
	var err error

	// Try to create a prometheus.Registerer from the gatherer
	var registerer prometheus.Registerer
	if reg, ok := gatherer.(prometheus.Registerer); ok {
		registerer = reg
	} else if multiGatherer, ok := gatherer.(interface {
		Register(string, prometheus.Gatherer) error
	}); ok {
		// Create a registry and register it with the MultiGatherer
		reg := prometheus.NewRegistry()
		if err := multiGatherer.Register(metricsPrefix, reg); err != nil {
			return nil, fmt.Errorf("couldn't register %q metrics: %w", metricsPrefix, err)
		}
		registerer = reg
	}

	switch name {
	case leveldb.Name:
		db, err = newLevelDB(dbPath, config, logger, registerer, metricsPrefix)
	case pebbledb.Name:
		db, err = newPebbleDB(dbPath, config, logger, registerer, metricsPrefix)
	case badgerdb.Name:
		db, err = newBadgerDB(dbPath, config, logger, registerer, metricsPrefix)
	case memdb.Name:
		db = memdb.New()
	default:
		return nil, fmt.Errorf("unknown database type: %s", name)
	}

	if err != nil {
		return nil, err
	}

	// TODO: Fix logger interface mismatch between luxfi/log and internal logging
	// For now, skip corruptabledb wrapper
	// log := logging.NewZapAdapter(logger)
	// db = corruptabledb.New(db, log)

	// Wrap with versiondb if read-only (except memdb)
	if readOnly && name != memdb.Name {
		db = versiondb.New(db)
	}

	// Wrap with meterdb for metrics if we have a registerer
	if registerer != nil {
		meterDB, err := meterdb.New(registerer, db)
		if err != nil {
			return nil, fmt.Errorf("failed to create meterdb: %w", err)
		}
		return meterDB, nil
	}

	return db, nil
}
