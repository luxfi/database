// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"fmt"

	"github.com/luxfi/database"
	"github.com/prometheus/client_golang/prometheus"
)

// DatabaseConfig contains all the parameters necessary to create a database
type DatabaseConfig struct {
	Name        string
	Dir         string
	Config      []byte
	Type        string // "leveldb", "pebbledb", "badgerdb", "memdb"
	MetricsReg  prometheus.Registerer
}

// New creates a new database based on the config
func New(config DatabaseConfig) (db.Database, error) {
	switch config.Type {
	case "leveldb":
		return newLevelDB(config)
	case "pebbledb":
		return newPebbleDB(config)
	case "badgerdb":
		return newBadgerDB(config)
	case "memdb":
		return newMemDB(config)
	default:
		return nil, fmt.Errorf("unknown database type: %s", config.Type)
	}
}