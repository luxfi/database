// Copyright (C) 2019-2025, Lux Partners Limited. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"fmt"

	"github.com/luxfi/db"
	"github.com/luxfi/db/badgerdb"
	"github.com/luxfi/db/leveldb"
	"github.com/luxfi/db/memdb"
	"github.com/luxfi/db/pebbledb"
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
		return leveldb.New(config.Dir, config.Config, config.Name, config.MetricsReg)
	case "pebbledb":
		return pebbledb.New(config.Dir, config.Config, config.Name, config.MetricsReg)
	case "badgerdb":
		return badgerdb.New(config.Dir, config.Config, config.Name, config.MetricsReg)
	case "memdb":
		return memdb.New(), nil
	default:
		return nil, fmt.Errorf("unknown database type: %s", config.Type)
	}
}