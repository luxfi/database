// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build pebbledb

package manager

import (
	db "github.com/luxfi/database"
	"github.com/luxfi/database/pebbledb"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterDatabaseType("pebbledb", func(path string, config *Config, registerer prometheus.Registerer) (db.Database, error) {
		return pebbledb.New(path, config.CacheSize, config.HandleCap, config.Namespace, config.ReadOnly)
	})
}

// DefaultPebbleDBConfig returns a default PebbleDB configuration.
func DefaultPebbleDBConfig(path string) *Config {
	return &Config{
		Type:      "pebbledb",
		Path:      path,
		CacheSize: 512,
		HandleCap: 1024,
	}
}
