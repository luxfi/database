// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build pebbledb

package manager

import (
	db "github.com/luxfi/database"
	"github.com/luxfi/database/pebbledb"
	"github.com/luxfi/metric"
)

func init() {
	RegisterDatabaseType("pebbledb", func(path string, config *Config, registerer metric.Registerer) (db.Database, error) {
		_ = registerer // metrics handled by meterdb wrapper in manager
		return pebbledb.New(path, config.CacheSize, config.HandleCap, config.Namespace, config.ReadOnly)
	})
}

// DefaultPebbleDBConfig returns a default PebbleDB configuration.
func DefaultPebbleDBConfig(path string) *Config {
	return &Config{
		Type:      "pebbledb",
		Path:      path,
		CacheSize: 16,
		HandleCap: 1024,
	}
}
