// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build leveldb

package manager

import (
	db "github.com/luxfi/database"
	"github.com/luxfi/database/leveldb"
	"github.com/prometheus/client_golang/prometheus"
)

func init() {
	RegisterDatabaseType("leveldb", func(path string, config *Config, registerer prometheus.Registerer) (db.Database, error) {
		return leveldb.New(path, config.CacheSize, config.CacheSize/2, config.HandleCap)
	})
}

// DefaultLevelDBConfig returns a default LevelDB configuration.
func DefaultLevelDBConfig(path string) *Config {
	return &Config{
		Type:      "leveldb",
		Path:      path,
		CacheSize: 16,
		HandleCap: 1024,
	}
}
