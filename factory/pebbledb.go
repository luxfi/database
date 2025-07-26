// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build pebbledb
// +build pebbledb

package factory

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/pebbledb"
)

func newPebbleDB(config DatabaseConfig) (db.Database, error) {
	// Default cache sizes for pebbledb
	cache := 512 * 1024 * 1024 // 512 MB
	handles := 256
	readonly := false
	return pebbledb.New(config.Dir, cache, handles, config.Name, readonly)
}