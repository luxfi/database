// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build pebbledb

package factory

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/pebbledb"
	log "github.com/luxfi/log"
	"github.com/luxfi/metric"
)

func init() {
	RegisterDatabase(pebbledb.Name, newPebbleDB)
}

func newPebbleDB(
	dbPath string,
	config []byte,
	logger log.Logger,
	registerer metric.Registerer,
	metricsPrefix string,
	readOnly bool,
) (database.Database, error) {
	const cacheSize = 12 * 1024 * 1024 // 12 MB block cache
	const handleCap = 1024
	return pebbledb.New(dbPath, cacheSize, handleCap, pebbledb.Name, readOnly)
}
