// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/pebbledb"
	"github.com/luxfi/log"
	"github.com/prometheus/client_golang/prometheus"
)

func newPebbleDB(
	dbPath string,
	config []byte,
	logger log.Logger,
	registerer prometheus.Registerer,
	metricsPrefix string,
	readOnly bool,
) (database.Database, error) {
	// Default cache sizes for pebbledb
	cache := 512 * 1024 * 1024 // 512 MB
	handles := 256
	return pebbledb.New(dbPath, cache, handles, "pebbledb", readOnly)
}
