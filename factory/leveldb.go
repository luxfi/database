// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/leveldb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func newLevelDB(
	dbPath string,
	config []byte,
	logger *zap.Logger,
	registerer prometheus.Registerer,
	metricsPrefix string,
) (database.Database, error) {
	// Default cache sizes for leveldb
	blockCacheSize := 12 * 1024 * 1024 // 12 MB
	writeCacheSize := 4 * 1024 * 1024  // 4 MB
	handleCap := 1024
	return leveldb.New(dbPath, blockCacheSize, writeCacheSize, handleCap)
}
