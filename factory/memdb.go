// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/memdb"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func newMemDB(
	dbPath string,
	config []byte,
	logger *zap.Logger,
	registerer prometheus.Registerer,
	metricsPrefix string,
) (database.Database, error) {
	return memdb.New(), nil
}
