// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/badgerdb"
	"github.com/luxfi/log"
	"github.com/prometheus/client_golang/prometheus"
)

func newBadgerDB(
	dbPath string,
	config []byte,
	logger log.Logger,
	registerer prometheus.Registerer,
	metricsPrefix string,
) (database.Database, error) {
	return badgerdb.New(dbPath, config, "badgerdb", registerer)
}