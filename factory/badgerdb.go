// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/badgerdb"
)

func newBadgerDB(config DatabaseConfig) (database.Database, error) {
	return badgerdb.New(config.Dir, config.Config, config.Name, config.MetricsReg)
}