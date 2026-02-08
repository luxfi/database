// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package manager

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/zapdb"
	"github.com/luxfi/metric"
)

func newBadgerDB(path string, configBytes []byte, namespace string, metrics metric.Registerer) (database.Database, error) {
	return zapdb.New(path, configBytes, namespace, metrics)
}
