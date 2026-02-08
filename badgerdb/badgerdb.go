// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

// Package badgerdb provides backwards compatibility for code importing
// github.com/luxfi/database/badgerdb. The implementation has moved to
// github.com/luxfi/database/zapdb.
package badgerdb

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/zapdb"
	"github.com/luxfi/metric"
)

const Name = zapdb.Name

// New creates a new badgerdb-backed database.
// This is a compatibility wrapper around zapdb.New.
func New(file string, configBytes []byte, namespace string, metrics metric.Registerer) (database.Database, error) {
	return zapdb.New(file, configBytes, namespace, metrics)
}
