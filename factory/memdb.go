// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"github.com/luxfi/db"
	"github.com/luxfi/db/memdb"
)

func newMemDB(config DatabaseConfig) (db.Database, error) {
	return memdb.New(), nil
}