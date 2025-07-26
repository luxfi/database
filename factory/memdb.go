// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	db "github.com/luxfi/database"
	"github.com/luxfi/database/memdb"
)

func newMemDB(config DatabaseConfig) (db.Database, error) {
	return memdb.New(), nil
}
