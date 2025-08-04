// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package manager

import (
	db "github.com/luxfi/database"
	"github.com/luxfi/database/pebbledb"
)

func newPebbleDB(path string, cacheSize, handleCap int, namespace string, readOnly bool) (db.Database, error) {
	return pebbledb.New(path, cacheSize, handleCap, namespace, readOnly)
}
