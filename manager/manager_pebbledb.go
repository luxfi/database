// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build pebbledb
// +build pebbledb

package manager

import (
	"github.com/luxfi/db"
	"github.com/luxfi/db/pebbledb"
)

func newPebbleDB(path string, cacheSize, handleCap int, namespace string, readOnly bool) (db.Database, error) {
	return pebbledb.New(path, cacheSize, handleCap, namespace, readOnly)
}