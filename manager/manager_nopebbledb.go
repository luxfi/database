// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !pebbledb
// +build !pebbledb

package manager

import (
	"errors"

	db "github.com/luxfi/database"
)

var errPebbleDBNotSupported = errors.New("pebbledb not supported in this build")

func newPebbleDB(path string, cacheSize, handleCap int, namespace string, readOnly bool) (db.Database, error) {
	return nil, errPebbleDBNotSupported
}
