// Copyright (C) 2019-2025, Lux Partners Limited. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !pebbledb
// +build !pebbledb

package manager

import (
	"errors"

	"github.com/luxfi/db"
)

var errPebbleDBNotSupported = errors.New("pebbledb not supported in this build")

func newPebbleDB(path string, cacheSize, handleCap int, namespace string, readOnly bool) (db.Database, error) {
	return nil, errPebbleDBNotSupported
}