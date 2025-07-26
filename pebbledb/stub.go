// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !pebbledb
// +build !pebbledb

package pebbledb

import (
	"github.com/luxfi/database"
)

// New returns a clear error when PebbleDB is disabled.
func New(_ string, _ int, _ int, _ string, _ bool) (database.Database, error) {
	return nil, database.NewErrBackendDisabled("pebbledb")
}
