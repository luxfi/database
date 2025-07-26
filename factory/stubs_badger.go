// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !badgerdb
// +build !badgerdb

package factory

import (
	"github.com/luxfi/db"
)

func newBadgerDB(config DatabaseConfig) (db.Database, error) {
	return nil, db.NewErrBackendDisabled("badgerdb")
}