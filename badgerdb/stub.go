// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !badgerdb
// +build !badgerdb

package badgerdb

import (
	"github.com/luxfi/database"
	"github.com/prometheus/client_golang/prometheus"
)

// New returns a clear error when BadgerDB is disabled.
func New(_ string, _ []byte, _ string, _ prometheus.Registerer) (db.Database, error) {
	return nil, db.NewErrBackendDisabled("badgerdb")
}