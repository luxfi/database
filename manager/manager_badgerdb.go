// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build badgerdb
// +build badgerdb

package manager

import (
	"github.com/luxfi/database"
	"github.com/luxfi/database/badgerdb"
	"github.com/prometheus/client_golang/prometheus"
)

func newBadgerDB(path string, configBytes []byte, namespace string, metrics prometheus.Registerer) (db.Database, error) {
	return badgerdb.New(path, configBytes, namespace, metrics)
}