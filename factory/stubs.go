// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !pebbledb
// +build !pebbledb

package factory

import (
	"github.com/luxfi/database"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

func newPebbleDB(
	dbPath string,
	config []byte,
	logger *zap.Logger,
	registerer prometheus.Registerer,
	metricsPrefix string,
) (database.Database, error) {
	return nil, database.NewErrBackendDisabled("pebbledb")
}
