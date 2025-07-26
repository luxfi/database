// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !badgerdb
// +build !badgerdb

package manager

import (
	"errors"

	"github.com/luxfi/database"
	"github.com/prometheus/client_golang/prometheus"
)

var errBadgerDBNotSupported = errors.New("badgerdb not supported in this build")

func newBadgerDB(path string, configBytes []byte, namespace string, metrics prometheus.Registerer) (db.Database, error) {
	return nil, errBadgerDBNotSupported
}