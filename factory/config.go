// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"github.com/luxfi/database"
	"github.com/luxfi/log"
	"github.com/prometheus/client_golang/prometheus"
)

// DatabaseConfig is a convenience struct for database configuration
type DatabaseConfig struct {
	Type           string
	Dir            string
	Name           string
	ReadOnly       bool
	Config         []byte
	MetricsReg     prometheus.Registerer
	Logger         log.Logger
	MetricsPrefix  string
	MeterDBRegName string
}

// NewFromConfig creates a new database from a DatabaseConfig
func NewFromConfig(cfg DatabaseConfig) (database.Database, error) {
	// Default logger if not provided
	if cfg.Logger == nil {
		cfg.Logger = log.NewNoOpLogger()
	}
	
	// Convert Registerer to interface{} for the factory
	var gatherer interface{}
	if cfg.MetricsReg != nil {
		gatherer = cfg.MetricsReg
	}
	
	return New(
		cfg.Type,
		cfg.Dir,
		cfg.ReadOnly,
		cfg.Config,
		gatherer,
		cfg.Logger,
		cfg.MetricsPrefix,
		cfg.MeterDBRegName,
	)
}