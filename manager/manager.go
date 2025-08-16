// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package manager

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	db "github.com/luxfi/database"
	"github.com/luxfi/database/leveldb"
	"github.com/luxfi/database/memdb"
	"github.com/luxfi/database/meterdb"
	"github.com/luxfi/database/prefixdb"
	"github.com/luxfi/database/versiondb"
	"github.com/luxfi/metric"
	"github.com/prometheus/client_golang/prometheus"
)

// Manager is a database manager that can create new database instances.
type Manager struct {
	baseDir    string
	registerer prometheus.Registerer
}

// NewManager creates a new database manager.
func NewManager(baseDir string, registerer prometheus.Registerer) *Manager {
	return &Manager{
		baseDir:    baseDir,
		registerer: registerer,
	}
}

// Config defines the database configuration.
type Config struct {
	// Type is the database type ("leveldb", "pebbledb", "memdb")
	Type string

	// Path is the database path (relative to base directory)
	Path string

	// Namespace is used for metrics
	Namespace string

	// CacheSize is the size of the cache in MB
	CacheSize int

	// HandleCap is the maximum number of open file handles
	HandleCap int

	// EnableMetrics enables prometheus metrics
	EnableMetrics bool

	// EnableVersioning enables database versioning
	EnableVersioning bool

	// Prefix adds a prefix to all keys
	Prefix []byte

	// ReadOnly opens the database in read-only mode
	ReadOnly bool
}

// New creates a new database instance.
func (m *Manager) New(config *Config) (db.Database, error) {
	if config == nil {
		return nil, errors.New("config is required")
	}

	// Create base database
	var database db.Database
	var err error

	switch config.Type {
	case "memdb", "memory":
		database = memdb.New()

	case "leveldb":
		path := filepath.Join(m.baseDir, config.Path)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
		database, err = leveldb.New(path, config.CacheSize, config.CacheSize/2, config.HandleCap)
		if err != nil {
			return nil, fmt.Errorf("failed to create leveldb: %w", err)
		}

	case "pebbledb":
		path := filepath.Join(m.baseDir, config.Path)
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
		database, err = newPebbleDB(path, config.CacheSize, config.HandleCap, config.Namespace, config.ReadOnly)
		if err != nil {
			return nil, fmt.Errorf("failed to create pebbledb: %w", err)
		}

	case "badgerdb":
		path := filepath.Join(m.baseDir, config.Path)
		if err := os.MkdirAll(path, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
		database, err = newBadgerDB(path, nil, config.Namespace, m.registerer)
		if err != nil {
			return nil, fmt.Errorf("failed to create badgerdb: %w", err)
		}

	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.Type)
	}

	// Add prefix wrapper if needed
	if len(config.Prefix) > 0 {
		database = prefixdb.New(config.Prefix, database)
	}

	// Add versioning wrapper if needed
	if config.EnableVersioning {
		database = versiondb.New(database)
	}

	// Add metrics wrapper if needed
	if config.EnableMetrics && m.registerer != nil {
		// Create metrics instance using metric package
		metricsInstance := metric.New(config.Namespace)
		database, err = meterdb.New(metricsInstance, database)
		if err != nil {
			return nil, fmt.Errorf("failed to create meter database: %w", err)
		}
	}

	return database, nil
}

// DefaultLevelDBConfig returns a default LevelDB configuration.
func DefaultLevelDBConfig(path string) *Config {
	return &Config{
		Type:      "leveldb",
		Path:      path,
		CacheSize: 16,
		HandleCap: 1024,
	}
}

// DefaultPebbleDBConfig returns a default PebbleDB configuration.
func DefaultPebbleDBConfig(path string) *Config {
	return &Config{
		Type:      "pebbledb",
		Path:      path,
		CacheSize: 512,
		HandleCap: 1024,
	}
}

// DefaultBadgerDBConfig returns a default BadgerDB configuration.
func DefaultBadgerDBConfig(path string) *Config {
	return &Config{
		Type:      "badgerdb",
		Path:      path,
		CacheSize: 512,
		HandleCap: 1024,
	}
}

// DefaultMemoryConfig returns a default in-memory database configuration.
func DefaultMemoryConfig() *Config {
	return &Config{
		Type: "memdb",
	}
}
