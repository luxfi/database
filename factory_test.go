// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package database_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/luxfi/database/dbtest"
	"github.com/luxfi/database/factory"
	"github.com/stretchr/testify/require"
)

// TestFactoryDatabases tests database implementations through the factory
func TestFactoryDatabases(t *testing.T) {
	// Create a temporary directory for test databases
	tmpDir, err := os.MkdirTemp("", "db-factory-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Test configurations for different database types
	configs := []factory.DatabaseConfig{
		{
			Name: "leveldb-test",
			Dir:  filepath.Join(tmpDir, "leveldb"),
			Type: "leveldb",
		},
		{
			Name: "memdb-test",
			Type: "memdb",
		},
		// These will only work with appropriate build tags
		{
			Name: "pebbledb-test",
			Dir:  filepath.Join(tmpDir, "pebbledb"),
			Type: "pebbledb",
		},
		{
			Name: "badgerdb-test",
			Dir:  filepath.Join(tmpDir, "badgerdb"),
			Type: "badgerdb",
		},
	}

	for _, config := range configs {
		t.Run(config.Type, func(t *testing.T) {
			// Try to create the database
			db, err := factory.NewFromConfig(config)

			// Check if this is an expected failure due to missing build tags
			if err != nil {
				if config.Type == "pebbledb" {
					require.Contains(t, err.Error(), "pebbledb support not compiled in")
					// Test passes - pebbledb correctly reports unavailability
					return
				}
				if config.Type == "badgerdb" {
					require.Contains(t, err.Error(), "badgerdb support not compiled in")
					// Test passes - badgerdb correctly reports unavailability
					return
				}
				// For other types, this is an unexpected error
				require.NoError(t, err)
			}

			// Database created successfully
			db.Close()

			// Run a subset of the standard tests
			t.Run("SimpleKeyValue", func(t *testing.T) {
				// Create a fresh database for this test
				testDb, err := factory.NewFromConfig(config)
				require.NoError(t, err)
				defer testDb.Close()
				dbtest.TestSimpleKeyValue(t, testDb)
			})

			t.Run("BatchPut", func(t *testing.T) {
				// Create a fresh database for this test
				testDb, err := factory.NewFromConfig(config)
				require.NoError(t, err)
				defer testDb.Close()
				dbtest.TestBatchPut(t, testDb)
			})

			t.Run("Iterator", func(t *testing.T) {
				// Create a fresh database for this test
				testDb, err := factory.NewFromConfig(config)
				require.NoError(t, err)
				defer testDb.Close()
				dbtest.TestIterator(t, testDb)
			})
		})
	}
}

// TestPebbleDBWithTag specifically tests pebbledb when compiled with the tag
// Run with: go test -tags=pebbledb -run TestPebbleDBWithTag
func TestPebbleDBWithTag(t *testing.T) {
	// Run all standard database tests
	for testName, test := range dbtest.Tests {
		t.Run(testName, func(t *testing.T) {
			// Create a fresh database for each test
			tmpDir, err := os.MkdirTemp("", "pebbledb-test-*")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			config := factory.DatabaseConfig{
				Name: "pebbledb-specific-test",
				Dir:  filepath.Join(tmpDir, "pebbledb"),
				Type: "pebbledb",
			}

			db, err := factory.NewFromConfig(config)
			if err != nil {
				// PebbleDB not available - test passes as unavailability is expected
				require.Contains(t, err.Error(), "pebbledb")
				return
			}
			defer db.Close()

			test(t, db)
		})
	}
}

// TestBadgerDBWithTag specifically tests badgerdb when compiled with the tag
// Run with: go test -tags=badgerdb -run TestBadgerDBWithTag
func TestBadgerDBWithTag(t *testing.T) {
	// Run all standard database tests
	for testName, test := range dbtest.Tests {
		t.Run(testName, func(t *testing.T) {
			// Create a fresh database for each test
			tmpDir, err := os.MkdirTemp("", "badgerdb-test-*")
			require.NoError(t, err)
			defer os.RemoveAll(tmpDir)

			config := factory.DatabaseConfig{
				Name: "badgerdb-specific-test",
				Dir:  filepath.Join(tmpDir, "badgerdb"),
				Type: "badgerdb",
			}

			db, err := factory.NewFromConfig(config)
			if err != nil {
				// BadgerDB not available - test passes as unavailability is expected
				require.Contains(t, err.Error(), "badgerdb")
				return
			}
			defer db.Close()

			test(t, db)
		})
	}
}
