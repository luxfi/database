// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package database_test

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/luxfi/database"
	"github.com/luxfi/database/dbtest"
	"github.com/luxfi/database/factory"
)

var backends = []struct {
	name   string
	engine string
	tagged bool // whether it requires a build tag
}{
	{"memdb", "memdb", false},
	{"leveldb", "leveldb", false},
	{"pebbledb", "pebbledb", true},
	{"badgerdb", "badgerdb", true},
}

func TestAllBackends(t *testing.T) {
	t.Parallel()

	for _, b := range backends {
		b := b // capture
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()

			if b.tagged && !backendEnabled(b.engine) {
				t.Skipf("%s disabled by build tags", b.engine)
			}

			// Run each test in dbtest.Tests with a fresh database
			for testName, testFunc := range dbtest.Tests {
				testName, testFunc := testName, testFunc // capture
				t.Run(testName, func(t *testing.T) {
					dir := t.TempDir()
					dbi, err := factory.New(factory.DatabaseConfig{
						Type:       b.engine,
						Dir:        filepath.Join(dir, b.name),
						Name:       b.name,
						MetricsReg: nil,
						Config:     nil,
					})
					if err != nil {
						t.Fatalf("create %s: %v", b.engine, err)
					}
					defer dbi.Close()

					testFunc(t, dbi)
				})
			}
		})
	}
}

// backendEnabled checks if a backend is available
func backendEnabled(engine string) bool {
	// Try to create a dummy database to check if backend is available
	dbi, err := factory.New(factory.DatabaseConfig{
		Type: engine,
		Dir:  "",
	})
	if dbi != nil {
		dbi.Close()
	}

	// Check if the error is ErrBackendDisabled
	var errBackend *database.ErrBackendDisabled
	return !errors.As(err, &errBackend)
}

// TestBenchmarks runs the benchmark suite against all backends
func TestBenchmarks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark tests in short mode")
	}

	// Note: Benchmarks should be run with go test -bench, not as regular tests
	// This test just verifies that benchmarks can be set up without errors
	for _, b := range backends {
		if b.tagged && !backendEnabled(b.engine) {
			continue
		}

		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			dbi, err := factory.New(factory.DatabaseConfig{
				Type: b.engine,
				Dir:  filepath.Join(dir, b.name),
				Name: b.name,
			})
			if err != nil {
				t.Fatalf("create %s: %v", b.engine, err)
			}
			defer dbi.Close()

			// Just verify we can set up benchmark data
			for _, size := range dbtest.BenchmarkSizes {
				count, keySize, valueSize := size[0], size[1], size[2]
				// Create a minimal benchmark just to test setup
				result := testing.Benchmark(func(b *testing.B) {
					b.StopTimer()
					keys, values := dbtest.SetupBenchmark(b, count, keySize, valueSize)
					b.StartTimer()
					// Run one iteration to verify it works
					for i := 0; i < b.N && i < 1; i++ {
						if err := dbi.Put(keys[0], values[0]); err != nil {
							b.Fatal(err)
						}
					}
				})
				if result.N == 0 {
					t.Errorf("benchmark setup failed for size %v", size)
				}
			}
		})
	}
}

// FuzzKeyValue runs fuzz tests against all backends
func FuzzKeyValue(f *testing.F) {
	// Use only the first available backend for fuzzing
	var selectedBackend *struct {
		name   string
		engine string
		tagged bool
	}
	
	for _, b := range backends {
		b := b // capture range variable
		if b.tagged && !backendEnabled(b.engine) {
			continue
		}
		selectedBackend = &b
		break
	}
	
	if selectedBackend == nil {
		f.Skip("No available backend for fuzzing")
	}

	dir := f.TempDir()
	dbi, err := factory.New(factory.DatabaseConfig{
		Type: selectedBackend.engine,
		Dir:  filepath.Join(dir, selectedBackend.name),
		Name: selectedBackend.name,
	})
	if err != nil {
		f.Fatalf("create %s: %v", selectedBackend.engine, err)
	}
	defer dbi.Close()

	dbtest.FuzzKeyValue(f, dbi)
}

// FuzzNewIteratorWithPrefix runs fuzz tests for iterator with prefix
func FuzzNewIteratorWithPrefix(f *testing.F) {
	// Use only the first available backend for fuzzing
	var selectedBackend *struct {
		name   string
		engine string
		tagged bool
	}
	
	for _, b := range backends {
		b := b // capture range variable
		if b.tagged && !backendEnabled(b.engine) {
			continue
		}
		selectedBackend = &b
		break
	}
	
	if selectedBackend == nil {
		f.Skip("No available backend for fuzzing")
	}

	dir := f.TempDir()
	dbi, err := factory.New(factory.DatabaseConfig{
		Type: selectedBackend.engine,
		Dir:  filepath.Join(dir, selectedBackend.name),
		Name: selectedBackend.name,
	})
	if err != nil {
		f.Fatalf("create %s: %v", selectedBackend.engine, err)
	}
	defer dbi.Close()

	dbtest.FuzzNewIteratorWithPrefix(f, dbi)
}

// FuzzNewIteratorWithStartAndPrefix runs fuzz tests for iterator with start and prefix
func FuzzNewIteratorWithStartAndPrefix(f *testing.F) {
	// Use only the first available backend for fuzzing
	var selectedBackend *struct {
		name   string
		engine string
		tagged bool
	}
	
	for _, b := range backends {
		b := b // capture range variable
		if b.tagged && !backendEnabled(b.engine) {
			continue
		}
		selectedBackend = &b
		break
	}
	
	if selectedBackend == nil {
		f.Skip("No available backend for fuzzing")
	}

	dir := f.TempDir()
	dbi, err := factory.New(factory.DatabaseConfig{
		Type: selectedBackend.engine,
		Dir:  filepath.Join(dir, selectedBackend.name),
		Name: selectedBackend.name,
	})
	if err != nil {
		f.Fatalf("create %s: %v", selectedBackend.engine, err)
	}
	defer dbi.Close()

	dbtest.FuzzNewIteratorWithStartAndPrefix(f, dbi)
}
