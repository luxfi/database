// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build test

package zapdb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/luxfi/database"
	"github.com/luxfi/metric"
	"github.com/stretchr/testify/require"
)

// Benchmark sizes: count, keySize, valueSize
var benchSizes = []struct {
	name    string
	count   int
	keySize int
	valSize int
}{
	{"32B_keys", 4096, 32, 32},
	{"256B_keys", 4096, 256, 256},
	{"2KB_keys", 1024, 2048, 2048},
}

func setupKeys(b *testing.B, count, keySize, valSize int) ([][]byte, [][]byte) {
	b.Helper()
	keys := make([][]byte, count)
	values := make([][]byte, count)
	for i := range keys {
		keys[i] = make([]byte, keySize)
		values[i] = make([]byte, valSize)
		rand.Read(keys[i])
		rand.Read(values[i])
	}
	return keys, values
}

func newDiskDB(b *testing.B) *Database {
	b.Helper()
	db, err := New(b.TempDir(), nil, "bench", metric.NewRegistry())
	require.NoError(b, err)
	return db
}

func newGPUDB256MB(b *testing.B) *Database {
	b.Helper()
	cfg := DefaultGPUCacheConfig()
	cfg.GPUMemoryBudget = 256 * 1024 * 1024 // 256 MB
	cfg.InitialCapacity = 1 << 16            // 64K slots
	cfg.PromoteOnMiss = true
	cfg.WriteThrough = true
	db, err := NewWithGPU(b.TempDir(), nil, "bench-gpu", metric.NewRegistry(), cfg)
	require.NoError(b, err)
	return db
}

// =============================================================================
// Get Benchmarks — cache hot reads
// =============================================================================

func BenchmarkGet_Disk(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newDiskDB(b)
			defer db.Close()
			for i, k := range keys {
				db.Put(k, values[i])
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.Get(keys[i%sz.count])
			}
		})
	}
}

func BenchmarkGet_GPU(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newGPUDB256MB(b)
			defer db.Close()
			for i, k := range keys {
				db.Put(k, values[i])
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.Get(keys[i%sz.count])
			}
		})
	}
}

// =============================================================================
// Put Benchmarks — write-through performance
// =============================================================================

func BenchmarkPut_Disk(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newDiskDB(b)
			defer db.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.Put(keys[i%sz.count], values[i%sz.count])
			}
		})
	}
}

func BenchmarkPut_GPU(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newGPUDB256MB(b)
			defer db.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.Put(keys[i%sz.count], values[i%sz.count])
			}
		})
	}
}

// =============================================================================
// Has Benchmarks — existence check
// =============================================================================

func BenchmarkHas_Disk(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newDiskDB(b)
			defer db.Close()
			for i, k := range keys {
				db.Put(k, values[i])
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.Has(keys[i%sz.count])
			}
		})
	}
}

func BenchmarkHas_GPU(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newGPUDB256MB(b)
			defer db.Close()
			for i, k := range keys {
				db.Put(k, values[i])
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				db.Has(keys[i%sz.count])
			}
		})
	}
}

// =============================================================================
// Batch Write Benchmarks
// =============================================================================

func BenchmarkBatchWrite_Disk(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newDiskDB(b)
			defer db.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatch()
				for j := 0; j < sz.count; j++ {
					batch.Put(keys[j], values[j])
				}
				batch.Write()
			}
		})
	}
}

func BenchmarkBatchWrite_GPU(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newGPUDB256MB(b)
			defer db.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := db.NewBatch()
				for j := 0; j < sz.count; j++ {
					batch.Put(keys[j], values[j])
				}
				batch.Write()
			}
		})
	}
}

// =============================================================================
// Parallel Get Benchmarks — concurrent reads, EVM-like workload
// =============================================================================

func BenchmarkParallelGet_Disk(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newDiskDB(b)
			defer db.Close()
			for i, k := range keys {
				db.Put(k, values[i])
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for i := 0; pb.Next(); i++ {
					db.Get(keys[i%sz.count])
				}
			})
		})
	}
}

func BenchmarkParallelGet_GPU(b *testing.B) {
	for _, sz := range benchSizes {
		keys, values := setupKeys(b, sz.count, sz.keySize, sz.valSize)
		b.Run(sz.name, func(b *testing.B) {
			db := newGPUDB256MB(b)
			defer db.Close()
			for i, k := range keys {
				db.Put(k, values[i])
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for i := 0; pb.Next(); i++ {
					db.Get(keys[i%sz.count])
				}
			})
		})
	}
}

// =============================================================================
// Mixed Read/Write — simulates EVM state access patterns
// =============================================================================

func BenchmarkMixedRW_Disk(b *testing.B) {
	keys, values := setupKeys(b, 4096, 32, 256)
	db := newDiskDB(b)
	defer db.Close()

	// Pre-populate 80% of keys
	for i := 0; i < 3276; i++ {
		db.Put(keys[i], values[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 4096
		if idx%5 == 0 {
			// 20% writes
			db.Put(keys[idx], values[idx])
		} else {
			// 80% reads
			db.Get(keys[idx])
		}
	}
}

func BenchmarkMixedRW_GPU(b *testing.B) {
	keys, values := setupKeys(b, 4096, 32, 256)
	db := newGPUDB256MB(b)
	defer db.Close()

	// Pre-populate 80% of keys
	for i := 0; i < 3276; i++ {
		db.Put(keys[i], values[i])
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 4096
		if idx%5 == 0 {
			// 20% writes
			db.Put(keys[idx], values[idx])
		} else {
			// 80% reads
			db.Get(keys[idx])
		}
	}
}

// =============================================================================
// EVM State Simulation — 32-byte keys (account hashes), 256-byte values
// =============================================================================

func BenchmarkEVMStateGet_Disk(b *testing.B) {
	db := newDiskDB(b)
	defer db.Close()
	benchEVMStateGet(b, db)
}

func BenchmarkEVMStateGet_GPU(b *testing.B) {
	db := newGPUDB256MB(b)
	defer db.Close()
	benchEVMStateGet(b, db)
}

func benchEVMStateGet(b *testing.B, db database.Database) {
	// Simulate EVM state: 32-byte Keccak keys, ~100-300 byte account/storage values
	const numAccounts = 10000
	keys := make([][]byte, numAccounts)
	values := make([][]byte, numAccounts)

	for i := range keys {
		keys[i] = make([]byte, 32) // Keccak256 hash
		rand.Read(keys[i])
		// Simulate variable-size state values (RLP-encoded accounts, storage)
		valSize := 64 + rand.Intn(256) // 64-320 bytes
		values[i] = make([]byte, valSize)
		rand.Read(values[i])
	}

	// Populate
	for i, k := range keys {
		require.NoError(b, db.Put(k, values[i]))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(keys[i%numAccounts])
		if err != nil && err != database.ErrNotFound {
			b.Fatal(err)
		}
	}
}

func BenchmarkEVMParallelState_Disk(b *testing.B) {
	db := newDiskDB(b)
	defer db.Close()
	benchEVMParallelState(b, db)
}

func BenchmarkEVMParallelState_GPU(b *testing.B) {
	db := newGPUDB256MB(b)
	defer db.Close()
	benchEVMParallelState(b, db)
}

func benchEVMParallelState(b *testing.B, db database.Database) {
	const numAccounts = 10000
	keys := make([][]byte, numAccounts)
	values := make([][]byte, numAccounts)

	for i := range keys {
		keys[i] = make([]byte, 32)
		rand.Read(keys[i])
		valSize := 64 + rand.Intn(256)
		values[i] = make([]byte, valSize)
		rand.Read(values[i])
	}

	for i, k := range keys {
		require.NoError(b, db.Put(k, values[i]))
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for i := 0; pb.Next(); i++ {
			idx := i % numAccounts
			if i%10 == 0 {
				// 10% writes (storage updates)
				db.Put(keys[idx], values[idx])
			} else {
				// 90% reads (state lookups)
				db.Get(keys[idx])
			}
		}
	})
}

// Print stats after EVM simulation
func BenchmarkEVMStats(b *testing.B) {
	db := newGPUDB256MB(b)
	defer db.Close()

	const numAccounts = 10000
	keys := make([][]byte, numAccounts)
	values := make([][]byte, numAccounts)
	for i := range keys {
		keys[i] = make([]byte, 32)
		rand.Read(keys[i])
		values[i] = make([]byte, 128+rand.Intn(128))
		rand.Read(values[i])
	}

	for i, k := range keys {
		db.Put(k, values[i])
	}

	// Read them all back
	for _, k := range keys {
		db.Get(k)
	}

	stats := db.GPUCacheStats()
	if stats != nil {
		b.ReportMetric(float64(stats.Hits), "gpu_hits")
		b.ReportMetric(float64(stats.Misses), "gpu_misses")
		b.ReportMetric(float64(stats.Entries), "gpu_entries")
		b.ReportMetric(float64(stats.MemoryUsed)/1024/1024, "gpu_MB_used")
		b.ReportMetric(float64(stats.LoadFactor*100), "load_pct")
		fmt.Printf("\n  GPU Cache Stats: entries=%d hits=%d misses=%d promotions=%d load=%.1f%% mem=%.1fMB\n",
			stats.Entries, stats.Hits, stats.Misses, stats.Promotions,
			stats.LoadFactor*100, float64(stats.MemoryUsed)/1024/1024)
	}
}
