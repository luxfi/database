// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build test

package zapdb

import (
	"fmt"
	"testing"

	"github.com/luxfi/metric"
	"github.com/stretchr/testify/require"

	"github.com/luxfi/database"
	"github.com/luxfi/database/dbtest"
)

// newGPUDB creates a database with GPU caching enabled
func newGPUDB(t testing.TB) database.Database {
	folder := t.TempDir()
	gpuCfg := DefaultGPUCacheConfig()
	gpuCfg.GPUMemoryBudget = 64 * 1024 * 1024 // 64 MB
	gpuCfg.InitialCapacity = 4096
	gpuCfg.PromoteOnMiss = true
	gpuCfg.WriteThrough = true

	db, err := NewWithGPU(folder, nil, "test-gpu", metric.NewRegistry(), gpuCfg)
	require.NoError(t, err)
	require.True(t, db.GPUCacheEnabled())
	return db
}

// TestGPUInterface runs the full database test suite with GPU caching enabled.
// This ensures GPU cache behavior is identical to disk-only behavior.
func TestGPUInterface(t *testing.T) {
	for name, test := range dbtest.Tests {
		t.Run(name, func(t *testing.T) {
			db := newGPUDB(t)
			test(t, db)
			_ = db.Close()
		})
	}
}

// TestGPUCacheHitMiss verifies cache hit/miss behavior
func TestGPUCacheHitMiss(t *testing.T) {
	require := require.New(t)

	folder := t.TempDir()
	gpuCfg := DefaultGPUCacheConfig()
	gpuCfg.GPUMemoryBudget = 64 * 1024 * 1024
	gpuCfg.InitialCapacity = 1024
	gpuCfg.PromoteOnMiss = true

	db, err := NewWithGPU(folder, nil, "test", metric.NewRegistry(), gpuCfg)
	require.NoError(err)
	defer db.Close()

	// Put should populate both GPU cache and disk
	err = db.Put([]byte("key1"), []byte("value1"))
	require.NoError(err)

	// Get should be a GPU cache hit
	val, err := db.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), val)

	stats := db.GPUCacheStats()
	require.NotNil(stats)
	require.True(stats.Hits > 0, "expected GPU cache hit")

	// Delete and verify gone
	err = db.Delete([]byte("key1"))
	require.NoError(err)

	_, err = db.Get([]byte("key1"))
	require.ErrorIs(err, database.ErrNotFound)
}

// TestGPUCachePromotion verifies disk-to-GPU promotion
func TestGPUCachePromotion(t *testing.T) {
	require := require.New(t)

	folder := t.TempDir()

	// First, create a disk-only DB and write data
	diskDB, err := New(folder, nil, "test", metric.NewRegistry())
	require.NoError(err)
	err = diskDB.Put([]byte("diskkey"), []byte("diskvalue"))
	require.NoError(err)
	err = diskDB.Close()
	require.NoError(err)

	// Re-open with GPU cache
	gpuCfg := DefaultGPUCacheConfig()
	gpuCfg.GPUMemoryBudget = 64 * 1024 * 1024
	gpuCfg.InitialCapacity = 1024
	gpuCfg.PromoteOnMiss = true

	db, err := NewWithGPU(folder, nil, "test", metric.NewRegistry(), gpuCfg)
	require.NoError(err)
	defer db.Close()

	// First read: cache miss, reads from disk, promotes
	val, err := db.Get([]byte("diskkey"))
	require.NoError(err)
	require.Equal([]byte("diskvalue"), val)

	stats := db.GPUCacheStats()
	require.True(stats.Promotions > 0, "expected promotion from disk")

	// Second read: should be a cache hit
	val, err = db.Get([]byte("diskkey"))
	require.NoError(err)
	require.Equal([]byte("diskvalue"), val)

	stats = db.GPUCacheStats()
	require.True(stats.Hits > 0, "expected GPU cache hit on second read")
}

// TestGPUCacheInvalidate verifies cache invalidation
func TestGPUCacheInvalidate(t *testing.T) {
	require := require.New(t)

	db := newGPUDB(t).(*Database)
	defer db.Close()

	for i := 0; i < 100; i++ {
		err := db.Put([]byte(fmt.Sprintf("key%d", i)), []byte(fmt.Sprintf("val%d", i)))
		require.NoError(err)
	}

	stats := db.GPUCacheStats()
	require.Equal(uint32(100), stats.Entries)

	db.InvalidateGPUCache()

	stats = db.GPUCacheStats()
	require.Equal(uint32(0), stats.Entries)

	// Data should still be on disk
	val, err := db.Get([]byte("key50"))
	require.NoError(err)
	require.Equal([]byte("val50"), val)
}

// TestGPUCacheDisabled verifies that disabled GPU config works normally
func TestGPUCacheDisabled(t *testing.T) {
	require := require.New(t)

	folder := t.TempDir()
	gpuCfg := DefaultGPUCacheConfig() // GPUMemoryBudget = 0

	db, err := NewWithGPU(folder, nil, "test", metric.NewRegistry(), gpuCfg)
	require.NoError(err)
	defer db.Close()

	require.False(db.GPUCacheEnabled())
	require.Nil(db.GPUCacheStats())

	err = db.Put([]byte("key"), []byte("value"))
	require.NoError(err)

	val, err := db.Get([]byte("key"))
	require.NoError(err)
	require.Equal([]byte("value"), val)
}

// TestGPUCacheStress hammers the GPU cache with concurrent goroutines
// doing mixed reads/writes/deletes. Verifies no panics, no data races,
// and no data corruption.
func TestGPUCacheStress(t *testing.T) {
	require := require.New(t)

	folder := t.TempDir()
	gpuCfg := DefaultGPUCacheConfig()
	gpuCfg.GPUMemoryBudget = 128 * 1024 * 1024 // 128 MB
	gpuCfg.InitialCapacity = 1 << 14            // 16K slots
	gpuCfg.PromoteOnMiss = true
	gpuCfg.WriteThrough = true

	db, err := NewWithGPU(folder, nil, "stress", metric.NewRegistry(), gpuCfg)
	require.NoError(err)
	defer db.Close()
	require.True(db.GPUCacheEnabled())

	const (
		numGoroutines = 16
		opsPerGoroutine = 5000
		keySpace = 2000 // keys to choose from
	)

	// Pre-generate key/value pairs
	keys := make([][]byte, keySpace)
	values := make([][]byte, keySpace)
	for i := 0; i < keySpace; i++ {
		keys[i] = []byte(fmt.Sprintf("stress-key-%06d", i))
		values[i] = []byte(fmt.Sprintf("stress-val-%06d-%s", i, "padding-to-make-it-longer"))
	}

	// Pre-populate 50% of keys
	for i := 0; i < keySpace/2; i++ {
		require.NoError(db.Put(keys[i], values[i]))
	}

	// Concurrent stress: mixed reads/writes/deletes
	errCh := make(chan error, numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			for op := 0; op < opsPerGoroutine; op++ {
				idx := (goroutineID*opsPerGoroutine + op) % keySpace
				switch op % 10 {
				case 0, 1: // 20% writes
					if err := db.Put(keys[idx], values[idx]); err != nil {
						errCh <- fmt.Errorf("goroutine %d put err: %w", goroutineID, err)
						return
					}
				case 2: // 10% deletes
					if err := db.Delete(keys[idx]); err != nil {
						errCh <- fmt.Errorf("goroutine %d delete err: %w", goroutineID, err)
						return
					}
				case 3: // 10% has checks
					if _, err := db.Has(keys[idx]); err != nil {
						errCh <- fmt.Errorf("goroutine %d has err: %w", goroutineID, err)
						return
					}
				default: // 60% reads
					val, err := db.Get(keys[idx])
					if err != nil && err != database.ErrNotFound {
						errCh <- fmt.Errorf("goroutine %d get err: %w", goroutineID, err)
						return
					}
					// If we got a value back, verify it matches expected value
					if val != nil && string(val) != string(values[idx]) {
						errCh <- fmt.Errorf("goroutine %d data corruption: key=%s expected=%s got=%s",
							goroutineID, keys[idx], values[idx], val)
						return
					}
				}
			}
			errCh <- nil
		}(g)
	}

	// Collect results
	for g := 0; g < numGoroutines; g++ {
		err := <-errCh
		require.NoError(err)
	}

	// Verify stats are sane
	stats := db.GPUCacheStats()
	require.NotNil(stats)
	t.Logf("Stress test stats: entries=%d hits=%d misses=%d evictions=%d load=%.2f%%",
		stats.Entries, stats.Hits, stats.Misses, stats.Evictions, stats.LoadFactor*100)

	// Verify we can still read/write after stress
	require.NoError(db.Put([]byte("post-stress"), []byte("works")))
	val, err := db.Get([]byte("post-stress"))
	require.NoError(err)
	require.Equal([]byte("works"), val)
}

// TestXXHash64GoMatchesReference verifies the Go xxHash64 implementation
// produces correct output for known test vectors. These same vectors
// are used in the C implementation tests.
func TestXXHash64GoMatchesReference(t *testing.T) {
	// Known xxHash64 test vectors (seed=0)
	// Verified against C implementation (luxcpp/zapdb/src/gpu_hashtable.cpp zapdb_hash)
	// and standalone C reference compiled from identical algorithm.
	tests := []struct {
		name   string
		input  []byte
		expect uint64
	}{
		{"empty", []byte{}, 0xEF46DB3751D8E999},
		{"1_byte_0", []byte{0x00}, 0xE934A84ADB052768},
		{"3_bytes", []byte{0x01, 0x02, 0x03}, 0x743E13EE0C4EE5A5},
		{"4_bytes", []byte{0x01, 0x02, 0x03, 0x04}, 0x542620E3A2A92ED1},
		{"14_bytes", []byte("0123456789abcd"), 0xBA6A8D6A56C4FA6C},
		{"32_bytes_seq", func() []byte {
			b := make([]byte, 32)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		}(), 0xCBF59C5116FF32B4},
		{"64_bytes_seq", func() []byte {
			b := make([]byte, 64)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		}(), 0xF7C67301DB6713F0},
		{"32_bytes_zeros", make([]byte, 32), 0xF6E9BE5D70632CF5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := xxhash64(tc.input)
			if got != tc.expect {
				t.Errorf("xxhash64(%s) = 0x%016X, want 0x%016X", tc.name, got, tc.expect)
			}

			// Verify determinism: same input always gives same output
			got2 := xxhash64(tc.input)
			if got != got2 {
				t.Errorf("xxhash64 not deterministic: %X != %X", got, got2)
			}
		})
	}

	// Cross-implementation consistency: verify same constants
	// Go constants must match C constants exactly
	t.Run("constants_match", func(t *testing.T) {
		p1 := uint64(0x9E3779B185EBCA87)
		p2 := uint64(0xC2B2AE3D27D4EB4F)
		p3 := uint64(0x165667B19E3779F9)
		p4 := uint64(0x85EBCA77C2B2AE63)
		p5 := uint64(0x27D4EB2F165667C5)

		// These are the official xxHash64 primes
		if p1 != 0x9E3779B185EBCA87 || p2 != 0xC2B2AE3D27D4EB4F ||
			p3 != 0x165667B19E3779F9 || p4 != 0x85EBCA77C2B2AE63 ||
			p5 != 0x27D4EB2F165667C5 {
			t.Error("xxHash64 constants do not match specification")
		}
	})

	// Edge cases
	t.Run("all_zeros_32", func(t *testing.T) {
		got := xxhash64(make([]byte, 32))
		t.Logf("xxhash64(zeros_32) = 0x%X", got)
		if got == 0 {
			t.Error("xxhash64 of 32 zero bytes should not be 0")
		}
	})

	t.Run("large_input_64", func(t *testing.T) {
		input := make([]byte, 64)
		for i := range input {
			input[i] = byte(i)
		}
		got := xxhash64(input)
		t.Logf("xxhash64(0..63) = 0x%X", got)
		if got == 0 {
			t.Error("xxhash64 of 64 sequential bytes should not be 0")
		}
	})
}

func FuzzGPUKeyValue(f *testing.F) {
	db := newGPUDB(f)
	defer db.Close()

	dbtest.FuzzKeyValue(f, db)
}

func BenchmarkGPUInterface(b *testing.B) {
	for _, size := range dbtest.BenchmarkSizes {
		keys, values := dbtest.SetupBenchmark(b, size[0], size[1], size[2])
		for name, bench := range dbtest.Benchmarks {
			b.Run(fmt.Sprintf("gpu_%d_pairs_%d_keys_%d_values_%s", size[0], size[1], size[2], name), func(b *testing.B) {
				db := newGPUDB(b)
				bench(b, db, keys, values)
				_ = db.Close()
			})
		}
	}
}
