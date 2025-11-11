// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package memdb

import (
	"testing"

	"github.com/luxfi/database"
	"github.com/luxfi/database/heightindexdb/dbtest"
)

// TestDatabase runs the complete test suite against memdb implementation
func TestDatabase(t *testing.T) {
	dbtest.TestSuite(t, func() database.HeightIndex {
		return New()
	})
}

// BenchmarkPut benchmarks Put operations
func BenchmarkPut(b *testing.B) {
	db := New()
	defer db.Close()

	value := []byte("benchmark_data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Put(uint64(i), value)
	}
}

// BenchmarkGet benchmarks Get operations
func BenchmarkGet(b *testing.B) {
	db := New()
	defer db.Close()

	value := []byte("benchmark_data")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		_ = db.Put(uint64(i), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Get(uint64(i % 10000))
	}
}

// BenchmarkHas benchmarks Has operations
func BenchmarkHas(b *testing.B) {
	db := New()
	defer db.Close()

	value := []byte("benchmark_data")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		_ = db.Put(uint64(i), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = db.Has(uint64(i % 10000))
	}
}

// BenchmarkDelete benchmarks Delete operations
func BenchmarkDelete(b *testing.B) {
	db := New()
	defer db.Close()

	value := []byte("benchmark_data")

	// Pre-populate
	for i := 0; i < b.N; i++ {
		_ = db.Put(uint64(i), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = db.Delete(uint64(i))
	}
}

// BenchmarkConcurrentReadWrite benchmarks concurrent operations
func BenchmarkConcurrentReadWrite(b *testing.B) {
	db := New()
	defer db.Close()

	value := []byte("benchmark_data")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		_ = db.Put(uint64(i), value)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			if i%2 == 0 {
				_ = db.Put(i%10000, value)
			} else {
				_, _ = db.Get(i % 10000)
			}
			i++
		}
	})
}
