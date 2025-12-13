// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package dbtest

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/luxfi/database"
)

// TestSuite runs a comprehensive test suite against any HeightIndex implementation.
// This allows all HeightIndex implementations to be tested consistently.
func TestSuite(t *testing.T, newDB func() database.HeightIndex) {
	t.Run("PutGet", func(t *testing.T) {
		testPutGet(t, newDB())
	})
	t.Run("GetNotFound", func(t *testing.T) {
		testGetNotFound(t, newDB())
	})
	t.Run("Has", func(t *testing.T) {
		testHas(t, newDB())
	})
	t.Run("Delete", func(t *testing.T) {
		testDelete(t, newDB())
	})
	t.Run("MultipleHeights", func(t *testing.T) {
		testMultipleHeights(t, newDB())
	})
	t.Run("PutOverwrite", func(t *testing.T) {
		testPutOverwrite(t, newDB())
	})
	t.Run("EmptyValue", func(t *testing.T) {
		testEmptyValue(t, newDB())
	})
	t.Run("ValueImmutability", func(t *testing.T) {
		testValueImmutability(t, newDB())
	})
	t.Run("Close", func(t *testing.T) {
		testClose(t, newDB())
	})
	t.Run("BoundaryValues", func(t *testing.T) {
		testBoundaryValues(t, newDB())
	})
	t.Run("DeleteNonExistent", func(t *testing.T) {
		testDeleteNonExistent(t, newDB())
	})
	t.Run("ConcurrentAccess", func(t *testing.T) {
		testConcurrentAccess(t, newDB())
	})
}

// testPutGet tests basic Put and Get operations
func testPutGet(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	height := uint64(100)
	value := []byte("block_data")

	err := idx.Put(height, value)
	require.NoError(err)

	retrieved, err := idx.Get(height)
	require.NoError(err)
	require.Equal(value, retrieved)
}

// testGetNotFound tests Get on non-existent height
func testGetNotFound(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	_, err := idx.Get(uint64(999))
	require.ErrorIs(err, database.ErrNotFound)
}

// testHas tests Has operation
func testHas(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	height := uint64(100)
	value := []byte("block_data")

	// Should not exist initially
	exists, err := idx.Has(height)
	require.NoError(err)
	require.False(exists)

	// Should exist after Put
	err = idx.Put(height, value)
	require.NoError(err)

	exists, err = idx.Has(height)
	require.NoError(err)
	require.True(exists)
}

// testDelete tests Delete operation
func testDelete(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	height := uint64(100)
	value := []byte("block_data")

	// Put value
	err := idx.Put(height, value)
	require.NoError(err)

	// Verify it exists
	exists, err := idx.Has(height)
	require.NoError(err)
	require.True(exists)

	// Delete it
	err = idx.Delete(height)
	require.NoError(err)

	// Verify it no longer exists
	exists, err = idx.Has(height)
	require.NoError(err)
	require.False(exists)

	// Get should return ErrNotFound
	_, err = idx.Get(height)
	require.ErrorIs(err, database.ErrNotFound)
}

// testMultipleHeights tests operations on multiple heights
func testMultipleHeights(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	// Put multiple heights
	heights := []uint64{0, 1, 100, 1000, 10000}
	for _, h := range heights {
		value := make([]byte, 8)
		binary.BigEndian.PutUint64(value, h)
		err := idx.Put(h, value)
		require.NoError(err)
	}

	// Verify all heights exist
	for _, h := range heights {
		exists, err := idx.Has(h)
		require.NoError(err)
		require.True(exists)

		value, err := idx.Get(h)
		require.NoError(err)
		require.Len(value, 8)
		require.Equal(h, binary.BigEndian.Uint64(value))
	}
}

// testPutOverwrite tests overwriting existing height
func testPutOverwrite(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	height := uint64(100)
	value1 := []byte("first_value")
	value2 := []byte("second_value")

	// Put first value
	err := idx.Put(height, value1)
	require.NoError(err)

	retrieved, err := idx.Get(height)
	require.NoError(err)
	require.Equal(value1, retrieved)

	// Overwrite with second value
	err = idx.Put(height, value2)
	require.NoError(err)

	retrieved, err = idx.Get(height)
	require.NoError(err)
	require.Equal(value2, retrieved)
}

// testEmptyValue tests Put and Get with empty/nil values
func testEmptyValue(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	height := uint64(100)

	// Test with nil value
	err := idx.Put(height, nil)
	require.NoError(err)

	retrieved, err := idx.Get(height)
	require.NoError(err)
	require.Empty(retrieved)

	// Test with empty slice
	err = idx.Put(height+1, []byte{})
	require.NoError(err)

	retrieved, err = idx.Get(height + 1)
	require.NoError(err)
	require.Empty(retrieved)
}

// testValueImmutability tests that values can be safely modified after Put
func testValueImmutability(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	height := uint64(100)
	value := []byte("block_data")

	// Put value
	err := idx.Put(height, value)
	require.NoError(err)

	// Modify the original value
	value[0] = 'X'

	// Retrieved value should be unchanged
	retrieved, err := idx.Get(height)
	require.NoError(err)
	require.Equal([]byte("block_data"), retrieved)
	require.NotEqual(value, retrieved)
}

// testClose tests Close operation
func testClose(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)

	height := uint64(100)
	value := []byte("block_data")

	// Operations should work before Close
	err := idx.Put(height, value)
	require.NoError(err)

	// Close the index
	err = idx.Close()
	require.NoError(err)

	// Operations should fail after Close
	err = idx.Put(height+1, value)
	require.ErrorIs(err, database.ErrClosed)

	_, err = idx.Get(height)
	require.ErrorIs(err, database.ErrClosed)

	_, err = idx.Has(height)
	require.ErrorIs(err, database.ErrClosed)

	err = idx.Delete(height)
	require.ErrorIs(err, database.ErrClosed)

	// Double close should return error
	err = idx.Close()
	require.ErrorIs(err, database.ErrClosed)
}

// testBoundaryValues tests boundary conditions for uint64 heights
func testBoundaryValues(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	// Test boundary values
	boundaries := []uint64{
		0,              // min
		1,              // min + 1
		^uint64(0) - 1, // max - 1
		^uint64(0),     // max
	}

	for _, height := range boundaries {
		value := []byte("boundary_test")
		err := idx.Put(height, value)
		require.NoError(err)

		retrieved, err := idx.Get(height)
		require.NoError(err)
		require.Equal(value, retrieved)
	}
}

// testDeleteNonExistent tests deleting non-existent height
func testDeleteNonExistent(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	// Delete should not error on non-existent height
	err := idx.Delete(uint64(999))
	require.NoError(err)
}

// testConcurrentAccess tests concurrent Put/Get operations
func testConcurrentAccess(t *testing.T, idx database.HeightIndex) {
	require := require.New(t)
	defer idx.Close()

	const (
		numGoroutines = 10
		numOperations = 100
	)

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2) // writers + readers

	// Start writers
	for g := 0; g < numGoroutines; g++ {
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < numOperations; i++ {
				height := uint64(gID*numOperations + i)
				value := make([]byte, 8)
				binary.BigEndian.PutUint64(value, height)
				err := idx.Put(height, value)
				require.NoError(err)
			}
		}(g)
	}

	// Start readers
	for g := 0; g < numGoroutines; g++ {
		go func(gID int) {
			defer wg.Done()
			for i := 0; i < numOperations; i++ {
				height := uint64(gID*numOperations + i)
				// May or may not exist depending on timing
				_, err := idx.Get(height)
				if err != nil {
					require.True(errors.Is(err, database.ErrNotFound))
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify all written values can be read
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < numOperations; i++ {
			height := uint64(g*numOperations + i)
			value, err := idx.Get(height)
			require.NoError(err)
			require.Equal(height, binary.BigEndian.Uint64(value))
		}
	}
}
