// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package database

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

var _ HeightIndex = (*mockHeightIndex)(nil)

// mockHeightIndex implements HeightIndex for testing
type mockHeightIndex struct {
	data   map[uint64][]byte
	closed bool
}

func newMockHeightIndex() *mockHeightIndex {
	return &mockHeightIndex{
		data: make(map[uint64][]byte),
	}
}

func (m *mockHeightIndex) Put(height uint64, value []byte) error {
	if m.closed {
		return ErrClosed
	}
	// Clone the value to ensure callers can safely modify after Put
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)
	m.data[height] = valueCopy
	return nil
}

func (m *mockHeightIndex) Get(height uint64) ([]byte, error) {
	if m.closed {
		return nil, ErrClosed
	}
	value, exists := m.data[height]
	if !exists {
		return nil, ErrNotFound
	}
	return value, nil
}

func (m *mockHeightIndex) Has(height uint64) (bool, error) {
	if m.closed {
		return false, ErrClosed
	}
	_, exists := m.data[height]
	return exists, nil
}

func (m *mockHeightIndex) Delete(height uint64) error {
	if m.closed {
		return ErrClosed
	}
	delete(m.data, height)
	return nil
}

func (m *mockHeightIndex) Close() error {
	if m.closed {
		return ErrClosed
	}
	m.closed = true
	return nil
}

// TestHeightIndexPutGet tests basic Put and Get operations
func TestHeightIndexPutGet(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
	defer idx.Close()

	// Test Put and Get
	height := uint64(100)
	value := []byte("block_data")

	err := idx.Put(height, value)
	require.NoError(err)

	retrieved, err := idx.Get(height)
	require.NoError(err)
	require.Equal(value, retrieved)
}

// TestHeightIndexGetNotFound tests Get on non-existent height
func TestHeightIndexGetNotFound(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
	defer idx.Close()

	_, err := idx.Get(uint64(999))
	require.ErrorIs(err, ErrNotFound)
}

// TestHeightIndexHas tests Has operation
func TestHeightIndexHas(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
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

// TestHeightIndexDelete tests Delete operation
func TestHeightIndexDelete(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
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
	require.ErrorIs(err, ErrNotFound)
}

// TestHeightIndexMultipleHeights tests operations on multiple heights
func TestHeightIndexMultipleHeights(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
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

// TestHeightIndexPutOverwrite tests overwriting existing height
func TestHeightIndexPutOverwrite(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
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

// TestHeightIndexEmptyValue tests Put and Get with empty/nil values
func TestHeightIndexEmptyValue(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
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

// TestHeightIndexValueImmutability tests that values can be safely modified after Put
func TestHeightIndexValueImmutability(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
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

// TestHeightIndexClose tests Close operation
func TestHeightIndexClose(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()

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
	require.ErrorIs(err, ErrClosed)

	_, err = idx.Get(height)
	require.ErrorIs(err, ErrClosed)

	_, err = idx.Has(height)
	require.ErrorIs(err, ErrClosed)

	err = idx.Delete(height)
	require.ErrorIs(err, ErrClosed)

	// Double close should return error
	err = idx.Close()
	require.ErrorIs(err, ErrClosed)
}

// TestHeightIndexBoundaryValues tests boundary conditions for uint64 heights
func TestHeightIndexBoundaryValues(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
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

// TestHeightIndexDeleteNonExistent tests deleting non-existent height
func TestHeightIndexDeleteNonExistent(t *testing.T) {
	require := require.New(t)
	idx := newMockHeightIndex()
	defer idx.Close()

	// Delete should not error on non-existent height
	err := idx.Delete(uint64(999))
	require.NoError(err)
}

// BenchmarkHeightIndexPut benchmarks Put operations
func BenchmarkHeightIndexPut(b *testing.B) {
	idx := newMockHeightIndex()
	defer idx.Close()

	value := []byte("benchmark_data")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = idx.Put(uint64(i), value)
	}
}

// BenchmarkHeightIndexGet benchmarks Get operations
func BenchmarkHeightIndexGet(b *testing.B) {
	idx := newMockHeightIndex()
	defer idx.Close()

	value := []byte("benchmark_data")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		_ = idx.Put(uint64(i), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = idx.Get(uint64(i % 10000))
	}
}

// BenchmarkHeightIndexHas benchmarks Has operations
func BenchmarkHeightIndexHas(b *testing.B) {
	idx := newMockHeightIndex()
	defer idx.Close()

	value := []byte("benchmark_data")

	// Pre-populate
	for i := 0; i < 10000; i++ {
		_ = idx.Put(uint64(i), value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = idx.Has(uint64(i % 10000))
	}
}

// mockErrorHeightIndex implements HeightIndex that always returns errors
type mockErrorHeightIndex struct {
	err error
}

func (m *mockErrorHeightIndex) Put(height uint64, value []byte) error {
	return m.err
}

func (m *mockErrorHeightIndex) Get(height uint64) ([]byte, error) {
	return nil, m.err
}

func (m *mockErrorHeightIndex) Has(height uint64) (bool, error) {
	return false, m.err
}

func (m *mockErrorHeightIndex) Delete(height uint64) error {
	return m.err
}

func (m *mockErrorHeightIndex) Close() error {
	return m.err
}

// TestHeightIndexErrorHandling tests error propagation
func TestHeightIndexErrorHandling(t *testing.T) {
	require := require.New(t)

	customErr := errors.New("custom error")
	idx := &mockErrorHeightIndex{err: customErr}

	err := idx.Put(1, []byte("test"))
	require.ErrorIs(err, customErr)

	_, err = idx.Get(1)
	require.ErrorIs(err, customErr)

	_, err = idx.Has(1)
	require.ErrorIs(err, customErr)

	err = idx.Delete(1)
	require.ErrorIs(err, customErr)

	err = idx.Close()
	require.ErrorIs(err, customErr)
}
