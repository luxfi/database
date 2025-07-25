// Copyright (C) 2019-2025, Lux Partners Limited. All rights reserved.
// See the file LICENSE for licensing terms.

package memdb

import (
	"testing"

	database "github.com/luxfi/db"
	"github.com/stretchr/testify/require"
)

func TestMemDB(t *testing.T) {
	db := New()
	defer db.Close()

	// Test basic operations
	key := []byte("key")
	value := []byte("value")

	// Put
	err := db.Put(key, value)
	require.NoError(t, err)

	// Has
	exists, err := db.Has(key)
	require.NoError(t, err)
	require.True(t, exists)

	// Get
	got, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, value, got)

	// Delete
	err = db.Delete(key)
	require.NoError(t, err)

	exists, err = db.Has(key)
	require.NoError(t, err)
	require.False(t, exists)

	_, err = db.Get(key)
	require.ErrorIs(t, err, database.ErrNotFound)
}

func TestMemDBBatch(t *testing.T) {
	db := New()
	defer db.Close()

	batch := db.NewBatch()

	// Add operations to batch
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		err := batch.Put(key, value)
		require.NoError(t, err)
	}

	// Write batch
	err := batch.Write()
	require.NoError(t, err)

	// Verify all values were written
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		expected := []byte{byte(i * 2)}
		got, err := db.Get(key)
		require.NoError(t, err)
		require.Equal(t, expected, got)
	}
}

func TestMemDBIterator(t *testing.T) {
	db := New()
	defer db.Close()

	// Insert test data
	for i := 0; i < 10; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i * 2)}
		err := db.Put(key, value)
		require.NoError(t, err)
	}

	// Test iterator
	iter := db.NewIterator()
	defer iter.Release()

	count := 0
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		require.Equal(t, []byte{byte(count)}, key)
		require.Equal(t, []byte{byte(count * 2)}, value)
		count++
	}
	require.NoError(t, iter.Error())
	require.Equal(t, 10, count)
}