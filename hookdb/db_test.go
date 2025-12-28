// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package hookdb

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/luxfi/database/memdb"
	"github.com/stretchr/testify/require"
)

func TestHookDB_Put(t *testing.T) {
	require := require.New(t)

	memDB := memdb.New()
	hookDB := New(memDB, DefaultConfig())
	defer hookDB.Close()

	var eventCount atomic.Int32
	var lastEvent Event

	hookDB.RegisterHandler(nil, func(e Event) {
		eventCount.Add(1)
		lastEvent = e
	})

	// Put a value
	err := hookDB.Put([]byte("key1"), []byte("value1"))
	require.NoError(err)

	// Wait for async processing
	time.Sleep(50 * time.Millisecond)

	require.Equal(int32(1), eventCount.Load())
	require.Equal(EventPut, lastEvent.Type)
	require.Equal([]byte("key1"), lastEvent.Key)
	require.Equal([]byte("value1"), lastEvent.Value)

	// Verify data was written
	val, err := hookDB.Get([]byte("key1"))
	require.NoError(err)
	require.Equal([]byte("value1"), val)
}

func TestHookDB_Delete(t *testing.T) {
	require := require.New(t)

	memDB := memdb.New()
	hookDB := New(memDB, DefaultConfig())
	defer hookDB.Close()

	var eventCount atomic.Int32

	hookDB.RegisterHandler(nil, func(e Event) {
		eventCount.Add(1)
	})

	// Put then delete
	err := hookDB.Put([]byte("key1"), []byte("value1"))
	require.NoError(err)

	err = hookDB.Delete([]byte("key1"))
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Equal(int32(2), eventCount.Load()) // Put + Delete
}

func TestHookDB_PrefixFilter(t *testing.T) {
	require := require.New(t)

	memDB := memdb.New()
	hookDB := New(memDB, DefaultConfig())
	defer hookDB.Close()

	var blockEvents atomic.Int32
	var txEvents atomic.Int32

	hookDB.RegisterHandler([]byte("block:"), func(e Event) {
		blockEvents.Add(1)
	})

	hookDB.RegisterHandler([]byte("tx:"), func(e Event) {
		txEvents.Add(1)
	})

	// Write with different prefixes
	hookDB.Put([]byte("block:123"), []byte("block data"))
	hookDB.Put([]byte("tx:456"), []byte("tx data"))
	hookDB.Put([]byte("other:789"), []byte("other data"))

	time.Sleep(50 * time.Millisecond)

	require.Equal(int32(1), blockEvents.Load())
	require.Equal(int32(1), txEvents.Load())
}

func TestHookDB_Batch(t *testing.T) {
	require := require.New(t)

	memDB := memdb.New()
	hookDB := New(memDB, DefaultConfig())
	defer hookDB.Close()

	var eventCount atomic.Int32

	hookDB.RegisterHandler(nil, func(e Event) {
		eventCount.Add(1)
	})

	// Create batch
	batch := hookDB.NewBatch()
	batch.Put([]byte("key1"), []byte("value1"))
	batch.Put([]byte("key2"), []byte("value2"))
	batch.Put([]byte("key3"), []byte("value3"))

	// Events shouldn't fire until Write
	time.Sleep(50 * time.Millisecond)
	require.Equal(int32(0), eventCount.Load())

	// Write batch
	err := batch.Write()
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)
	require.Equal(int32(3), eventCount.Load())
}

func TestHookDB_MultipleHandlers(t *testing.T) {
	require := require.New(t)

	memDB := memdb.New()
	hookDB := New(memDB, DefaultConfig())
	defer hookDB.Close()

	var handler1Count atomic.Int32
	var handler2Count atomic.Int32

	hookDB.RegisterHandler(nil, func(e Event) {
		handler1Count.Add(1)
	})

	hookDB.RegisterHandler(nil, func(e Event) {
		handler2Count.Add(1)
	})

	hookDB.Put([]byte("key"), []byte("value"))

	time.Sleep(50 * time.Millisecond)

	// Both handlers should receive the event
	require.Equal(int32(1), handler1Count.Load())
	require.Equal(int32(1), handler2Count.Load())
}

func BenchmarkHookDB_Put(b *testing.B) {
	memDB := memdb.New()
	hookDB := New(memDB, Config{BufferSize: 100000, Workers: 4})
	defer hookDB.Close()

	hookDB.RegisterHandler(nil, func(e Event) {
		// Simulate some processing
	})

	key := []byte("benchmark-key")
	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hookDB.Put(key, value)
	}
}
