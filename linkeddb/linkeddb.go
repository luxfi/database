// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package linkeddb

import (
	"slices"
	"sync"

	"github.com/luxfi/database"
	"github.com/luxfi/database/cache"
	"github.com/luxfi/database/cache/lru"
)

const (
	defaultCacheSize = 1024
)

var (
	headKey = []byte{0x01}

	_ LinkedDB      = (*linkedDB)(nil)
	_ database.Iterator   = (*iterator)(nil)
)

// LinkedDB provides a key value interface while allowing iteration.
type LinkedDB interface {
	database.KeyValueReaderWriterDeleter

	IsEmpty() (bool, error)
	HeadKey() ([]byte, error)
	Head() (key []byte, value []byte, err error)

	NewIterator() database.Iterator
	NewIteratorWithStart(start []byte) database.Iterator
}

type linkedDB struct {
	// lock ensure that this data structure handles its thread safety correctly.
	lock sync.RWMutex

	cacheLock sync.Mutex
	// these variables provide caching for the head key.
	headKeyIsSynced, headKeyExists, headKeyIsUpdated, updatedHeadKeyExists bool
	headKey, updatedHeadKey                                                []byte
	// these variables provide caching for the nodes.
	nodeCache    cache.Cacher[string, *node] // key -> *node
	updatedNodes map[string]*node

	// db is the underlying database that this list is stored in.
	db database.Database
	// batch writes to [db] atomically.
	batch database.Batch
}

type node struct {
	Value       []byte `serialize:"true"`
	HasNext     bool   `serialize:"true"`
	Next        []byte `serialize:"true"`
	HasPrevious bool   `serialize:"true"`
	Previous    []byte `serialize:"true"`
}

func New(database database.Database, cacheSize int) LinkedDB {
	return &linkedDB{
		nodeCache:    lru.NewCache[string, *node](cacheSize),
		updatedNodes: make(map[string]*node),
		db:           database,
		batch:        database.NewBatch(),
	}
}

func NewDefault(database database.Database) LinkedDB {
	return New(database, defaultCacheSize)
}

func (ldb *linkedDB) Has(key []byte) (bool, error) {
	ldatabase.lock.RLock()
	defer ldatabase.lock.RUnlock()

	return ldatabase.database.Has(nodeKey(key))
}

func (ldb *linkedDB) Get(key []byte) ([]byte, error) {
	ldatabase.lock.RLock()
	defer ldatabase.lock.RUnlock()

	node, err := ldatabase.getNode(key)
	return node.Value, err
}

func (ldb *linkedDB) Put(key, value []byte) error {
	ldatabase.lock.Lock()
	defer ldatabase.lock.Unlock()

	ldatabase.resetBatch()

	// If the key already has a node in the list, update that node.
	existingNode, err := ldatabase.getNode(key)
	if err == nil {
		existingNode.Value = slices.Clone(value)
		if err := ldatabase.putNode(key, existingNode); err != nil {
			return err
		}
		return ldatabase.writeBatch()
	}
	if err != database.ErrNotFound {
		return err
	}

	// The key isn't currently in the list, so we should add it as the head.
	// Note we will copy the key so it's safe to store references to it.
	key = slices.Clone(key)
	newHead := node{Value: slices.Clone(value)}
	if headKey, err := ldatabase.getHeadKey(); err == nil {
		// The list currently has a head, so we need to update the old head.
		oldHead, err := ldatabase.getNode(headKey)
		if err != nil {
			return err
		}
		oldHead.HasPrevious = true
		oldHead.Previous = key
		if err := ldatabase.putNode(headKey, oldHead); err != nil {
			return err
		}

		newHead.HasNext = true
		newHead.Next = headKey
	} else if err != database.ErrNotFound {
		return err
	}
	if err := ldatabase.putNode(key, newHead); err != nil {
		return err
	}
	if err := ldatabase.putHeadKey(key); err != nil {
		return err
	}
	return ldatabase.writeBatch()
}

func (ldb *linkedDB) Delete(key []byte) error {
	ldatabase.lock.Lock()
	defer ldatabase.lock.Unlock()

	currentNode, err := ldatabase.getNode(key)
	if err == database.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	ldatabase.resetBatch()

	// We're trying to delete this node.
	if err := ldatabase.deleteNode(key); err != nil {
		return err
	}

	switch {
	case currentNode.HasPrevious:
		// We aren't modifying the head.
		previousNode, err := ldatabase.getNode(currentNode.Previous)
		if err != nil {
			return err
		}
		previousNode.HasNext = currentNode.HasNext
		previousNode.Next = currentNode.Next
		if err := ldatabase.putNode(currentNode.Previous, previousNode); err != nil {
			return err
		}
		if currentNode.HasNext {
			// We aren't modifying the tail.
			nextNode, err := ldatabase.getNode(currentNode.Next)
			if err != nil {
				return err
			}
			nextNode.HasPrevious = true
			nextNode.Previous = currentNode.Previous
			if err := ldatabase.putNode(currentNode.Next, nextNode); err != nil {
				return err
			}
		}
	case !currentNode.HasNext:
		// This is the only node, so we don't have a head anymore.
		if err := ldatabase.deleteHeadKey(); err != nil {
			return err
		}
	default:
		// The next node will be the new head.
		if err := ldatabase.putHeadKey(currentNode.Next); err != nil {
			return err
		}
		nextNode, err := ldatabase.getNode(currentNode.Next)
		if err != nil {
			return err
		}
		nextNode.HasPrevious = false
		nextNode.Previous = nil
		if err := ldatabase.putNode(currentNode.Next, nextNode); err != nil {
			return err
		}
	}
	return ldatabase.writeBatch()
}

func (ldb *linkedDB) IsEmpty() (bool, error) {
	_, err := ldatabase.HeadKey()
	if err == database.ErrNotFound {
		return true, nil
	}
	return false, err
}

func (ldb *linkedDB) HeadKey() ([]byte, error) {
	ldatabase.lock.RLock()
	defer ldatabase.lock.RUnlock()

	return ldatabase.getHeadKey()
}

func (ldb *linkedDB) Head() ([]byte, []byte, error) {
	ldatabase.lock.RLock()
	defer ldatabase.lock.RUnlock()

	headKey, err := ldatabase.getHeadKey()
	if err != nil {
		return nil, nil, err
	}
	head, err := ldatabase.getNode(headKey)
	return headKey, head.Value, err
}

// This iterator does not guarantee that keys are returned in lexicographic
// order.
func (ldb *linkedDB) NewIterator() database.Iterator {
	return &iterator{ldb: ldb}
}

// NewIteratorWithStart returns an iterator that starts at [start].
// This iterator does not guarantee that keys are returned in lexicographic
// order.
// If [start] is not in the list, starts iterating from the list head.
func (ldb *linkedDB) NewIteratorWithStart(start []byte) database.Iterator {
	hasStartKey, err := ldatabase.Has(start)
	if err == nil && hasStartKey {
		return &iterator{
			ldb:         ldb,
			initialized: true,
			nextKey:     start,
		}
	}
	// If the start key isn't present, start from the head
	return ldatabase.NewIterator()
}

func (ldb *linkedDB) getHeadKey() ([]byte, error) {
	// If the ldb read lock is held, then there needs to be additional
	// synchronization here to avoid racy behavior.
	ldatabase.cacheLock.Lock()
	defer ldatabase.cacheLock.Unlock()

	if ldatabase.headKeyIsSynced {
		if ldatabase.headKeyExists {
			return ldatabase.headKey, nil
		}
		return nil, database.ErrNotFound
	}
	headKey, err := ldatabase.database.Get(headKey)
	if err == nil {
		ldatabase.headKeyIsSynced = true
		ldatabase.headKeyExists = true
		ldatabase.headKey = headKey
		return headKey, nil
	}
	if err == database.ErrNotFound {
		ldatabase.headKeyIsSynced = true
		ldatabase.headKeyExists = false
		return nil, database.ErrNotFound
	}
	return headKey, err
}

func (ldb *linkedDB) putHeadKey(key []byte) error {
	ldatabase.headKeyIsUpdated = true
	ldatabase.updatedHeadKeyExists = true
	ldatabase.updatedHeadKey = key
	return ldatabase.batch.Put(headKey, key)
}

func (ldb *linkedDB) deleteHeadKey() error {
	ldatabase.headKeyIsUpdated = true
	ldatabase.updatedHeadKeyExists = false
	return ldatabase.batch.Delete(headKey)
}

func (ldb *linkedDB) getNode(key []byte) (node, error) {
	// If the ldb read lock is held, then there needs to be additional
	// synchronization here to avoid racy behavior.
	ldatabase.cacheLock.Lock()
	defer ldatabase.cacheLock.Unlock()

	keyStr := string(key)
	if n, exists := ldatabase.nodeCache.Get(keyStr); exists {
		if n == nil {
			return node{}, database.ErrNotFound
		}
		return *n, nil
	}

	nodeBytes, err := ldatabase.database.Get(nodeKey(key))
	if err == database.ErrNotFound {
		ldatabase.nodeCache.Put(keyStr, nil)
		return node{}, err
	}
	if err != nil {
		return node{}, err
	}
	n := node{}
	_, err = Codec.Unmarshal(nodeBytes, &n)
	if err == nil {
		ldatabase.nodeCache.Put(keyStr, &n)
	}
	return n, err
}

func (ldb *linkedDB) putNode(key []byte, n node) error {
	ldatabase.updatedNodes[string(key)] = &n
	nodeBytes, err := Codec.Marshal(CodecVersion, n)
	if err != nil {
		return err
	}
	return ldatabase.batch.Put(nodeKey(key), nodeBytes)
}

func (ldb *linkedDB) deleteNode(key []byte) error {
	ldatabase.updatedNodes[string(key)] = nil
	return ldatabase.batch.Delete(nodeKey(key))
}

func (ldb *linkedDB) resetBatch() {
	ldatabase.headKeyIsUpdated = false
	clear(ldatabase.updatedNodes)
	ldatabase.batch.Reset()
}

func (ldb *linkedDB) writeBatch() error {
	if err := ldatabase.batch.Write(); err != nil {
		return err
	}
	if ldatabase.headKeyIsUpdated {
		ldatabase.headKeyIsSynced = true
		ldatabase.headKeyExists = ldatabase.updatedHeadKeyExists
		ldatabase.headKey = ldatabase.updatedHeadKey
	}
	for key, n := range ldatabase.updatedNodes {
		ldatabase.nodeCache.Put(key, n)
	}
	return nil
}

type iterator struct {
	ldb                    *linkedDB
	initialized, exhausted bool
	key, value, nextKey    []byte
	err                    error
}

func (it *iterator) Next() bool {
	// If the iterator has been exhausted, there is no next value.
	if it.exhausted {
		it.key = nil
		it.value = nil
		return false
	}

	it.ldatabase.lock.RLock()
	defer it.ldatabase.lock.RUnlock()

	// If the iterator was not yet initialized, do it now.
	if !it.initialized {
		it.initialized = true
		headKey, err := it.ldatabase.getHeadKey()
		if err == database.ErrNotFound {
			it.exhausted = true
			it.key = nil
			it.value = nil
			return false
		}
		if err != nil {
			it.exhausted = true
			it.key = nil
			it.value = nil
			it.err = err
			return false
		}
		it.nextKey = headKey
	}

	nextNode, err := it.ldatabase.getNode(it.nextKey)
	if err == database.ErrNotFound {
		it.exhausted = true
		it.key = nil
		it.value = nil
		return false
	}
	if err != nil {
		it.exhausted = true
		it.key = nil
		it.value = nil
		it.err = err
		return false
	}
	it.key = it.nextKey
	it.value = nextNode.Value
	it.nextKey = nextNode.Next
	it.exhausted = !nextNode.HasNext
	return true
}

func (it *iterator) Error() error {
	return it.err
}

func (it *iterator) Key() []byte {
	return it.key
}

func (it *iterator) Value() []byte {
	return it.value
}

func (*iterator) Release() {}

func nodeKey(key []byte) []byte {
	newKey := make([]byte, len(key)+1)
	copy(newKey[1:], key)
	return newKey
}
