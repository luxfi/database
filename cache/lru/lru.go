// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package lru

import (
	"container/list"
	"sync"

	"github.com/luxfi/database/cache"
)

// Cache implements an LRU cache
type Cache[K comparable, V any] struct {
	lock     sync.Mutex
	capacity int
	list     *list.List
	elements map[K]*list.Element
}

type entry[K comparable, V any] struct {
	key   K
	value V
}

// NewCache creates a new LRU cache with the given capacity
func NewCache[K comparable, V any](capacity int) cache.Cacher[K, V] {
	return &Cache[K, V]{
		capacity: capacity,
		list:     list.New(),
		elements: make(map[K]*list.Element),
	}
}

// Put adds a key-value pair to the cache
func (c *Cache[K, V]) Put(key K, value V) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, ok := c.elements[key]; ok {
		c.list.MoveToFront(elem)
		elem.Value.(*entry[K, V]).value = value
		return
	}

	elem := c.list.PushFront(&entry[K, V]{key: key, value: value})
	c.elements[key] = elem

	if c.list.Len() > c.capacity {
		c.removeOldest()
	}
}

// Get retrieves a value from the cache
func (c *Cache[K, V]) Get(key K) (V, bool) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, ok := c.elements[key]; ok {
		c.list.MoveToFront(elem)
		return elem.Value.(*entry[K, V]).value, true
	}

	var zero V
	return zero, false
}

// Evict removes a key from the cache
func (c *Cache[K, V]) Evict(key K) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if elem, ok := c.elements[key]; ok {
		c.removeElement(elem)
	}
}

// Flush clears the cache
func (c *Cache[K, V]) Flush() {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.list.Init()
	c.elements = make(map[K]*list.Element)
}

// Len returns the number of items in the cache
func (c *Cache[K, V]) Len() int {
	c.lock.Lock()
	defer c.lock.Unlock()

	return c.list.Len()
}

func (c *Cache[K, V]) removeOldest() {
	elem := c.list.Back()
	if elem != nil {
		c.removeElement(elem)
	}
}

func (c *Cache[K, V]) removeElement(elem *list.Element) {
	c.list.Remove(elem)
	entry := elem.Value.(*entry[K, V])
	delete(c.elements, entry.key)
}