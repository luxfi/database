// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package cache

// Cacher defines the interface for cache implementations
type Cacher[K comparable, V any] interface {
	Put(key K, value V)
	Get(key K) (value V, found bool)
	Evict(key K)
	Flush()
	Len() int
}
