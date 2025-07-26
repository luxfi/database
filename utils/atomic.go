// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import "sync/atomic"

// Atomic provides atomic operations for a value of type T
type Atomic[T any] struct {
	v atomic.Value
}

// Get returns the value
func (a *Atomic[T]) Get() T {
	v := a.v.Load()
	if v == nil {
		var zero T
		return zero
	}
	return v.(T)
}

// Set sets the value
func (a *Atomic[T]) Set(v T) {
	a.v.Store(v)
}