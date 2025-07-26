// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package set

// Set is a generic set implementation
type Set[T comparable] map[T]struct{}

// NewSet creates a new set with optional initial capacity
func NewSet[T comparable](capacity int) Set[T] {
	return make(Set[T], capacity)
}

// Add adds an element to the set
func (s Set[T]) Add(elts ...T) {
	for _, elt := range elts {
		s[elt] = struct{}{}
	}
}

// Remove removes an element from the set
func (s Set[T]) Remove(elt T) {
	delete(s, elt)
}

// Contains returns true if the element is in the set
func (s Set[T]) Contains(elt T) bool {
	_, ok := s[elt]
	return ok
}

// Len returns the number of elements in the set
func (s Set[T]) Len() int {
	return len(s)
}

// Clear removes all elements from the set
func (s Set[T]) Clear() {
	for k := range s {
		delete(s, k)
	}
}