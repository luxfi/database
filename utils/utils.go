// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

// Zero returns the zero value of type T
func Zero[T any]() T {
	var zero T
	return zero
}

// Sortable is the interface required by slices.SortFunc
type Sortable[T any] interface {
	Compare(T) int
}

// Constants for common byte sizes
const (
	KiB = 1024       // 1 KiB
	MiB = 1024 * KiB // 1 MiB
	GiB = 1024 * MiB // 1 GiB
)
