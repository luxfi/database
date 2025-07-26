// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"crypto/rand"
)

// RandomBytes returns a slice of n random bytes
func RandomBytes(n int) []byte {
	bytes := make([]byte, n)
	// #nosec G404 - we don't need cryptographic randomness for test data
	_, _ = rand.Read(bytes)
	return bytes
}