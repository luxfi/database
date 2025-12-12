// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package lru

import (
	"testing"

	"github.com/luxfi/node/cache/cachetest"
	"github.com/luxfi/ids"
)

func TestCache(t *testing.T) {
	c := NewCache[ids.ID, int64](1)
	cachetest.Basic(t, c)
}

func TestCacheEviction(t *testing.T) {
	c := NewCache[ids.ID, int64](2)
	cachetest.Eviction(t, c)
}
