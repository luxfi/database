// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build test

package metercacher

import "github.com/luxfi/metric"

import (
	"testing"

	"github.com/luxfi/metric"
	"github.com/stretchr/testify/require"

	"github.com/luxfi/ids"
	"github.com/luxfi/node/cache"
	"github.com/luxfi/node/cache/cachetest"
	"github.com/luxfi/node/cache/lru"
	"github.com/luxfi/ids"
)

func TestInterface(t *testing.T) {
	scenarios := []struct {
		name  string
		setup func(size int) cache.Cacher[ids.ID, int64]
	}{
		{
			name: "cache LRU",
			setup: func(size int) cache.Cacher[ids.ID, int64] {
				return lru.NewCache[ids.ID, int64](size)
			},
		},
		{
			name: "sized cache LRU",
			setup: func(size int) cache.Cacher[ids.ID, int64] {
				return lru.NewSizedCache(size*cachetest.IntSize, cachetest.IntSizeFunc)
			},
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			for _, test := range cachetest.Tests {
				baseCache := scenario.setup(test.Size)
				c, err := New("", metric.NewRegistry(), baseCache)
				require.NoError(t, err)
				test.Func(t, c)
			}
		})
	}
}
