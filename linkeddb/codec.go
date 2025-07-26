// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package linkeddb

import (
	"math"

	"github.com/luxfi/db/codec"
	"github.com/luxfi/db/codec/linear"
)

const CodecVersion = 0

var Codec codec.Manager

func init() {
	lc := linear.NewDefault()
	Codec = linear.NewManager(math.MaxInt32)

	if err := Codec.RegisterCodec(CodecVersion, lc); err != nil {
		panic(err)
	}
}