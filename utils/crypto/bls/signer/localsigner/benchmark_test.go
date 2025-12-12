// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package localsigner

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/luxfi/node/utils"
	"github.com/luxfi/node/utils/crypto/bls/blstest"
)

func BenchmarkSign(b *testing.B) {
	signer := NewSigner(require.New(b))
	for _, messageSize := range blstest.BenchmarkSizes {
		b.Run(strconv.Itoa(messageSize), func(b *testing.B) {
			message := utils.RandomBytes(messageSize)

			b.ResetTimer()

			for n := 0; n < b.N; n++ {
				_, _ = signer.Sign(message)
			}
		})
	}
}
