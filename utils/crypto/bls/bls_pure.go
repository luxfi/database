// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !cgo
// +build !cgo

package bls

import (
	"errors"

	luxbls "github.com/luxfi/crypto/bls"
)

const PublicKeyLen = luxbls.PublicKeyLen

var (
	ErrNoPublicKeys               = luxbls.ErrNoPublicKeys
	ErrFailedPublicKeyDecompress  = luxbls.ErrFailedPublicKeyDecompress
	errInvalidPublicKey           = errors.New("invalid public key")
	errFailedPublicKeyAggregation = errors.New("couldn't aggregate public keys")
)

type PublicKey = luxbls.PublicKey

// PublicKeyToCompressedBytes returns the compressed big-endian format of the
// public key.
func PublicKeyToCompressedBytes(pk *PublicKey) []byte {
	return luxbls.PublicKeyToCompressedBytes(pk)
}

// PublicKeyFromCompressedBytes parses the compressed big-endian format of the
// public key into a public key.
func PublicKeyFromCompressedBytes(pkBytes []byte) (*PublicKey, error) {
	return luxbls.PublicKeyFromCompressedBytes(pkBytes)
}

// PublicKeyToUncompressedBytes returns the uncompressed big-endian format of
// the public key.
func PublicKeyToUncompressedBytes(key *PublicKey) []byte {
	return luxbls.PublicKeyToUncompressedBytes(key)
}

// PublicKeyFromUncompressedBytes parses the uncompressed big-endian format of
// the public key into a public key.
func PublicKeyFromUncompressedBytes(pkBytes []byte) (*PublicKey, error) {
	// luxfi/crypto handles uncompressed format
	pk := luxbls.PublicKeyFromValidUncompressedBytes(pkBytes)
	if pk == nil {
		return nil, errInvalidPublicKey
	}
	return pk, nil
}

// PublicKeyFromValidUncompressedBytes parses the uncompressed big-endian format of
// the public key into a public key without performing validation.
// This should only be used when the public key is known to be valid.
func PublicKeyFromValidUncompressedBytes(pkBytes []byte) *PublicKey {
	return luxbls.PublicKeyFromValidUncompressedBytes(pkBytes)
}

// AggregatePublicKeys aggregates a non-zero number of public keys into a
// single aggregated public key.
func AggregatePublicKeys(pks []*PublicKey) (*PublicKey, error) {
	if len(pks) == 0 {
		return nil, ErrNoPublicKeys
	}
	return luxbls.AggregatePublicKeys(pks)
}
