// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build cgo
// +build cgo

package bls

import (
	"errors"

	luxbls "github.com/luxfi/crypto/bls"
)

const SignatureLen = luxbls.SignatureLen

var (
	ErrNoSignatures               = luxbls.ErrNoSignatures
	ErrFailedSignatureDecompress  = luxbls.ErrFailedSignatureDecompress
	errInvalidSignature           = errors.New("invalid signature")
	errFailedSignatureAggregation = errors.New("couldn't aggregate signatures")
	errInvalidSignatureLen        = errors.New("invalid signature length")
)

type (
	SecretKey = luxbls.SecretKey
	Signature = luxbls.Signature
	Signer    = luxbls.Signer
)

// NewSecretKey generates a new secret key
func NewSecretKey() (*SecretKey, error) {
	return luxbls.NewSecretKey()
}

// SecretKeyFromBytes parses a secret key from bytes
func SecretKeyFromBytes(skBytes []byte) (*SecretKey, error) {
	return luxbls.SecretKeyFromBytes(skBytes)
}

// SignatureToBytes returns the compressed big-endian format of the signature.
func SignatureToBytes(sig *Signature) []byte {
	return luxbls.SignatureToBytes(sig)
}

// SignatureFromBytes parses the compressed big-endian format of the signature into a signature.
func SignatureFromBytes(sigBytes []byte) (*Signature, error) {
	return luxbls.SignatureFromBytes(sigBytes)
}

// SecretKeyToBytes returns the big-endian format of the secret key.
func SecretKeyToBytes(sk *SecretKey) []byte {
	return luxbls.SecretKeyToBytes(sk)
}

// Verify verifies that the signature was signed from the given public key on the given message.
func Verify(pk *PublicKey, sig *Signature, msg []byte) bool {
	if pk == nil || sig == nil {
		return false
	}
	return luxbls.Verify(pk, sig, msg)
}

// VerifyProofOfPossession verifies that the signature was signed from the
// given public key on the given message using the proof of possession domain
// separation tag.
func VerifyProofOfPossession(pk *PublicKey, sig *Signature, msg []byte) bool {
	if pk == nil || sig == nil {
		return false
	}
	// luxfi/crypto should handle PoP verification with CGO optimization
	return luxbls.Verify(pk, sig, msg)
}

// AggregateSignatures aggregates a non-zero number of signatures into a single aggregated signature.
func AggregateSignatures(sigs []*Signature) (*Signature, error) {
	if len(sigs) == 0 {
		return nil, ErrNoSignatures
	}
	// luxfi/crypto should use optimized CGO implementation when available
	return luxbls.AggregateSignatures(sigs)
}
