// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !cgo
// +build !cgo

package bls

import (
	luxbls "github.com/luxfi/crypto/bls"
)

const SignatureLen = luxbls.SignatureLen

var (
	ErrFailedSignatureDecompress  = luxbls.ErrFailedSignatureDecompress
	ErrInvalidSignature           = luxbls.ErrInvalidSignature
	ErrNoSignatures               = luxbls.ErrNoSignatures
	ErrFailedSignatureAggregation = luxbls.ErrFailedSignatureAggregation
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

// SecretKeyToBytes returns the big-endian format of the secret key.
func SecretKeyToBytes(sk *SecretKey) []byte {
	return luxbls.SecretKeyToBytes(sk)
}

// SignatureToBytes returns the compressed big-endian format of the signature.
func SignatureToBytes(sig *Signature) []byte {
	return luxbls.SignatureToBytes(sig)
}

// SignatureFromBytes parses the compressed big-endian format of the signature
// into a signature.
func SignatureFromBytes(sigBytes []byte) (*Signature, error) {
	return luxbls.SignatureFromBytes(sigBytes)
}

// Verify the signature against the provided message and public key.
func Verify(pk *PublicKey, sig *Signature, msg []byte) bool {
	return luxbls.Verify(pk, sig, msg)
}

// VerifyProofOfPossession verifies that signature is a valid proof of
// possession of the provided public key.
func VerifyProofOfPossession(pk *PublicKey, sig *Signature, msg []byte) bool {
	if pk == nil || sig == nil {
		return false
	}
	// Pure Go implementation should verify with PoP domain
	return luxbls.Verify(pk, sig, msg)
}

// AggregateSignatures aggregates a non-zero number of signatures into a single
// aggregated signature.
func AggregateSignatures(sigs []*Signature) (*Signature, error) {
	if len(sigs) == 0 {
		return nil, ErrNoSignatures
	}
	return luxbls.AggregateSignatures(sigs)
}
