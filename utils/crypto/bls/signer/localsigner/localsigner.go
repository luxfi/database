// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build cgo
// +build cgo

package localsigner

import (
	"errors"

	"github.com/luxfi/node/utils/crypto/bls"
)

var (
	ErrFailedSecretKeyDeserialize            = errors.New("couldn't deserialize secret key")
	_                             bls.Signer = (*LocalSigner)(nil)
)

type LocalSigner struct {
	sk *bls.SecretKey
	pk *bls.PublicKey
}

// NewSecretKey generates a new secret key from the local source of
// cryptographically secure randomness.
func New() (*LocalSigner, error) {
	sk, err := bls.NewSecretKey()
	if err != nil {
		return nil, err
	}
	pk := sk.PublicKey()

	return &LocalSigner{sk: sk, pk: pk}, nil
}

// ToBytes returns the big-endian format of the secret key.
func (s *LocalSigner) ToBytes() []byte {
	return bls.SecretKeyToBytes(s.sk)
}

// FromBytes parses the big-endian format of the secret key into a
// secret key.
func FromBytes(skBytes []byte) (*LocalSigner, error) {
	sk, err := bls.SecretKeyFromBytes(skBytes)
	if err != nil {
		return nil, ErrFailedSecretKeyDeserialize
	}
	pk := sk.PublicKey()

	return &LocalSigner{sk: sk, pk: pk}, nil
}

// PublicKey returns the public key that corresponds to this secret
// key.
func (s *LocalSigner) PublicKey() *bls.PublicKey {
	return s.pk
}

// Sign [msg] to authorize this message
func (s *LocalSigner) Sign(msg []byte) (*bls.Signature, error) {
	return s.sk.Sign(msg)
}

// Sign [msg] to prove the ownership
func (s *LocalSigner) SignProofOfPossession(msg []byte) (*bls.Signature, error) {
	return s.sk.SignProofOfPossession(msg)
}
