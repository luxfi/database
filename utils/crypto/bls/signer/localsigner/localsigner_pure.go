// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !cgo
// +build !cgo

package localsigner

import (
	luxsigner "github.com/luxfi/crypto/bls/signer/localsigner"

	"github.com/luxfi/node/utils/crypto/bls"
)

var (
	ErrFailedSecretKeyDeserialize            = luxsigner.ErrFailedSecretKeyDeserialize
	_                             bls.Signer = (*LocalSigner)(nil)
)

// LocalSigner wraps the Lux crypto localsigner
type LocalSigner struct {
	signer luxsigner.LocalSigner
}

// New generates a new signer with a random secret key
func New() (*LocalSigner, error) {
	signer, err := luxsigner.New()
	if err != nil {
		return nil, err
	}
	return &LocalSigner{signer: *signer}, nil
}

// FromBytes parses the big-endian format of the secret key into a LocalSigner
func FromBytes(skBytes []byte) (*LocalSigner, error) {
	signer, err := luxsigner.FromBytes(skBytes)
	if err != nil {
		return nil, err
	}
	return &LocalSigner{signer: *signer}, nil
}

// ToBytes returns the big-endian format of the secret key
func (s *LocalSigner) ToBytes() []byte {
	return s.signer.ToBytes()
}

// PublicKey returns the public key associated with this signer
func (s *LocalSigner) PublicKey() *bls.PublicKey {
	pk := s.signer.PublicKey()
	return (*bls.PublicKey)(pk)
}

// Sign creates a signature from the provided message
func (s *LocalSigner) Sign(msg []byte) (*bls.Signature, error) {
	sig, err := s.signer.Sign(msg)
	if err != nil {
		return nil, err
	}
	return (*bls.Signature)(sig), nil
}

// SignProofOfPossession creates a proof of possession signature
func (s *LocalSigner) SignProofOfPossession(msg []byte) (*bls.Signature, error) {
	sig, err := s.signer.SignProofOfPossession(msg)
	if err != nil {
		return nil, err
	}
	return (*bls.Signature)(sig), nil
}
