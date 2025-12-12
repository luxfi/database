// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package keychain

import (
	"errors"

	"github.com/luxfi/ids"
	"github.com/luxfi/math/set"
)

var (
	ErrInvalidIndicesLength    = errors.New("number of indices should be greater than 0")
	ErrInvalidNumAddrsToDerive = errors.New("number of addresses to derive should be greater than 0")
	ErrInvalidNumAddrsDerived  = errors.New("incorrect number of ledger derived addresses")
	ErrInvalidNumSignatures    = errors.New("incorrect number of signatures")
)

// Signer implements functions for a keychain to return its main address and
// to sign a hash
type Signer interface {
	SignHash([]byte) ([]byte, error)
	Sign([]byte) ([]byte, error)
	Address() ids.ShortID
}

// Keychain maintains a set of addresses together with their corresponding
// signers
type Keychain interface {
	// The returned Signer can provide a signature for [addr]
	Get(addr ids.ShortID) (Signer, bool)
	// Returns the set of addresses for which the accessor keeps an associated
	// signer
	Addresses() set.Set[ids.ShortID]
}

// Ledger interface for hardware wallet support
type Ledger interface {
	Address(displayHRP string, addressIndex uint32) (ids.ShortID, error)
	SignHash(hash []byte, addressIndex uint32) ([]byte, error)
	Sign(hash []byte, addressIndex uint32) ([]byte, error)
	SignTransaction(rawUnsignedHash []byte, addressIndices []uint32) ([][]byte, error)
	GetAddresses(addressIndices []uint32) ([]ids.ShortID, error)
	Disconnect() error
}

// ledgerKeychain is an abstraction of the underlying ledger hardware device,
// to be able to get a signer from a finite set of derived signers
type ledgerKeychain struct {
	ledger    Ledger
	addrs     set.Set[ids.ShortID]
	addrToIdx map[ids.ShortID]uint32
}

// ledgerSigner is an abstraction of the underlying ledger hardware device,
// capable of extracting its main address and signing a hash
type ledgerSigner struct {
	ledger Ledger
	idx    uint32
	addr   ids.ShortID
}

// NewLedgerKeychain creates a new ledger keychain
func NewLedgerKeychain(ledger Ledger, indices []uint32) (Keychain, error) {
	if len(indices) == 0 {
		return nil, ErrInvalidIndicesLength
	}

	addresses, err := ledger.GetAddresses(indices)
	if err != nil {
		return nil, err
	}

	if len(addresses) != len(indices) {
		return nil, ErrInvalidNumAddrsDerived
	}

	addrToIdx := make(map[ids.ShortID]uint32)
	addrs := set.Set[ids.ShortID]{}
	for i, addr := range addresses {
		addrToIdx[addr] = indices[i]
		addrs.Add(addr)
	}

	return &ledgerKeychain{
		ledger:    ledger,
		addrs:     addrs,
		addrToIdx: addrToIdx,
	}, nil
}

func (l *ledgerKeychain) Get(addr ids.ShortID) (Signer, bool) {
	idx, ok := l.addrToIdx[addr]
	if !ok {
		return nil, false
	}
	return &ledgerSigner{
		ledger: l.ledger,
		idx:    idx,
		addr:   addr,
	}, true
}

func (l *ledgerKeychain) Addresses() set.Set[ids.ShortID] {
	return l.addrs
}

func (l *ledgerSigner) SignHash(hash []byte) ([]byte, error) {
	return l.ledger.SignHash(hash, l.idx)
}

func (l *ledgerSigner) Sign(hash []byte) ([]byte, error) {
	return l.ledger.Sign(hash, l.idx)
}

func (l *ledgerSigner) Address() ids.ShortID {
	return l.addr
}
