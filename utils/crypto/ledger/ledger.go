// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package ledger

import (
	"errors"
	"fmt"

	"github.com/luxfi/ids"
	"github.com/luxfi/node/utils/crypto/keychain"
	"github.com/luxfi/node/version"

	ledger "github.com/luxfi/ledger-lux-go"
)

// LedgerAdapter wraps ledger.LedgerLux to implement keychain.Ledger interface
type LedgerAdapter struct {
	device *ledger.LedgerLux
}

// NewLedger creates a new ledger adapter
func NewLedger() (keychain.Ledger, error) {
	device, err := ledger.FindLedgerLuxApp()
	if err != nil {
		return nil, err
	}
	return &LedgerAdapter{device: device}, nil
}

// Version returns the app version
func (l *LedgerAdapter) Version() (*version.Semantic, error) {
	ver, err := l.device.GetVersion()
	if err != nil {
		return nil, err
	}
	return &version.Semantic{
		Major: int(ver.Major),
		Minor: int(ver.Minor),
		Patch: int(ver.Patch),
	}, nil
}

// Address returns an address at the given index
func (l *LedgerAdapter) Address(displayHRP string, addressIndex uint32) (ids.ShortID, error) {
	pathStr := fmt.Sprintf("44'/9000'/%d'/0/0", addressIndex)
	resp, err := l.device.GetPubKey(pathStr, false, displayHRP, "")
	if err != nil {
		return ids.ShortID{}, err
	}
	return ids.ShortFromString(resp.Address)
}

// SignHash signs a hash with the given address index
func (l *LedgerAdapter) SignHash(hash []byte, addressIndex uint32) ([]byte, error) {
	pathPrefix := fmt.Sprintf("44'/9000'/%d'", addressIndex)
	signingPaths := []string{"0/0"}
	resp, err := l.device.SignHash(pathPrefix, signingPaths, hash)
	if err != nil {
		return nil, err
	}
	// Get the first signature from the response
	for _, sig := range resp.Signature {
		return sig, nil
	}
	return nil, errors.New("no signature returned")
}

// Sign signs data with the given address index
func (l *LedgerAdapter) Sign(data []byte, addressIndex uint32) ([]byte, error) {
	return l.SignHash(data, addressIndex)
}

// SignTransaction signs a transaction with multiple addresses
func (l *LedgerAdapter) SignTransaction(rawUnsignedHash []byte, addressIndices []uint32) ([][]byte, error) {
	// Build signing paths for all addresses
	signingPaths := make([]string, len(addressIndices))
	for i, idx := range addressIndices {
		signingPaths[i] = fmt.Sprintf("%d'/0/0", idx)
	}

	// Sign with all paths at once
	pathPrefix := "44'/9000'"
	resp, err := l.device.SignHash(pathPrefix, signingPaths, rawUnsignedHash)
	if err != nil {
		return nil, err
	}

	// Extract signatures
	sigs := make([][]byte, 0, len(resp.Signature))
	for _, sig := range resp.Signature {
		sigs = append(sigs, sig)
	}

	if len(sigs) != len(addressIndices) {
		return nil, errors.New("incorrect number of signatures returned")
	}

	return sigs, nil
}

// GetAddresses returns addresses at the given indices
func (l *LedgerAdapter) GetAddresses(addressIndices []uint32) ([]ids.ShortID, error) {
	addrs := make([]ids.ShortID, len(addressIndices))
	for i, idx := range addressIndices {
		addr, err := l.Address("", idx)
		if err != nil {
			return nil, err
		}
		addrs[i] = addr
	}
	return addrs, nil
}

// Disconnect closes the ledger connection
func (l *LedgerAdapter) Disconnect() error {
	return l.device.Close()
}
