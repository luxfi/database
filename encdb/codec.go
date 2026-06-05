// Copyright (C) 2019-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package encdb

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// CodecVersion is the wire version of the encryptedValue envelope.
// On-disk schema; bump on any incompatible layout change.
const CodecVersion = uint16(0)

var (
	errShortBuffer    = errors.New("encdb: short buffer")
	errInvalidVersion = errors.New("encdb: invalid version")
)

// marshalEncryptedValue encodes an encryptedValue as:
//
//	u16 version | u32 ciphertext_len | ciphertext | u32 nonce_len | nonce
//
// Big-endian. Replaces the prior linearcodec.Marshal path so the encdb
// package no longer depends on github.com/luxfi/codec.
func marshalEncryptedValue(v *encryptedValue) ([]byte, error) {
	const headerLen = 2 + 4 + 4
	if uint64(len(v.Ciphertext)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("encdb: ciphertext too large (%d bytes)", len(v.Ciphertext))
	}
	if uint64(len(v.Nonce)) > uint64(^uint32(0)) {
		return nil, fmt.Errorf("encdb: nonce too large (%d bytes)", len(v.Nonce))
	}
	out := make([]byte, headerLen+len(v.Ciphertext)+len(v.Nonce))
	binary.BigEndian.PutUint16(out[0:2], CodecVersion)
	binary.BigEndian.PutUint32(out[2:6], uint32(len(v.Ciphertext)))
	off := 6
	copy(out[off:], v.Ciphertext)
	off += len(v.Ciphertext)
	binary.BigEndian.PutUint32(out[off:off+4], uint32(len(v.Nonce)))
	off += 4
	copy(out[off:], v.Nonce)
	return out, nil
}

func unmarshalEncryptedValue(b []byte, v *encryptedValue) error {
	if len(b) < 2 {
		return errShortBuffer
	}
	ver := binary.BigEndian.Uint16(b[0:2])
	if ver != CodecVersion {
		return fmt.Errorf("%w: got %d want %d", errInvalidVersion, ver, CodecVersion)
	}
	if len(b) < 6 {
		return errShortBuffer
	}
	ctLen := binary.BigEndian.Uint32(b[2:6])
	if uint64(len(b)) < 6+uint64(ctLen)+4 {
		return errShortBuffer
	}
	v.Ciphertext = make([]byte, ctLen)
	copy(v.Ciphertext, b[6:6+ctLen])
	off := 6 + ctLen
	nLen := binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	if uint64(len(b)) < uint64(off)+uint64(nLen) {
		return errShortBuffer
	}
	v.Nonce = make([]byte, nLen)
	copy(v.Nonce, b[off:off+uint32(nLen)])
	return nil
}
