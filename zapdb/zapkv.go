// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

// Canonical ZAP encoding for a KV record — the wire format defined in the
// colocated schema ./zapdb.zap (written in the ZAP format whose layout rules
// live in github.com/zap-proto/zap-spec). Unlike luxfi/zapdb's pb.marshal_zap
// (a sequential [u32 len][bytes]… layout that is "zap" in name only), this is a
// true ZAP struct: a fixed 48-byte header of scalars and (offset:u32, length:u32)
// pointer pairs, with payloads in the heap area that follows. Any field resolves
// in O(1) from its compile-time offset, and the byte accessors return slices that
// ALIAS the source buffer — zero-copy reads, no per-field allocation.
//
// This is the prototype runtime for moving the ZapDB backup codec onto the
// canonical schema. See zapkv_test.go for byte-stability + zero-copy proof and
// the package doc note on why this is not yet wired into luxfi/zapdb's backup.

import (
	"encoding/binary"
	"fmt"
)

// kvHeaderSize is the fixed-area size of a canonical ZAP KV: two u64 scalars
// (Version, ExpiresAt) + four (offset,length) pointer pairs (Key, Value,
// UserMeta, Meta), 8 bytes each = 48 bytes. Offsets are from the message start.
const kvHeaderSize = 48

func leU32(b []byte, off int) uint32 { return binary.LittleEndian.Uint32(b[off:]) }
func leU64(b []byte, off int) uint64 { return binary.LittleEndian.Uint64(b[off:]) }

// zapKV is a zero-copy view over a canonical ZAP KV record.
type zapKV struct{ b []byte }

func (k zapKV) Version() uint64   { return leU64(k.b, 0) }
func (k zapKV) ExpiresAt() uint64 { return leU64(k.b, 8) }

// ptr resolves an (offset,length) pointer at header offset hoff to a slice that
// aliases the underlying message — no copy.
func (k zapKV) ptr(hoff int) []byte {
	off, length := leU32(k.b, hoff), leU32(k.b, hoff+4)
	if length == 0 {
		return nil
	}
	return k.b[off : off+length]
}

func (k zapKV) Key() []byte      { return k.ptr(16) }
func (k zapKV) Value() []byte    { return k.ptr(24) }
func (k zapKV) UserMeta() []byte { return k.ptr(32) }
func (k zapKV) Meta() []byte     { return k.ptr(40) }

// encodeZapKV writes the canonical fixed-header + heap encoding of one KV record.
func encodeZapKV(version, expiresAt uint64, key, value, userMeta, meta []byte) []byte {
	b := make([]byte, kvHeaderSize+len(key)+len(value)+len(userMeta)+len(meta))
	binary.LittleEndian.PutUint64(b[0:], version)
	binary.LittleEndian.PutUint64(b[8:], expiresAt)
	off := kvHeaderSize
	put := func(hoff int, data []byte) {
		binary.LittleEndian.PutUint32(b[hoff:], uint32(off))
		binary.LittleEndian.PutUint32(b[hoff+4:], uint32(len(data)))
		off += copy(b[off:], data)
	}
	put(16, key)
	put(24, value)
	put(32, userMeta)
	put(40, meta)
	return b
}

// decodeZapKV returns a zero-copy view after bounds-checking every pointer, so an
// adversarial/truncated buffer can't make an accessor read out of range.
func decodeZapKV(b []byte) (zapKV, error) {
	if len(b) < kvHeaderSize {
		return zapKV{}, fmt.Errorf("zapkv: record %d bytes < header %d", len(b), kvHeaderSize)
	}
	for _, hoff := range []int{16, 24, 32, 40} {
		off, length := leU32(b, hoff), leU32(b, hoff+4)
		if uint64(off)+uint64(length) > uint64(len(b)) {
			return zapKV{}, fmt.Errorf("zapkv: pointer @%d (off=%d len=%d) exceeds buffer %d", hoff, off, length, len(b))
		}
	}
	return zapKV{b}, nil
}
