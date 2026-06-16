package zapdb

import (
	"testing"
	"unsafe"
)

// TestZapKVRoundTrip proves the canonical ZAP KV codec (zap-spec/zapdb.zap)
// round-trips every field byte-for-byte.
func TestZapKVRoundTrip(t *testing.T) {
	key := []byte("account/0x35d64ff3/balance")
	val := []byte{0x19, 0xd9, 0x71, 0xe4, 0xfe, 0x84, 0x01, 0xe7}
	um := []byte{0x01}
	meta := []byte("trie")

	enc := encodeZapKV(424242, 9999, key, val, um, meta)
	if len(enc) != kvHeaderSize+len(key)+len(val)+len(um)+len(meta) {
		t.Fatalf("size %d unexpected", len(enc))
	}
	kv, err := decodeZapKV(enc)
	if err != nil {
		t.Fatal(err)
	}
	if kv.Version() != 424242 || kv.ExpiresAt() != 9999 {
		t.Fatalf("scalars: ver=%d exp=%d", kv.Version(), kv.ExpiresAt())
	}
	if string(kv.Key()) != string(key) || string(kv.Value()) != string(val) ||
		string(kv.UserMeta()) != string(um) || string(kv.Meta()) != string(meta) {
		t.Fatal("field mismatch")
	}
}

// TestZapKVZeroCopy proves the accessors alias the source buffer — no allocation,
// no copy. The Key() slice must point INTO enc, which is the whole point of ZAP
// vs the legacy sequential marshal_zap (which has no in-place reads).
func TestZapKVZeroCopy(t *testing.T) {
	key := []byte("the-key-bytes")
	enc := encodeZapKV(1, 0, key, []byte("v"), nil, nil)
	kv, err := decodeZapKV(enc)
	if err != nil {
		t.Fatal(err)
	}
	got := kv.Key()
	// The returned slice's backing array must be the same as enc's — i.e. its
	// data pointer falls within enc. That's zero-copy.
	base := uintptr(unsafe.Pointer(&enc[0]))
	end := base + uintptr(len(enc))
	p := uintptr(unsafe.Pointer(&got[0]))
	if p < base || p >= end {
		t.Fatalf("Key() did not alias the message buffer (copy detected)")
	}
	// Mutating the message is visible through the view → confirms aliasing.
	enc[int(p-base)] = 'X'
	if kv.Key()[0] != 'X' {
		t.Fatal("view did not reflect buffer mutation — not zero-copy")
	}
}

// TestZapKVBoundsCheck proves a truncated/forged record can't read out of range.
func TestZapKVBoundsCheck(t *testing.T) {
	enc := encodeZapKV(1, 0, []byte("key"), []byte("value"), nil, nil)
	if _, err := decodeZapKV(enc[:kvHeaderSize-1]); err == nil {
		t.Fatal("expected short-header error")
	}
	// Corrupt Value length to overflow the buffer.
	bad := make([]byte, len(enc))
	copy(bad, enc)
	bad[28] = 0xff
	bad[29] = 0xff
	if _, err := decodeZapKV(bad); err == nil {
		t.Fatal("expected out-of-range pointer error")
	}
}

// BenchmarkZapKVDecodeRead contrasts the cost of accessing a field. The canonical
// ZAP decode is a bounds-check then O(1) pointer reads with no allocation.
func BenchmarkZapKVDecodeRead(b *testing.B) {
	enc := encodeZapKV(1, 0, []byte("account/0x35d64ff3/balance"), make([]byte, 64), []byte{1}, nil)
	b.ReportAllocs()
	b.ResetTimer()
	var n int
	for i := 0; i < b.N; i++ {
		kv, err := decodeZapKV(enc)
		if err != nil {
			b.Fatal(err)
		}
		n += len(kv.Key()) + len(kv.Value()) + int(kv.Version())
	}
	_ = n
}
