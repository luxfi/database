// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package linear

import (
	"bytes"
	"errors"
	"testing"
)

// TestLargeBlobRoundTrip verifies that the codec accepts blobs sized like
// real FHE bootstrap keys (STD128 / PN10QP27 / PN11QP54 are ~50MB; we test
// 100MB to confirm headroom). Surfaced by luxcpp/fhed standalone smoke test
// (issue #251). Cap raised from implicit math.MaxInt32 / 1MB upstream default
// to an explicit 256MiB DefaultMaxBlobSize (issue #252).
func TestLargeBlobRoundTrip(t *testing.T) {
	const blobSize = 100 * 1024 * 1024 // 100 MiB

	mgr := NewManager(0) // 0 → DefaultMaxBlobSize = 256MiB
	if err := mgr.RegisterCodec(0, NewDefault()); err != nil {
		t.Fatalf("register codec: %v", err)
	}

	src := make([]byte, blobSize)
	for i := range src {
		src[i] = byte(i)
	}

	encoded, err := mgr.Marshal(0, src)
	if err != nil {
		t.Fatalf("marshal 100MB blob: %v", err)
	}
	if len(encoded) < blobSize {
		t.Fatalf("encoded length %d < input %d", len(encoded), blobSize)
	}

	var dst []byte
	version, err := mgr.Unmarshal(encoded, &dst)
	if err != nil {
		t.Fatalf("unmarshal 100MB blob: %v", err)
	}
	if version != 0 {
		t.Fatalf("version: got %d want 0", version)
	}
	if !bytes.Equal(dst, src) {
		t.Fatalf("round-trip mismatch: len(dst)=%d len(src)=%d", len(dst), len(src))
	}
}

// TestBlobCapEnforced verifies the per-Manager cap rejects oversize blobs.
// This protects against unbounded allocations on adversarial inputs.
func TestBlobCapEnforced(t *testing.T) {
	const cap = 1 * 1024 * 1024 // 1 MiB

	mgr := NewManager(cap)
	if err := mgr.RegisterCodec(0, NewDefault()); err != nil {
		t.Fatalf("register codec: %v", err)
	}

	oversize := make([]byte, cap+1)

	if _, err := mgr.Marshal(0, oversize); !errors.Is(err, ErrMaxSliceLenExceeded) {
		t.Fatalf("marshal: got %v want ErrMaxSliceLenExceeded", err)
	}

	// Build a too-large encoded payload manually: 2-byte version + cap+1 bytes.
	encoded := make([]byte, 2+cap+1+4)
	if _, err := mgr.Unmarshal(encoded, &[]byte{}); !errors.Is(err, ErrMaxSliceLenExceeded) {
		t.Fatalf("unmarshal: got %v want ErrMaxSliceLenExceeded", err)
	}
}

// TestDefaultCapNonZero guards the default cap value so a future refactor
// that drops the default to 0 is caught by CI rather than at runtime.
func TestDefaultCapNonZero(t *testing.T) {
	if DefaultMaxBlobSize <= 0 {
		t.Fatalf("DefaultMaxBlobSize must be positive, got %d", DefaultMaxBlobSize)
	}
	if DefaultMaxBlobSize < 64*1024*1024 {
		t.Fatalf("DefaultMaxBlobSize=%d below FHE bootstrap key headroom (need >=64MiB)", DefaultMaxBlobSize)
	}
	if DefaultMaxBlobSize > MaxInt {
		t.Fatalf("DefaultMaxBlobSize=%d exceeds MaxInt=%d", DefaultMaxBlobSize, MaxInt)
	}
}
