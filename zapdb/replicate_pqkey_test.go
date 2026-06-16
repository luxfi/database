package zapdb

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/luxfi/age"
)

// TestPQKeyOutOfBox: a key file is generated on first use (ML-KEM-768 hybrid),
// stable across reload, and its derived recipient round-trips a full encrypted
// backup → restore. This is the "PQ native out of the box" path.
func TestPQKeyOutOfBox(t *testing.T) {
	kf := filepath.Join(t.TempDir(), "pq.key")
	s1, err := loadOrCreatePQKey(kf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(s1, "AGE-SECRET-KEY-PQ-1") {
		t.Fatalf("not a PQ identity: %.24s", s1)
	}
	if s2, _ := loadOrCreatePQKey(kf); s1 != s2 {
		t.Fatal("key not stable across reload")
	}
	ids, _ := age.ParseIdentities(strings.NewReader(s1))
	r := recipientFor(ids[0])
	if r == nil {
		t.Fatal("no recipient derived from identity")
	}
	roundTrip(t, r, ids[0])
}
