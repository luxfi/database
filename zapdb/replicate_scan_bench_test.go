package zapdb

import (
	"fmt"
	"io"
	"testing"
	"time"
)

// TestScanCostVsGate quantifies the Carmack fix: an idle incremental tick used
// to call db.Backup(since=max), which walks the WHOLE keyspace just to discover
// nothing changed. The MaxVersion() gate replaces that O(#keys) scan with an
// O(#tables) probe. This is a measurement, not an assertion — run with -v.
func TestScanCostVsGate(t *testing.T) {
	dir := t.TempDir()
	db, err := New(dir, nil, "bench", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	const n = 200_000
	batch := db.NewBatch()
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key-%012d", i))
		if err := batch.Put(k, []byte("value-payload-32-bytes-padding!!")); err != nil {
			t.Fatal(err)
		}
		if i%10000 == 0 {
			if err := batch.Write(); err != nil {
				t.Fatal(err)
			}
			batch = db.NewBatch()
		}
	}
	if err := batch.Write(); err != nil {
		t.Fatal(err)
	}

	max := db.db.MaxVersion()
	// CORRECTNESS GUARD: the gate keys off MaxVersion(). If it returned 0 after a
	// real write, `cur <= since` would skip every backup and silently kill
	// replication. It must be non-zero and must agree with a full Backup's max.
	if max == 0 {
		t.Fatalf("MaxVersion()=0 after %d writes — gate would suppress all backups", n)
	}
	fullMax, err := db.db.Backup(io.Discard, 0)
	if err != nil {
		t.Fatal(err)
	}
	if fullMax != max {
		t.Fatalf("MaxVersion()=%d disagrees with Backup(0) max=%d — gate would be wrong", max, fullMax)
	}

	// Idle incremental, OLD path: full Backup scan with since=max yields nothing.
	t0 := time.Now()
	mv, err := db.db.Backup(io.Discard, max)
	scan := time.Since(t0)
	if err != nil {
		t.Fatal(err)
	}
	if mv != 0 {
		t.Fatalf("idle Backup(since=max) returned max=%d, expected 0 (nothing newer)", mv)
	}

	// Idle incremental, NEW path: the cheap gate.
	t1 := time.Now()
	for i := 0; i < 1000; i++ {
		_ = db.db.MaxVersion()
	}
	gate := time.Since(t1) / 1000

	t.Logf("keys=%d  idle Backup(since=max) scan=%v (maxVer=%d)  MaxVersion() gate=%v  speedup=%.0fx",
		n, scan, mv, gate, float64(scan)/float64(gate+1))
}
