package zapdb

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestCDCIncrementalRoundTrip proves a CDC incremental (shipped from the change
// feed, never a scan) restores byte-exact through the normal Restore path —
// because it's framed identically to a db.Backup batch.
func TestCDCIncrementalRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src, err := New(dir, nil, "cdc", nil)
	if err != nil {
		t.Fatal(err)
	}

	mem := newMemBackend()
	r := &vfsReplicator{db: src.db, be: mem, prefix: "c", compress: true, cdc: true, maxPending: 16}
	go r.runChangeFeed(ctx)
	time.Sleep(50 * time.Millisecond) // let the subscriber register

	const n = 3000
	b := src.NewBatch()
	for i := 0; i < n; i++ {
		if err := b.Put([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("v%06d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}

	// Wait for the feed to deliver, then ship the delta. No scan happened.
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.cdcMu.Lock()
		got := len(r.cdcBuf)
		r.cdcMu.Unlock()
		if got >= n || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := r.incrementalCDC(ctx); err != nil {
		t.Fatalf("incrementalCDC: %v", err)
	}
	src.Close()

	// Restore into a fresh DB from the shipped incrementals only.
	dst := t.TempDir()
	restored, err := New(dst, nil, "cdc", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	r2 := &vfsReplicator{db: restored.db, be: mem, prefix: "c", compress: true, maxPending: 16}
	if err := r2.Restore(ctx); err != nil {
		t.Fatalf("restore: %v", err)
	}

	for i := 0; i < n; i++ {
		got, err := restored.Get([]byte(fmt.Sprintf("k%06d", i)))
		if err != nil {
			t.Fatalf("key %d missing: %v", i, err)
		}
		if want := fmt.Sprintf("v%06d", i); string(got) != want {
			t.Fatalf("key %d: got %q want %q", i, got, want)
		}
	}
	t.Logf("%d keys shipped via change feed (no scan) → restored byte-exact", n)
}
