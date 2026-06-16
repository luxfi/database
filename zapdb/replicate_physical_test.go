package zapdb

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

// memBackend stores objects in memory so a test can round-trip a physical
// snapshot (Put → List → Get) without S3.
type memBackend struct {
	mu   sync.Mutex
	objs map[string][]byte
}

func newMemBackend() *memBackend { return &memBackend{objs: map[string][]byte{}} }

func (m *memBackend) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	m.objs[key] = cp
	return nil
}
func (m *memBackend) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.objs[key]
	if !ok {
		return nil, backend.ErrNotFound
	}
	return v, nil
}
func (m *memBackend) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}
func (m *memBackend) List(_ context.Context, prefix string) (<-chan string, <-chan error) {
	m.mu.Lock()
	keys := make([]string, 0)
	for k := range m.objs {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	m.mu.Unlock()
	ks := make(chan string, len(keys)+1)
	es := make(chan error, 1)
	for _, k := range keys {
		ks <- k
	}
	close(ks)
	close(es)
	return ks, es
}
func (m *memBackend) Stat(context.Context, string) (int64, error) { return 0, backend.ErrNotFound }
func (m *memBackend) Close() error                                { return nil }
func (m *memBackend) String() string                              { return "mem://" }

// TestPhysicalSnapshotRoundTrip proves a physical (SST-copy) snapshot restores
// to byte-identical key/value state in a fresh dir, then logical incrementals
// layered on top recover writes made after the snapshot.
func TestPhysicalSnapshotRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src, err := New(dir, nil, "phys", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Seed 5000 keys, then snapshot physically.
	put := func(db *Database, lo, hi int) {
		b := db.NewBatch()
		for i := lo; i < hi; i++ {
			if err := b.Put([]byte(fmt.Sprintf("k%06d", i)), []byte(fmt.Sprintf("v%06d", i))); err != nil {
				t.Fatal(err)
			}
		}
		if err := b.Write(); err != nil {
			t.Fatal(err)
		}
	}
	put(src, 0, 5000)

	mem := newMemBackend()
	r := &vfsReplicator{db: src.db, dbPath: dir, be: mem, prefix: "p", compress: true, physical: true, maxPending: 16, snapMinIncs: 0}
	if err := r.physicalSnapshot(ctx); err != nil {
		t.Fatalf("physical snapshot: %v", err)
	}
	snapVer := r.lastSnapVersion
	if snapVer == 0 {
		t.Fatal("snapshot version 0")
	}

	// Write 5000 MORE keys after the snapshot, captured as a logical incremental.
	put(src, 5000, 10000)
	if err := r.Incremental(ctx); err != nil {
		t.Fatalf("incremental: %v", err)
	}
	src.Close()

	// Restore into a fresh dir: physical hydrate, then replay incrementals > snapVer.
	dst := t.TempDir()
	gotVer, err := extractLatestPhysical(ctx, mem, "p", nil, dst)
	if err != nil {
		t.Fatalf("extract physical: %v", err)
	}
	if gotVer != snapVer {
		t.Fatalf("restored phys version %d != snapshot %d", gotVer, snapVer)
	}
	restored, err := New(dst, nil, "phys", nil)
	if err != nil {
		t.Fatalf("open restored: %v", err)
	}
	defer restored.Close()

	// Apply the logical incremental (versions > snapVer) on top.
	r2 := &vfsReplicator{db: restored.db, be: mem, prefix: "p", compress: true, maxPending: 16}
	r2.sinceVersion = snapVer
	if err := r2.Restore(ctx); err != nil {
		t.Fatalf("restore incrementals: %v", err)
	}

	// Verify all 10000 keys present and correct.
	for i := 0; i < 10000; i++ {
		want := fmt.Sprintf("v%06d", i)
		got, err := restored.Get([]byte(fmt.Sprintf("k%06d", i)))
		if err != nil {
			t.Fatalf("key %d missing after restore: %v", i, err)
		}
		if string(got) != want {
			t.Fatalf("key %d: got %q want %q", i, got, want)
		}
	}
	t.Logf("physical snapshot @v%d (5000 keys) + logical incremental → 10000 keys restored byte-exact", snapVer)
}

// BenchmarkSnapshotPhysicalVsLogical contrasts the allocation cost of a physical
// (file-copy) snapshot against the logical (Backup-iterate) snapshot of the same
// DB. Physical should slash allocs/op (no Stream framework, no per-key marshal).
func BenchmarkSnapshotPhysicalVsLogical(b *testing.B) {
	run := func(b *testing.B, physical bool) {
		r, done := benchReplicator(b, 200_000, true)
		defer done()
		// benchReplicator's discardBackend drops Put; fine for an alloc bench.
		r.physical = physical
		ctx := context.Background()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			r.lastSnapVersion = 0
			r.incsSinceSnap = 0
			var err error
			if physical {
				err = r.physicalSnapshot(ctx)
			} else {
				err = r.backupAndUpload(ctx, 0, true)
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Run("logical", func(b *testing.B) { run(b, false) })
	b.Run("physical", func(b *testing.B) { run(b, true) })
}
