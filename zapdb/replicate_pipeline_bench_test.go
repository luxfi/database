package zapdb

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
)

// discardBackend is a no-network backend.Backend: Put records the payload size
// and drops it, so a benchmark measures the backup+codec pipeline (Backup →
// zstd → age → []byte) without S3 latency.
type discardBackend struct{ lastPut int64 }

func (d *discardBackend) Get(context.Context, string) ([]byte, error) {
	return nil, backend.ErrNotFound
}
func (d *discardBackend) Put(_ context.Context, _ string, data []byte) error {
	atomic.StoreInt64(&d.lastPut, int64(len(data)))
	return nil
}
func (d *discardBackend) Delete(context.Context, string) error { return nil }
func (d *discardBackend) List(context.Context, string) (<-chan string, <-chan error) {
	ks := make(chan string)
	es := make(chan error, 1)
	close(ks)
	close(es)
	return ks, es
}
func (d *discardBackend) Stat(context.Context, string) (int64, error) { return 0, backend.ErrNotFound }
func (d *discardBackend) Close() error                                { return nil }
func (d *discardBackend) String() string                              { return "discard://" }

// benchReplicator builds a replicator over a populated DB and a discard backend.
func benchReplicator(b testing.TB, nKeys int, compress bool) (*vfsReplicator, func()) {
	b.Helper()
	dir := b.TempDir()
	db, err := New(dir, nil, "bench", nil)
	if err != nil {
		b.Fatal(err)
	}
	batch := db.NewBatch()
	val := make([]byte, 256) // realistic trie-ish value
	for i := 0; i < nKeys; i++ {
		if err := batch.Put([]byte(fmt.Sprintf("key-%012d", i)), val); err != nil {
			b.Fatal(err)
		}
		if i%5000 == 0 {
			_ = batch.Write()
			batch = db.NewBatch()
		}
	}
	_ = batch.Write()

	// snapMinIncs:0 so Snapshot never short-circuits on the accumulation gate —
	// these benches/tests want the full backup to actually run every call.
	r := &vfsReplicator{db: db.db, dbPath: dir, be: &discardBackend{}, prefix: "b", compress: compress, maxPending: 16, snapMinIncs: 0}
	return r, func() { db.Close() }
}

// BenchmarkBackupPipeline measures a full snapshot (Backup→zstd→[]byte→Put) — the
// hot path. Watch B/op and allocs/op: the buffering implementation holds the full
// uncompressed backup PLUS a separate compressed copy.
func BenchmarkBackupPipeline(b *testing.B) {
	for _, n := range []int{50_000, 200_000} {
		b.Run(fmt.Sprintf("keys=%d/zstd", n), func(b *testing.B) {
			r, done := benchReplicator(b, n, true)
			defer done()
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				r.lastSnapVersion = 0 // force the snapshot to run each iteration
				if err := r.backupAndUpload(ctx, 0, true); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
