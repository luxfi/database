package zapdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	zdb "github.com/luxfi/zapdb"
	"github.com/luxfi/zapdb/pb"
)

// frameKVList frames changes exactly like db.Backup — [u64-LE len][marshaled
// KVList] — so db.Load restores a CDC delta with no special-casing.
func frameKVList(kvs []*pb.KV) []byte {
	body, _ := pb.Marshal(&pb.KVList{Kv: kvs})
	out := make([]byte, 8+len(body))
	binary.LittleEndian.PutUint64(out, uint64(len(body)))
	copy(out[8:], body)
	return out
}

// collectChanges subscribes to the write path (the logical WAL tail) and returns
// the next `want` changed KVs. This is what a CDC incremental drains each tick.
func collectChanges(t testing.TB, db *zdb.DB, want int, mutate func()) []*pb.KV {
	t.Helper()
	var (
		mu   sync.Mutex
		got  []*pb.KV
		done = make(chan struct{})
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go db.Subscribe(ctx, func(kvs *pb.KVList) error {
		mu.Lock()
		got = append(got, kvs.Kv...)
		full := len(got) >= want
		mu.Unlock()
		if full {
			select {
			case <-done:
			default:
				close(done)
			}
		}
		return nil
	}, []pb.Match{{Prefix: []byte{}}})

	time.Sleep(20 * time.Millisecond) // let the subscriber register
	mutate()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for change feed")
	}
	mu.Lock()
	defer mu.Unlock()
	return got
}

// BenchmarkIncrementalDeltaShip contrasts the two ways to ship a small delta out
// of a big DB: scan the whole keyspace (db.Backup(since)) vs drain the change
// feed (Subscribe). Same 100-key change; watch how scan tracks DB size while CDC
// tracks the change.
func BenchmarkIncrementalDeltaShip(b *testing.B) {
	const churn = 100
	r, done := benchReplicator(b, 200_000, true)
	defer done()
	db := r.db

	mutate := func() {
		bt := db.NewWriteBatch()
		for i := 0; i < churn; i++ {
			_ = bt.Set([]byte(fmt.Sprintf("key-%012d", i)), []byte("CHANGED-32-byte-value-padding!!!"))
		}
		_ = bt.Flush()
	}

	b.Run("scan_Backup_since", func(b *testing.B) {
		since := db.MaxVersion()
		mutate()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			var buf nullWriter
			if _, err := db.Backup(&buf, since); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("cdc_Subscribe", func(b *testing.B) {
		kvs := collectChanges(b, db, churn, mutate)
		if len(kvs) < churn {
			b.Fatalf("got %d changes, want >= %d", len(kvs), churn)
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = frameKVList(kvs[:churn])
		}
	})
}

type nullWriter struct{ n int64 }

func (w *nullWriter) Write(p []byte) (int, error) { w.n += int64(len(p)); return len(p), nil }
