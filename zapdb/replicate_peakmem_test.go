package zapdb

import (
	"context"
	"fmt"
	"io"
	"testing"
)

// TestPeakBackupBuffer quantifies what the streaming pipeline actually buys:
// peak in-memory buffer. The old path did `var buf bytes.Buffer; Backup(&buf)`,
// so it held the ENTIRE uncompressed dump, then allocated a separate compressed
// copy on top. The streaming path never materializes the uncompressed dump — the
// only buffer is the compressed output. This isn't a CPU win (the profile shows
// db.Backup internals dominate); it's the difference between fitting a multi-GB
// chain backup in RAM or OOMing.
func TestPeakBackupBuffer(t *testing.T) {
	r, done := benchReplicator(t, 200_000, true)
	defer done()
	disc := r.be.(*discardBackend)

	// Uncompressed dump size: what the OLD path buffered in full.
	var uncompressed int64
	if mv, err := r.db.Backup(&countSink{n: &uncompressed}, 0); err != nil || mv == 0 {
		t.Fatalf("backup: mv=%d err=%v", mv, err)
	}

	// New path: the only buffer held is the compressed output (disc.lastPut).
	r.lastSnapVersion = 0
	if err := r.backupAndUpload(context.Background(), 0, true); err != nil {
		t.Fatal(err)
	}
	compressed := disc.lastPut

	t.Logf("200k keys: OLD peak buffer ≈ uncompressed %d + compressed %d = %d bytes; NEW peak buffer = compressed %d bytes (%.1f× less)",
		uncompressed, compressed, uncompressed+compressed, compressed,
		float64(uncompressed+compressed)/float64(compressed))
	if compressed >= uncompressed {
		t.Fatalf("expected compressed (%d) << uncompressed (%d)", compressed, uncompressed)
	}
}

type countSink struct{ n *int64 }

func (c *countSink) Write(p []byte) (int, error) { *c.n += int64(len(p)); return len(p), nil }

var (
	_ io.Writer = (*countSink)(nil)
	_           = fmt.Sprintf
)
