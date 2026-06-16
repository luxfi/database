package zapdb

import (
	"crypto/rand"
	"io"
	"testing"

	"github.com/luxfi/age"
	"github.com/luxfi/zapdb/pb"
)

// realisticDelta builds n trie-like changed KVs: 32-byte keys + ~100-byte values,
// high-entropy (random) like real EVM state — i.e. poorly compressible, the
// honest worst case for the codec (unlike the zero-heavy synthetic data).
func realisticDelta(n int) []*pb.KV {
	kvs := make([]*pb.KV, n)
	for i := range kvs {
		k := make([]byte, 32)
		v := make([]byte, 100)
		rand.Read(k)
		rand.Read(v)
		kvs[i] = &pb.KV{Key: k, Value: v, Version: uint64(i + 1), UserMeta: []byte{0}}
	}
	return kvs
}

// BenchmarkPQOverhead studies the per-incremental encode cost across crypto
// choices: none / classical age (X25519) / post-quantum age (ML-KEM-768 + X25519
// hybrid). Same realistic 164-change delta the live C-chain ships per batch. The
// gap between classical and PQ is the price of quantum resistance per object.
func BenchmarkPQOverhead(b *testing.B) {
	kvs := realisticDelta(164)
	body, _ := pb.Marshal(&pb.KVList{Kv: kvs})
	produce := func(w io.Writer) error { _, e := w.Write(body); return e }

	x, _ := age.GenerateX25519Identity()
	pq, _ := age.GeneratePQIdentity(age.PQKemHPKEMLKEM768X25519)
	pqr := pq.(*age.HybridIdentity).Recipient()

	for _, c := range []struct {
		name string
		r    age.Recipient
	}{
		{"plaintext", nil},
		{"classical_x25519", x.Recipient()},
		{"pq_mlkem768x25519", pqr},
	} {
		b.Run(c.name, func(b *testing.B) {
			r := &vfsReplicator{compress: true, recipient: c.r}
			var objBytes int
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, _, err := r.encodeBlob(produce)
				if err != nil {
					b.Fatal(err)
				}
				objBytes = len(out)
			}
			b.ReportMetric(float64(objBytes), "obj_bytes")
			b.ReportMetric(float64(len(body)), "raw_bytes")
		})
	}
}
