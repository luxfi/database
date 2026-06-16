// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

// Change-data-capture incrementals.
//
// db.Backup(since) finds a delta by SCANNING the whole keyspace — O(total), so a
// 100-key change out of a 200k-key DB costs ~20ms and ~1GB of allocations. ZapDB
// already records every write in its WAL, and db.Subscribe taps that write path
// and PUSHES each committed batch. Draining that feed ships the same delta in
// ~3µs / ~19KB — O(change), not O(DB size). The bytes are framed exactly like
// db.Backup, so Restore/Load consume a CDC incremental with no special case.

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/luxfi/age"
	log "github.com/luxfi/log"
	"github.com/luxfi/zapdb/pb"
)

// runChangeFeed subscribes to every committed write for the life of ctx and
// appends it to the buffer the incremental tick drains. Blocks; run in a
// goroutine. An empty-prefix match means "all keys".
func (r *vfsReplicator) runChangeFeed(ctx context.Context) {
	err := r.db.Subscribe(ctx, func(kvs *pb.KVList) error {
		r.cdcMu.Lock()
		r.cdcBuf = append(r.cdcBuf, kvs.Kv...)
		r.cdcMu.Unlock()
		return nil
	}, []pb.Match{{Prefix: []byte{}}})
	if err != nil && ctx.Err() == nil {
		log.Warn(fmt.Sprintf("[zapdb] replicate: change feed ended: %v", err))
	}
}

// incrementalCDC ships the buffered change feed as one inc/<version> object. No
// scan: the cost is proportional to what changed, not to the database size.
func (r *vfsReplicator) incrementalCDC(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cdcMu.Lock()
	kvs := r.cdcBuf
	r.cdcBuf = nil
	r.cdcMu.Unlock()
	if len(kvs) == 0 {
		return nil
	}

	var maxVer uint64
	for _, kv := range kvs {
		if kv.Version > maxVer {
			maxVer = kv.Version
		}
	}

	// Frame as [u64-LE len][marshaled KVList] — one db.Backup batch — then run it
	// through the shared zstd→age pipeline.
	body, err := pb.Marshal(&pb.KVList{Kv: kvs})
	if err != nil {
		return fmt.Errorf("marshal changes: %w", err)
	}
	payload, raw, err := r.encodeBlob(func(w io.Writer) error {
		var hdr [8]byte
		binary.LittleEndian.PutUint64(hdr[:], uint64(len(body)))
		if _, err := w.Write(hdr[:]); err != nil {
			return err
		}
		_, err := w.Write(body)
		return err
	})
	if err != nil {
		return err
	}

	key := r.incKey(maxVer)
	if err := r.be.Put(ctx, key, payload); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	r.sinceVersion = maxVer
	r.incsSinceSnap++
	log.Info(fmt.Sprintf("[zapdb] replicate: cdc incremental %s (%d changes, %d→%d bytes, version %d)",
		key, len(kvs), raw, len(payload), maxVer))
	return nil
}

// encodeBlob runs produce through the [zstd]→[age] codec chain and returns the
// encoded bytes plus the uncompressed byte count. The CDC delta is small, so this
// uses a plain buffer (the large backup/snapshot paths stream into a pooled one).
func (r *vfsReplicator) encodeBlob(produce func(io.Writer) error) ([]byte, int64, error) {
	var out bytes.Buffer
	var sink io.Writer = &out
	var ageW io.WriteCloser
	if r.recipient != nil {
		w, err := age.Encrypt(&out, r.recipient)
		if err != nil {
			return nil, 0, fmt.Errorf("encrypt: %w", err)
		}
		ageW, sink = w, w
	}
	var zw *zstd.Encoder
	if r.compress {
		zw = zstdWriterPool.Get().(*zstd.Encoder)
		zw.Reset(sink)
		sink = zw
	}
	cnt := &countWriter{w: sink}
	err := produce(cnt)
	if zw != nil {
		if e := zw.Close(); err == nil {
			err = e
		}
		zstdWriterPool.Put(zw)
	}
	if ageW != nil {
		if e := ageW.Close(); err == nil {
			err = e
		}
	}
	if err != nil {
		return nil, 0, err
	}
	return out.Bytes(), cnt.n, nil
}
