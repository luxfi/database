// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

// Physical (file-copy) snapshots.
//
// A logical snapshot (db.Backup(0)) iterates EVERY key and re-serializes it —
// the Stream framework that the profile showed amplifies a 63 MB dump into
// ~1.2 GB of allocations. A physical snapshot skips all of that: it copies the
// on-disk LSM files (SSTs, value logs, memtable WALs, MANIFEST) verbatim. The
// raw tables are already Snappy-compressed, and Badger's on-disk format is
// crash-consistent — a point-in-time file set reopens cleanly, so a snapshot is
// just a "simulated crash" that Badger recovers from.
//
// Trade-off vs logical: physical ships raw table bytes (whole DB) rather than a
// since-version delta, so it is for the SNAPSHOT (full re-baseline) path only —
// incrementals stay logical. Restore: hydrate the dir from the physical object
// BEFORE Badger opens it, then replay logical incrementals layered on top.

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hanzoai/vfs/pkg/backend"
	"github.com/klauspost/compress/zstd"
	"github.com/luxfi/age"
	log "github.com/luxfi/log"
	zdb "github.com/luxfi/zapdb"
)

// physExt is the object suffix for a physical snapshot: a tar of the LSM dir,
// optionally zstd-compressed and age-encrypted. Distinct ".ztar" base so restore
// never confuses it with a logical ".zap" backup.
func (r *vfsReplicator) physExt() string {
	s := ".ztar"
	if r.compress {
		s += ".zst"
	}
	if r.recipient != nil || r.identity != nil {
		s += ".age"
	}
	return s
}

func (r *vfsReplicator) physKey(version uint64) string {
	return path.Join(r.prefix, "phys", fmt.Sprintf("%020d%s", version, r.physExt()))
}

// physicalSnapshot stages a consistent copy of the LSM dir, tars it through the
// streaming zstd→age pipeline, and uploads it under phys/<version>. No key is
// ever iterated or re-serialized.
func (r *vfsReplicator) physicalSnapshot(ctx context.Context) error {
	if r.dbPath == "" {
		return fmt.Errorf("physical snapshot: db path unknown")
	}
	// Version is the live MaxVersion captured BEFORE staging — a safe lower bound.
	// The staged files contain at least this version; writes that land during the
	// copy are re-shipped by the next logical incremental (Backup(since=version)).
	// Overlap is harmless (Badger keeps the max version); a gap would not be.
	version := r.db.MaxVersion()
	if version <= r.lastSnapVersion {
		return nil
	}
	stage := r.dbPath + ".zapsnap"
	defer os.RemoveAll(stage)
	if err := stagePhysical(r.dbPath, stage); err != nil {
		return fmt.Errorf("stage: %w", err)
	}

	out := backupBufPool.Get().(*bytes.Buffer)
	out.Reset()
	defer func() {
		if out.Cap() <= maxPooledBuf {
			backupBufPool.Put(out)
		}
	}()

	// tar → [zstd] → [age] → out, mirroring the logical streaming pipeline.
	var sink io.Writer = out
	var ageW io.WriteCloser
	if r.recipient != nil {
		w, err := age.Encrypt(out, r.recipient)
		if err != nil {
			return fmt.Errorf("encrypt: %w", err)
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

	raw, err := tarDir(cnt, stage)
	if err != nil {
		if zw != nil {
			_ = zw.Close()
			zstdWriterPool.Put(zw)
		}
		return fmt.Errorf("tar: %w", err)
	}
	if zw != nil {
		err := zw.Close()
		zstdWriterPool.Put(zw)
		if err != nil {
			return fmt.Errorf("compress: %w", err)
		}
	}
	if ageW != nil {
		if err := ageW.Close(); err != nil {
			return fmt.Errorf("encrypt: %w", err)
		}
	}

	key := r.physKey(version)
	if err := r.be.Put(ctx, key, out.Bytes()); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	r.sinceVersion = version
	r.lastSnapVersion = version
	r.incsSinceSnap = 0
	log.Info(fmt.Sprintf("[zapdb] replicate: physical snapshot %s (%d→%d bytes, version %d)",
		key, raw, out.Len(), version))
	return nil
}

// physicalStrict, when set (REPLICATE_PHYSICAL_STRICT=true), adds a Badger open
// of the staged copy as a belt-and-suspenders consistency check. It's OFF by
// default because the open costs as much as a logical backup (~the thing we're
// avoiding) and the cheap path below — copy the MANIFEST first, then its tables,
// retrying if any vanished — already catches the realistic race. The ultimate
// backstop is restore: a torn snapshot fails to open there, and the PREVIOUS
// snapshot + its incrementals (never deleted) still reconstruct the DB.
var physicalStrict = false

func stagePhysical(srcDir, stage string) error {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		_ = os.RemoveAll(stage)
		if err := os.MkdirAll(stage, 0o755); err != nil {
			return err
		}
		if err := copyLSMDir(srcDir, stage); err != nil {
			// A table named by the MANIFEST disappeared under us (compaction):
			// re-snapshot the file set and retry.
			lastErr = err
			continue
		}
		if physicalStrict {
			opts := zdb.DefaultOptions(stage)
			opts.Logger = nil
			opts.NumCompactors = 0
			opts.CompactL0OnClose = false
			db, err := zdb.Open(opts)
			if err != nil {
				lastErr = err
				continue
			}
			if err := db.Close(); err != nil {
				lastErr = err
				continue
			}
		}
		return nil
	}
	return fmt.Errorf("could not stage a consistent copy after retries: %w", lastErr)
}

// copyLSMDir copies every LSM file except the lock (Badger makes its own) and any
// staging dir, MANIFEST FIRST. Copying the MANIFEST before the tables it names
// means a concurrent compaction that then deletes one of those tables surfaces as
// a not-found error when we reach it (→ retry), rather than a silently truncated
// snapshot. A missing source file returns os.ErrNotExist so the caller retries.
func copyLSMDir(srcDir, dstDir string) error {
	entries, err := os.ReadDir(srcDir)
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || e.Name() == "LOCK" || strings.HasSuffix(e.Name(), ".zapsnap") {
			continue
		}
		names = append(names, e.Name())
	}
	// MANIFEST first, then the rest in name order.
	sort.Slice(names, func(i, j int) bool {
		if names[i] == "MANIFEST" {
			return true
		}
		if names[j] == "MANIFEST" {
			return false
		}
		return names[i] < names[j]
	})
	for _, name := range names {
		if err := copyFile(filepath.Join(srcDir, name), filepath.Join(dstDir, name)); err != nil {
			return fmt.Errorf("copy %s: %w", name, err)
		}
	}
	return nil
}

// SEEK_DATA / SEEK_HOLE (Linux & macOS) let us skip the holes in Badger's
// preallocated value log — it reserves a 1 GB file that is almost entirely a
// sparse hole (e.g. 1.8 MB real of 1 GB apparent). A plain io.Copy would read
// all 1 GB of zeros; a hole-aware copy moves only the live extents.
const (
	seekData = 3
	seekHole = 4
)

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	if err := copySparse(in, out); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// copySparse copies only the data extents of in to out and truncates out to the
// end of the LAST data extent — so Badger's preallocated value log (a 1.8 MB-real
// of 1 GB-apparent file with a giant trailing hole) is staged at its real size,
// which makes the downstream tar read live bytes only instead of 1 GB of zeros.
// Badger re-preallocates the vlog on the next write after restore. Falls back to
// a full io.Copy where SEEK_DATA/SEEK_HOLE is unsupported.
func copySparse(in, out *os.File) error {
	info, err := in.Stat()
	if err != nil {
		return err
	}
	size := info.Size()
	var off, realEnd int64
	for off < size {
		dataStart, err := in.Seek(off, seekData)
		if err != nil {
			// ENXIO past the last extent → no more data; or unsupported → fall back.
			if off == 0 {
				if _, serr := in.Seek(0, io.SeekStart); serr == nil {
					if _, cerr := io.Copy(out, in); cerr == nil {
						return out.Truncate(size)
					}
				}
			}
			break
		}
		holeStart, err := in.Seek(dataStart, seekHole)
		if err != nil {
			holeStart = size
		}
		if _, err := out.Seek(dataStart, io.SeekStart); err != nil {
			return err
		}
		if _, err := in.Seek(dataStart, io.SeekStart); err != nil {
			return err
		}
		if _, err := io.CopyN(out, in, holeStart-dataStart); err != nil {
			return err
		}
		off = holeStart
		realEnd = holeStart
	}
	return out.Truncate(realEnd)
}

// tarDir writes every regular file in dir as a tar stream to w (no per-key work),
// returning the uncompressed byte count. The lock and any staging dir are skipped.
func tarDir(w io.Writer, dir string) (int64, error) {
	tw := tar.NewWriter(w)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, e := range entries {
		if e.IsDir() || e.Name() == "LOCK" {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return total, err
		}
		hdr := &tar.Header{Name: e.Name(), Mode: 0o644, Size: info.Size()}
		if err := tw.WriteHeader(hdr); err != nil {
			return total, err
		}
		f, err := os.Open(filepath.Join(dir, e.Name()))
		if err != nil {
			return total, err
		}
		n, err := io.Copy(tw, f)
		f.Close()
		if err != nil {
			return total, err
		}
		total += n
	}
	return total, tw.Close()
}

// untarInto extracts a tar stream into dir (which must exist and be empty).
func untarInto(r io.Reader, dir string) error {
	tr := tar.NewReader(r)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// Defend against path traversal in object content.
		if strings.Contains(hdr.Name, "/") || strings.Contains(hdr.Name, "..") {
			return fmt.Errorf("untar: unsafe name %q", hdr.Name)
		}
		out, err := os.Create(filepath.Join(dir, hdr.Name))
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			return err
		}
		if err := out.Close(); err != nil {
			return err
		}
	}
}

// latestPhysicalInto downloads the newest phys/ object (if any) and extracts it
// into destDir, returning the snapshot version (0 if none). It opens its own
// backend so it can run from New() before the DB — and thus the replicator —
// exists. A no-op (returns 0) when physical replication isn't configured.
func latestPhysicalInto(ctx context.Context, cfg vfsReplicatorConfig, identity age.Identity, destDir string) (uint64, error) {
	be, err := backend.Open(ctx, backendURL(cfg))
	if err != nil {
		return 0, err
	}
	defer be.Close()
	return extractLatestPhysical(ctx, be, cfg.Path, identity, destDir)
}

// extractLatestPhysical is the backend-agnostic core: find the newest phys/
// object under prefix, decrypt/decompress per its suffix, and untar into destDir.
func extractLatestPhysical(ctx context.Context, be backend.Backend, keyPrefix string, identity age.Identity, destDir string) (uint64, error) {
	prefix := path.Join(keyPrefix, "phys") + "/"
	latest := ""
	keysCh, errCh := be.List(ctx, prefix)
	for k := range keysCh {
		if k > latest {
			latest = k
		}
	}
	if err := <-errCh; err != nil {
		return 0, err
	}
	if latest == "" {
		return 0, nil
	}

	data, err := be.Get(ctx, latest)
	if err != nil {
		return 0, fmt.Errorf("get %s: %w", latest, err)
	}
	if strings.HasSuffix(latest, ".age") {
		if identity == nil {
			return 0, fmt.Errorf("physical snapshot %s is encrypted but no identity set", latest)
		}
		if data, err = ageDecrypt(data, identity); err != nil {
			return 0, fmt.Errorf("decrypt %s: %w", latest, err)
		}
	}
	if strings.HasSuffix(strings.TrimSuffix(latest, ".age"), ".zst") {
		if data, err = zstdDecompress(data); err != nil {
			return 0, fmt.Errorf("decompress %s: %w", latest, err)
		}
	}
	if err := untarInto(bytes.NewReader(data), destDir); err != nil {
		return 0, fmt.Errorf("untar %s: %w", latest, err)
	}
	return versionFromKey(latest), nil
}
