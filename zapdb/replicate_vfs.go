// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

// Native ZAP replication on the hanzo storage stack.
//
// vfsReplicator streams ZapDB backups to an object store through
// github.com/hanzoai/vfs (its pluggable Backend → the s3:// backend), which
// talks to a github.com/hanzoai/s3 server (S3-wire-compatible). Objects are
// age-encrypted with github.com/luxfi/age. Layout, env, and naming match the
// github.com/hanzoai/replicate conventions (REPLICATE_* env, snap/ + inc/
// prefixes, .zap.age suffix) so the same bucket + identity interoperate with
// the SQLite-side replicate engine — ZapDB just carries Badger Backup()/Load()
// streams instead of LTX files.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/s3" // register the s3:// opener
	"github.com/klauspost/compress/zstd"
	"github.com/luxfi/age"
	log "github.com/luxfi/log"
	zdb "github.com/luxfi/zapdb"
	"github.com/luxfi/zapdb/pb"
)

// zstd decode codec for restore — DecodeAll is safe for concurrent use. The
// encode side streams through a pooled Encoder in backupAndUpload (so the full
// uncompressed backup never materializes); see there. The Backup() stream is
// uncompressed protobuf while the on-disk LSM is Snappy, so compressing on the
// wire keeps S3/bandwidth cost near on-disk efficiency.
var zstdDecoder, _ = zstd.NewReader(nil)

func zstdDecompress(b []byte) ([]byte, error) { return zstdDecoder.DecodeAll(b, nil) }

// Pools keep the backup hot path off the allocator across ticks: one output
// buffer and one zstd encoder per concurrent backup, reused via Reset.
var (
	backupBufPool  = sync.Pool{New: func() any { return new(bytes.Buffer) }}
	zstdWriterPool = sync.Pool{New: func() any {
		w, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
		return w
	}}
)

// maxPooledBuf bounds what we return to the pool so a single huge snapshot
// doesn't pin hundreds of MB for the life of the process.
const maxPooledBuf = 64 << 20

// countWriter tallies bytes forwarded (the uncompressed backup size, for the
// compression-ratio log line) without a second pass over the data.
type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// vfsReplicator continuously backs up a *zdb.DB to an object store: a full
// snapshot on a slow tick plus incrementals on a fast tick, age-encrypted.
type vfsReplicator struct {
	db        *zdb.DB
	dbPath    string // on-disk LSM dir, for physical (file-copy) snapshots
	be        backend.Backend
	prefix    string        // key prefix, e.g. "testnet/zaprepl"
	recipient age.Recipient // encrypt target; nil → plaintext
	identity  age.Identity  // decrypt key for restore; nil → no restore-decrypt
	compress  bool          // zstd-compress the backup stream before upload
	physical  bool          // snapshots copy SST files instead of iterating keys

	snapEvery   time.Duration
	incEvery    time.Duration
	snapMinIncs int // min incrementals to accumulate before a full snapshot is worth taking
	maxPending  int
	cdc         bool // ship incrementals from the change feed instead of scanning

	mu              sync.Mutex
	sinceVersion    uint64
	lastSnapVersion uint64 // DB MaxVersion captured by the last full snapshot
	incsSinceSnap   int    // incrementals uploaded since the last full snapshot
	lastSnapshot    time.Time
	stop            context.CancelFunc

	cdcMu  sync.Mutex
	cdcBuf []*pb.KV // changes accumulated from db.Subscribe since the last drain
}

// vfsReplicatorConfig is the resolved REPLICATE_* configuration.
type vfsReplicatorConfig struct {
	Endpoint    string // full URL incl. scheme, e.g. http://s3.lux-system:9000
	Bucket      string
	Region      string
	Path        string
	AccessKey   string
	SecretKey   string
	Recipient   age.Recipient
	Identity    age.Identity
	Compress    bool // zstd-compress backups (default true via newVFSReplicator)
	SnapEvery   time.Duration
	IncEvery    time.Duration
	SnapMinIncs int // min incrementals before a snapshot; 0 → default
	MaxPending  int
	DBPath      string // on-disk LSM dir (for physical snapshots)
	Physical    bool   // take physical (SST-copy) snapshots instead of logical
	CDC         bool   // ship incrementals from the change feed (Subscribe), not a scan
}

// backendURL builds the vfs s3:// URL (creds + endpoint + region in the query)
// from the resolved config. Shared by the replicator and the pre-open physical
// hydrate path so both reach the same bucket the same way.
func backendURL(cfg vfsReplicatorConfig) string {
	q := url.Values{}
	if cfg.Region != "" {
		q.Set("region", cfg.Region)
	}
	if cfg.Endpoint != "" {
		q.Set("endpoint", cfg.Endpoint) // presence also forces path-style in vfs
	}
	if cfg.AccessKey != "" {
		q.Set("access_key", cfg.AccessKey)
	}
	if cfg.SecretKey != "" {
		q.Set("secret_key", cfg.SecretKey)
	}
	u := url.URL{Scheme: "s3", Host: cfg.Bucket, RawQuery: q.Encode()}
	return u.String()
}

// newVFSReplicator opens the vfs S3 backend and returns a ready replicator.
func newVFSReplicator(ctx context.Context, db *zdb.DB, cfg vfsReplicatorConfig) (*vfsReplicator, error) {
	// Best-effort: create the bucket if it doesn't exist, so a fresh node (or a
	// fresh object store) can back up without an out-of-band provisioning step.
	// A pre-existing bucket (prod) just returns AlreadyOwned, which we ignore.
	ensureReplicationBucket(ctx, cfg)

	// Open at the bucket root; full object keys carry cfg.Path so List() prefixes
	// line up with the hanzo replicate layout.
	be, err := backend.Open(ctx, backendURL(cfg))
	if err != nil {
		return nil, fmt.Errorf("vfs backend open: %w", err)
	}

	snapEvery := cfg.SnapEvery
	if snapEvery <= 0 {
		snapEvery = time.Hour
	}
	incEvery := cfg.IncEvery
	if incEvery <= 0 {
		incEvery = time.Second
	}
	maxPending := cfg.MaxPending
	if maxPending <= 0 {
		maxPending = 16
	}
	snapMinIncs := cfg.SnapMinIncs
	if snapMinIncs <= 0 {
		// A full snapshot re-uploads the entire DB; only worth it once enough
		// incrementals have piled up that collapsing them meaningfully shortens
		// restore replay. Default: collapse ~64 incrementals per snapshot.
		snapMinIncs = 64
	}
	return &vfsReplicator{
		db:          db,
		dbPath:      cfg.DBPath,
		be:          be,
		prefix:      cfg.Path,
		recipient:   cfg.Recipient,
		identity:    cfg.Identity,
		compress:    cfg.Compress,
		physical:    cfg.Physical,
		cdc:         cfg.CDC,
		snapEvery:   snapEvery,
		incEvery:    incEvery,
		snapMinIncs: snapMinIncs,
		maxPending:  maxPending,
	}, nil
}

// ensureBucket best-effort-creates the replication bucket. Errors (already
// exists, or insufficient permission in a managed store) are logged, not fatal:
// the subsequent Put will surface a real failure if the bucket truly isn't usable.
func ensureReplicationBucket(ctx context.Context, cfg vfsReplicatorConfig) {
	if cfg.Bucket == "" {
		return
	}
	region := cfg.Region
	if region == "" {
		region = "us-central1"
	}
	opts := []func(*awsconfig.LoadOptions) error{awsconfig.WithRegion(region)}
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}
	acfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		log.Warn(fmt.Sprintf("[zapdb] replicate: bucket ensure (config): %v", err))
		return
	}
	cli := awss3.NewFromConfig(acfg, func(o *awss3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})
	if _, err := cli.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String(cfg.Bucket)}); err != nil {
		// Already-owned / already-exists is the common, fine case.
		log.Info(fmt.Sprintf("[zapdb] replicate: bucket %q ready (create note: %v)", cfg.Bucket, err))
		return
	}
	log.Info(fmt.Sprintf("[zapdb] replicate: created bucket %q", cfg.Bucket))
}

// ext builds the object suffix from the codec chain so restore can detect, from
// the key alone, whether to decompress and/or decrypt: .zap[.zst][.age].
func (r *vfsReplicator) ext() string {
	s := ".zap"
	if r.compress {
		s += ".zst"
	}
	if r.recipient != nil || r.identity != nil {
		s += ".age"
	}
	return s
}

func (r *vfsReplicator) incKey(version uint64) string {
	return path.Join(r.prefix, "inc", fmt.Sprintf("%020d%s", version, r.ext()))
}

// snapKey names a snapshot by the DB version it captures (zero-padded, like
// incrementals) so restore can read a snapshot's version from its key and skip
// incrementals the snapshot already subsumes. Lexical max key = newest snapshot.
func (r *vfsReplicator) snapKey(version uint64) string {
	return path.Join(r.prefix, "snap", fmt.Sprintf("%020d%s", version, r.ext()))
}

// Start runs the replication loop until ctx is cancelled or Stop is called.
func (r *vfsReplicator) Start(ctx context.Context) {
	ctx, r.stop = context.WithCancel(ctx)

	incremental := r.Incremental
	if r.cdc {
		// Tail the write path (the WAL) and ship deltas from it — O(change), not a
		// keyspace scan. Start the feed first, then one catch-up scan covers writes
		// from the last shipped version up to now; the overlap with buffered changes
		// is idempotent (Load is version-keyed). After that it's feed-only.
		go r.runChangeFeed(ctx)
		if err := r.Incremental(ctx); err != nil {
			log.Warn(fmt.Sprintf("[zapdb] replicate: cdc catch-up: %v", err))
		}
		incremental = r.incrementalCDC
	}

	incTicker := time.NewTicker(r.incEvery)
	defer incTicker.Stop()
	snapTicker := time.NewTicker(r.snapEvery)
	defer snapTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-incTicker.C:
			if err := incremental(ctx); err != nil {
				log.Warn(fmt.Sprintf("[zapdb] replicate: incremental: %v", err))
			}
		case <-snapTicker.C:
			if err := r.Snapshot(ctx); err != nil {
				log.Warn(fmt.Sprintf("[zapdb] replicate: snapshot: %v", err))
			}
		}
	}
}

// Stop cancels the replication loop and releases the backend.
func (r *vfsReplicator) Stop() {
	if r.stop != nil {
		r.stop()
	}
	if r.be != nil {
		_ = r.be.Close()
	}
}

// Incremental backs up all changes since the last replicated version.
func (r *vfsReplicator) Incremental(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.backupAndUpload(ctx, r.sinceVersion, false)
}

// Snapshot writes a full backup. With physical mode it copies the on-disk LSM
// files (zero per-key serialization); otherwise it logically iterates the DB.
func (r *vfsReplicator) Snapshot(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Same accumulation gate as logical snapshots: only collapse once enough
	// incrementals have piled up that a full re-baseline is worth it.
	if r.db.MaxVersion() <= r.lastSnapVersion || r.incsSinceSnap < r.snapMinIncs {
		return nil
	}
	var err error
	if r.physical {
		err = r.physicalSnapshot(ctx)
	} else {
		err = r.backupAndUpload(ctx, 0, true)
	}
	if err != nil {
		return err
	}
	r.lastSnapshot = time.Now()
	return nil
}

// backupAndUpload runs db.Backup(since), age-encrypts if configured, and PUTs
// the object. snapshot selects the key prefix. No-op when there are no changes.
func (r *vfsReplicator) backupAndUpload(ctx context.Context, since uint64, snapshot bool) error {
	// Cheap version probe (O(#tables), not O(#keys)) before the full Backup scan.
	// db.Backup(since) walks the entire keyspace to find versions > since; on an
	// idle DB that scan is pure waste and its cost grows with the DB, not with
	// churn. MaxVersion is monotonic across memtables+SSTs, so:
	//   - incremental: nothing newer than what we already shipped → skip the scan.
	//   - snapshot: DB unchanged since the last full snapshot → don't re-upload an
	//     identical full copy (the old loop re-PUT the whole DB every tick).
	cur := r.db.MaxVersion()
	if snapshot {
		// Skip unless the DB advanced AND enough incrementals have accumulated to
		// be worth collapsing. Re-baselining the whole DB to shave a handful of
		// tiny incrementals off restore replay is a net loss (a 200 KB full
		// re-upload to collapse 798-byte deltas). The first incremental is itself
		// a full backup (since=0), so restore never needs a snapshot to exist.
		if cur <= r.lastSnapVersion || r.incsSinceSnap < r.snapMinIncs {
			return nil
		}
	} else if cur <= since {
		return nil
	}

	// Stream the backup through the codec chain so the full uncompressed dump
	// never exists in memory at once: db.Backup → [zstd] → [age] → out. The s3
	// backend's Put takes []byte, so we land in a pooled buffer sized to the
	// COMPRESSED output (≈⅓–½ of uncompressed), not the raw dump plus copies.
	// (A backend implementing backend.Writer could stream straight to the wire
	// for O(1) memory — none does today, so we don't carry that dead branch.)
	out := backupBufPool.Get().(*bytes.Buffer)
	out.Reset()
	defer func() {
		if out.Cap() <= maxPooledBuf {
			backupBufPool.Put(out)
		}
	}()

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

	maxVersion, err := r.db.Backup(cnt, since)
	if err != nil {
		if zw != nil {
			_ = zw.Close()
			zstdWriterPool.Put(zw)
		}
		return fmt.Errorf("backup: %w", err)
	}
	// Flush DB-side inward: zstd first (push compressed bytes into age/out), then
	// age (finalize the ciphertext). Order matters — closing age first would seal
	// the stream before zstd's trailer arrives.
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
	// Backup(since) returns 0 when no key has a version > since — the gate above
	// makes that rare (only versions compacted away between probe and scan), but
	// guard anyway so we never upload an empty/no-op object.
	if maxVersion == 0 {
		return nil
	}

	var key string
	if snapshot {
		key = r.snapKey(maxVersion)
	} else {
		key = r.incKey(maxVersion)
	}
	if err := r.be.Put(ctx, key, out.Bytes()); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	// Badger Backup(since) streams versions strictly > since, so the next
	// incremental starts from maxVersion (exclusive) — no overlap.
	r.sinceVersion = maxVersion
	if snapshot {
		r.lastSnapVersion = maxVersion
		r.incsSinceSnap = 0
	} else {
		r.incsSinceSnap++
	}
	log.Info(fmt.Sprintf("[zapdb] replicate: uploaded %s (%d→%d bytes, version %d)", key, cnt.n, out.Len(), maxVersion))
	return nil
}

// Restore loads the latest snapshot, then applies newer incrementals in order.
// Safe to call only when the DB has no concurrent transactions (boot).
func (r *vfsReplicator) Restore(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var snapVersion uint64
	if r.physical {
		// A physical snapshot is hydrated into the LSM dir BEFORE Badger opens it
		// (zapdb.New → latestPhysicalInto), so the live DB already holds it. Its
		// version is the current MaxVersion; we only replay the logical
		// incrementals layered on top of it below.
		snapVersion = r.db.MaxVersion()
		r.sinceVersion = snapVersion
	} else {
		latestSnap, err := r.latestKey(ctx, path.Join(r.prefix, "snap")+"/")
		if err != nil {
			return fmt.Errorf("list snapshots: %w", err)
		}
		if latestSnap != "" {
			snapVersion = versionFromKey(latestSnap)
			log.Info(fmt.Sprintf("[zapdb] replicate: restoring snapshot %s", latestSnap))
			if err := r.downloadAndLoad(ctx, latestSnap); err != nil {
				return fmt.Errorf("restore snapshot: %w", err)
			}
			r.sinceVersion = snapVersion
		}
	}

	incs, err := r.listVersioned(ctx, path.Join(r.prefix, "inc")+"/")
	if err != nil {
		return fmt.Errorf("list incrementals: %w", err)
	}
	sort.Slice(incs, func(i, j int) bool { return incs[i].version < incs[j].version })
	for _, inc := range incs {
		// The snapshot already contains everything up to snapVersion; replaying
		// incrementals at or below it is redundant work (and re-loads superseded
		// versions). Apply only the tail the snapshot doesn't cover.
		if inc.version <= snapVersion {
			continue
		}
		log.Info(fmt.Sprintf("[zapdb] replicate: applying incremental %s", inc.key))
		if err := r.downloadAndLoad(ctx, inc.key); err != nil {
			return fmt.Errorf("restore incremental %s: %w", inc.key, err)
		}
		r.sinceVersion = inc.version
	}
	// The restored state already lives in S3 (snapshot + replayed incrementals),
	// so don't let the first snapshot tick re-upload the whole DB. The next
	// snapshot fires only once new writes push MaxVersion past this point.
	r.lastSnapVersion = r.sinceVersion
	log.Info(fmt.Sprintf("[zapdb] replicate: restore complete (sinceVersion=%d)", r.sinceVersion))
	return nil
}

func (r *vfsReplicator) downloadAndLoad(ctx context.Context, key string) error {
	data, err := r.be.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	// Reverse the codec chain by suffix: .zap[.zst][.age] → decrypt, decompress.
	if strings.HasSuffix(key, ".age") {
		if r.identity == nil {
			return fmt.Errorf("object %s is encrypted but no age identity configured", key)
		}
		dec, err := ageDecrypt(data, r.identity)
		if err != nil {
			return fmt.Errorf("decrypt %s: %w", key, err)
		}
		data = dec
	}
	if strings.HasSuffix(strings.TrimSuffix(key, ".age"), ".zst") {
		raw, err := zstdDecompress(data)
		if err != nil {
			return fmt.Errorf("decompress %s: %w", key, err)
		}
		data = raw
	}
	return r.db.Load(bytes.NewReader(data), r.maxPending)
}

// latestKey returns the lexicographically greatest key under prefix ("" if none).
// Keys are zero-padded fixed-width, so lexical max == newest.
func (r *vfsReplicator) latestKey(ctx context.Context, prefix string) (string, error) {
	latest := ""
	keysCh, errCh := r.be.List(ctx, prefix)
	for k := range keysCh {
		if k > latest {
			latest = k
		}
	}
	if err := <-errCh; err != nil {
		return "", err
	}
	return latest, nil
}

type versionedKey struct {
	key     string
	version uint64
}

func (r *vfsReplicator) listVersioned(ctx context.Context, prefix string) ([]versionedKey, error) {
	var out []versionedKey
	keysCh, errCh := r.be.List(ctx, prefix)
	for k := range keysCh {
		out = append(out, versionedKey{key: k, version: versionFromKey(k)})
	}
	if err := <-errCh; err != nil {
		return nil, err
	}
	return out, nil
}

// versionFromKey extracts the zero-padded version from an object key basename,
// stripping the codec suffix chain for any backup kind: logical (.zap[.zst][.age])
// or physical (.ztar[.zst][.age]).
func versionFromKey(key string) uint64 {
	base := path.Base(key)
	base = strings.TrimSuffix(base, ".age")
	base = strings.TrimSuffix(base, ".zst")
	base = strings.TrimSuffix(base, ".zap")
	base = strings.TrimSuffix(base, ".ztar")
	v, _ := strconv.ParseUint(base, 10, 64)
	return v
}

func ageDecrypt(ciphertext []byte, identity age.Identity) ([]byte, error) {
	rr, err := age.Decrypt(bytes.NewReader(ciphertext), identity)
	if err != nil {
		return nil, err
	}
	return io.ReadAll(rr)
}
