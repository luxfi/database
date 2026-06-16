// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

// Live streaming-replication test against a real S3 server (hanzoai/s3 / MinIO).
// Skipped unless ZAP_LIVE_S3_ENDPOINT is set, e.g.:
//
//	ZAP_LIVE_S3_ENDPOINT=http://127.0.0.1:9000 \
//	ZAP_LIVE_S3_KEY=luxadmin ZAP_LIVE_S3_SECRET=luxsecret123 \
//	go test ./zapdb/ -run TestLive -v
//
// Exercises the full path over the network: Badger Backup → age → vfs s3 PUT →
// hanzoai/s3, then List/GET → Load, plus the production StartReplicator loop and
// restore-on-boot.

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

func liveS3(t *testing.T) (endpoint, key, secret, bucket string) {
	t.Helper()
	endpoint = os.Getenv("ZAP_LIVE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ZAP_LIVE_S3_ENDPOINT to run the live S3 replication test")
	}
	key = os.Getenv("ZAP_LIVE_S3_KEY")
	secret = os.Getenv("ZAP_LIVE_S3_SECRET")
	bucket = "lux-snapshots"
	return
}

func ensureBucket(t *testing.T, endpoint, key, secret, bucket string) {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-central1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")),
	)
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	cli := awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
	_, err = cli.CreateBucket(context.Background(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)})
	if err != nil {
		// Already-exists is fine.
		t.Logf("create bucket %s (may already exist): %v", bucket, err)
	}
}

// TestLiveDeterministicRoundTrip: manual Snapshot + Incremental + Restore over a
// real S3 server, no timing dependence.
func TestLiveDeterministicRoundTrip(t *testing.T) {
	endpoint, key, secret, bucket := liveS3(t)
	ensureBucket(t, endpoint, key, secret, bucket)
	ctx := context.Background()

	src, err := New(t.TempDir(), nil, "src", nil)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	want := map[string]string{}
	for i := 0; i < 100; i++ {
		k, v := fmt.Sprintf("k-%04d", i), fmt.Sprintf("v-%04d", i)
		if err := src.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put: %v", err)
		}
		want[k] = v
	}

	rep, err := newVFSReplicator(ctx, src.db, vfsReplicatorConfig{
		Endpoint: endpoint, Bucket: bucket, Region: "us-central1",
		Path: "live-det/zaprepl", AccessKey: key, SecretKey: secret, Compress: true, MaxPending: 16,
	})
	if err != nil {
		t.Fatalf("new replicator: %v", err)
	}
	defer rep.Stop()
	if err := rep.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// More writes after the snapshot → captured by an incremental.
	for i := 100; i < 130; i++ {
		k, v := fmt.Sprintf("k-%04d", i), fmt.Sprintf("v-%04d", i)
		if err := src.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put2: %v", err)
		}
		want[k] = v
	}
	if err := rep.Incremental(ctx); err != nil {
		t.Fatalf("incremental: %v", err)
	}
	_ = src.Close()

	dst, err := New(t.TempDir(), nil, "dst", nil)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()
	rrep, err := newVFSReplicator(ctx, dst.db, vfsReplicatorConfig{
		Endpoint: endpoint, Bucket: bucket, Region: "us-central1",
		Path: "live-det/zaprepl", AccessKey: key, SecretKey: secret, Compress: true, MaxPending: 16,
	})
	if err != nil {
		t.Fatalf("new dst replicator: %v", err)
	}
	defer rrep.Stop()
	if err := rrep.Restore(ctx); err != nil {
		t.Fatalf("restore: %v", err)
	}
	verify(t, dst, want)
	t.Logf("deterministic round-trip OK: %d keys restored over live S3", len(want))
}

// TestLiveStreamingReplicator: the production StartReplicator loop streams
// incrementals to S3 while writes happen; a fresh DB restores on boot.
func TestLiveStreamingReplicator(t *testing.T) {
	endpoint, key, secret, bucket := liveS3(t)
	ensureBucket(t, endpoint, key, secret, bucket)

	env := map[string]string{
		"REPLICATE_S3_ENDPOINT":   endpoint,
		"REPLICATE_S3_USE_SSL":    "false",
		"REPLICATE_S3_BUCKET":     bucket,
		"REPLICATE_S3_REGION":     "us-central1",
		"REPLICATE_S3_ACCESS_KEY": key,
		"REPLICATE_S3_SECRET_KEY": secret,
		"REPLICATE_S3_PATH":       "live-stream/zaprepl",
	}
	for k, v := range env {
		t.Setenv(k, v)
	}

	// --- source: start the production replicator loop, write while it streams ---
	t.Setenv("REPLICATE_RESTORE_ON_BOOT", "false")
	src, err := New(t.TempDir(), nil, "src", nil)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := src.StartReplicator(ctx); err != nil {
		t.Fatalf("StartReplicator: %v", err)
	}
	if src.repl == nil {
		t.Fatal("replicator did not activate from env")
	}

	want := map[string]string{}
	// Write across several 1s incremental ticks.
	for i := 0; i < 40; i++ {
		k, v := fmt.Sprintf("s-%04d", i), fmt.Sprintf("v-%04d", i)
		if err := src.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put: %v", err)
		}
		want[k] = v
		time.Sleep(100 * time.Millisecond)
	}
	// Let the final incremental tick flush the last writes, then stop the loop.
	time.Sleep(1500 * time.Millisecond)
	cancel()
	src.repl.Stop()
	_ = src.Close()

	// --- destination: restore-on-boot from the streamed incrementals ---
	t.Setenv("REPLICATE_RESTORE_ON_BOOT", "true")
	dst, err := New(t.TempDir(), nil, "dst", nil)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := dst.StartReplicator(ctx2); err != nil {
		t.Fatalf("dst StartReplicator (restore-on-boot): %v", err)
	}
	if dst.repl != nil {
		dst.repl.Stop()
	}
	verify(t, dst, want)
	t.Logf("streaming round-trip OK: %d keys streamed + restored-on-boot over live S3", len(want))
}

// TestLiveIncrementalEfficiency quantifies how cheap incremental replication is
// vs full snapshots, using a controlled per-"block" write workload that mirrors
// how every Lux chain writes trie key/values into the shared base DB.
func TestLiveIncrementalEfficiency(t *testing.T) {
	endpoint, key, secret, bucket := liveS3(t)
	ensureBucket(t, endpoint, key, secret, bucket)
	ctx := context.Background()
	cli := effS3Client(t, endpoint, key, secret)

	src, err := New(t.TempDir(), nil, "src", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer src.Close()

	rep, err := newVFSReplicator(ctx, src.db, vfsReplicatorConfig{
		Endpoint: endpoint, Bucket: bucket, Region: "us-central1",
		Path: "eff-zstd/zaprepl", AccessKey: key, SecretKey: secret, Compress: true, MaxPending: 16,
	})
	if err != nil {
		t.Fatalf("replicator: %v", err)
	}
	defer rep.Stop()

	// "Genesis": pre-fill base state, then one full snapshot as the baseline.
	const baseKeys, valSz = 4000, 128
	writeBlock(t, src, "base-", baseKeys, valSz)
	if err := rep.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	snapBytes := sumObjects(t, ctx, cli, bucket, "eff-zstd/zaprepl/snap/")

	// Simulate blocks: each writes perBlock trie key/values, then one incremental.
	const blocks, perBlock = 40, 200
	dataPerBlock := perBlock * (valSz + 24) // ~key+value bytes mutated per block
	for b := 0; b < blocks; b++ {
		writeBlock(t, src, fmt.Sprintf("blk%05d-", b), perBlock, valSz)
		if err := rep.Incremental(ctx); err != nil {
			t.Fatalf("incremental %d: %v", b, err)
		}
	}
	incBytes := sumObjects(t, ctx, cli, bucket, "eff-zstd/zaprepl/inc/")
	incObjs := countObjects(t, ctx, cli, bucket, "eff-zstd/zaprepl/inc/")

	naiveFull := snapBytes * int64(blocks) // re-snapshotting every block
	t.Logf("full snapshot (baseline genesis state): %d KB", snapBytes/1024)
	t.Logf("workload: %d blocks x %d writes (~%d B mutated/block)", blocks, perBlock, dataPerBlock)
	t.Logf("incremental replication: %d objects, %d KB total (~%d B/block on the wire)",
		incObjs, incBytes/1024, incBytes/int64(blocks))
	if incBytes > 0 {
		t.Logf("EFFICIENCY: %d incrementals = %d KB vs re-snapshotting each block = %d KB  -> %.0fx less data",
			blocks, incBytes/1024, naiveFull/1024, float64(naiveFull)/float64(incBytes))
	}
}

func writeBlock(t *testing.T, db *Database, prefix string, n, valSz int) {
	t.Helper()
	val := make([]byte, valSz)
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("%s%06d", prefix, i))
		// vary value so it's a real write, not a no-op dedup
		val[0], val[1] = byte(i), byte(i>>8)
		if err := db.Put(k, val); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
}

func effS3Client(t *testing.T, endpoint, key, secret string) *awss3.Client {
	t.Helper()
	cfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-central1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(key, secret, "")))
	if err != nil {
		t.Fatalf("aws config: %v", err)
	}
	return awss3.NewFromConfig(cfg, func(o *awss3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})
}

func sumObjects(t *testing.T, ctx context.Context, cli *awss3.Client, bucket, prefix string) int64 {
	t.Helper()
	out, err := cli.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var total int64
	for _, o := range out.Contents {
		total += aws.ToInt64(o.Size)
	}
	return total
}

func countObjects(t *testing.T, ctx context.Context, cli *awss3.Client, bucket, prefix string) int {
	t.Helper()
	out, err := cli.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{Bucket: aws.String(bucket), Prefix: aws.String(prefix)})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	return len(out.Contents)
}

func verify(t *testing.T, db *Database, want map[string]string) {
	t.Helper()
	missing := 0
	for k, v := range want {
		got, err := db.Get([]byte(k))
		if err != nil {
			missing++
			continue
		}
		if string(got) != v {
			t.Errorf("key %s = %q, want %q", k, got, v)
		}
	}
	if missing > 0 {
		t.Fatalf("%d/%d keys missing after restore", missing, len(want))
	}
}
