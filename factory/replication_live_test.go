// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

// Live test of the node DB-open integration point: node.initDatabase calls
// factory.New(..., "all"), and startReplicationIfBase must auto-activate ZAP
// replication to S3 for that base DB. Skipped unless ZAP_LIVE_S3_ENDPOINT is set.
//
//	ZAP_LIVE_S3_ENDPOINT=http://127.0.0.1:9000 \
//	ZAP_LIVE_S3_KEY=luxadmin ZAP_LIVE_S3_SECRET=luxsecret123 \
//	go test ./factory/ -run TestLive -v

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/luxfi/database/zapdb"
)

func TestLiveFactoryBaseDBAutoReplicates(t *testing.T) {
	endpoint := os.Getenv("ZAP_LIVE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ZAP_LIVE_S3_ENDPOINT to run the live factory replication test")
	}
	key, secret, bucket := os.Getenv("ZAP_LIVE_S3_KEY"), os.Getenv("ZAP_LIVE_S3_SECRET"), "lux-snapshots"

	cli := s3Client(t, endpoint, key, secret)
	_, _ = cli.CreateBucket(context.Background(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)})

	// Exactly what node.initDatabase sets, pointed at the local server.
	for k, v := range map[string]string{
		"REPLICATE_S3_ENDPOINT":     endpoint,
		"REPLICATE_S3_USE_SSL":      "false",
		"REPLICATE_S3_BUCKET":       bucket,
		"REPLICATE_S3_REGION":       "us-central1",
		"REPLICATE_S3_ACCESS_KEY":   key,
		"REPLICATE_S3_SECRET_KEY":   secret,
		"REPLICATE_S3_PATH":         "live-factory/zaprepl",
		"REPLICATE_RESTORE_ON_BOOT": "false",
	} {
		t.Setenv(k, v)
	}

	// node.initDatabase: factory.New(name, path, readOnly, cfg, gatherer, log, ns, "all").
	db, err := New(zapdb.Name, t.TempDir(), false, nil, nil, nil, "all", "all")
	if err != nil {
		t.Fatalf("factory.New: %v", err)
	}
	defer db.Close()

	// Write across a few 1s incremental ticks.
	for i := 0; i < 30; i++ {
		if err := db.Put([]byte(string(rune('a'+i%26))+itoa(i)), []byte("v")); err != nil {
			t.Fatalf("put: %v", err)
		}
		time.Sleep(120 * time.Millisecond)
	}
	time.Sleep(1500 * time.Millisecond)

	// The base DB must have streamed objects to S3 with no explicit StartReplicator
	// call — proving factory.New("all") auto-activates replication.
	objs := listKeys(t, cli, bucket, "live-factory/zaprepl/inc/")
	if len(objs) == 0 {
		t.Fatal("no incremental objects in S3 — base DB did not auto-replicate")
	}
	t.Logf("base DB auto-replicated %d incremental object(s) via factory.New(\"all\"): %v", len(objs), objs)
}

// TestLiveFactoryPluginDoesNotReplicate guards the subprocess-collision fix:
// a VM-plugin DB (meterDBRegName="meterdb") must NOT replicate even with the
// same REPLICATE_* env present.
func TestLiveFactoryPluginDoesNotReplicate(t *testing.T) {
	endpoint := os.Getenv("ZAP_LIVE_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("set ZAP_LIVE_S3_ENDPOINT to run")
	}
	key, secret, bucket := os.Getenv("ZAP_LIVE_S3_KEY"), os.Getenv("ZAP_LIVE_S3_SECRET"), "lux-snapshots"
	cli := s3Client(t, endpoint, key, secret)
	_, _ = cli.CreateBucket(context.Background(), &awss3.CreateBucketInput{Bucket: aws.String(bucket)})

	for k, v := range map[string]string{
		"REPLICATE_S3_ENDPOINT": endpoint, "REPLICATE_S3_USE_SSL": "false",
		"REPLICATE_S3_BUCKET": bucket, "REPLICATE_S3_ACCESS_KEY": key,
		"REPLICATE_S3_SECRET_KEY": secret, "REPLICATE_S3_PATH": "live-plugin/zaprepl",
	} {
		t.Setenv(k, v)
	}
	db, err := New(zapdb.Name, t.TempDir(), false, nil, nil, nil, "meterdb", "meterdb")
	if err != nil {
		t.Fatalf("factory.New: %v", err)
	}
	defer db.Close()
	for i := 0; i < 10; i++ {
		_ = db.Put([]byte("k"+itoa(i)), []byte("v"))
	}
	time.Sleep(1500 * time.Millisecond)
	if objs := listKeys(t, cli, bucket, "live-plugin/zaprepl/"); len(objs) != 0 {
		t.Fatalf("plugin DB wrongly replicated: %v", objs)
	}
	t.Log("plugin DB (meterdb) correctly did NOT replicate")
}

func s3Client(t *testing.T, endpoint, key, secret string) *awss3.Client {
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

func listKeys(t *testing.T, cli *awss3.Client, bucket, prefix string) []string {
	t.Helper()
	out, err := cli.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix),
	})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var keys []string
	for _, o := range out.Contents {
		keys = append(keys, aws.ToString(o.Key))
	}
	return keys
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
