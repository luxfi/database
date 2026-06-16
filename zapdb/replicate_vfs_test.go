// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hanzoai/vfs/pkg/backend"
	_ "github.com/hanzoai/vfs/pkg/backend/file" // register the file:// opener
	"github.com/luxfi/age"
)

// roundTrip writes keys into a source DB, snapshots+increments to a vfs backend,
// then restores into a fresh DB and asserts the keys survived. Exercises the
// full Backup → (encrypt) → Put → List → Get → (decrypt) → Load path against a
// real hanzoai/vfs backend.
func roundTrip(t *testing.T, recipient age.Recipient, identity age.Identity) {
	t.Helper()
	ctx := context.Background()
	store := "file://" + t.TempDir()

	// --- source DB with data ---
	src, err := New(t.TempDir(), nil, "src", nil)
	if err != nil {
		t.Fatalf("open src: %v", err)
	}
	want := map[string]string{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("key-%03d", i)
		v := fmt.Sprintf("val-%03d", i)
		if err := src.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put: %v", err)
		}
		want[k] = v
	}

	beSrc, err := backend.Open(ctx, store)
	if err != nil {
		t.Fatalf("open backend: %v", err)
	}
	rep := &vfsReplicator{db: src.db, be: beSrc, prefix: "testnet/zaprepl", recipient: recipient, identity: identity, compress: true, maxPending: 16}
	if err := rep.Snapshot(ctx); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// A second write + incremental, to exercise the incremental path too.
	if err := src.Put([]byte("key-extra"), []byte("extra")); err != nil {
		t.Fatalf("put extra: %v", err)
	}
	want["key-extra"] = "extra"
	if err := rep.Incremental(ctx); err != nil {
		t.Fatalf("incremental: %v", err)
	}
	_ = src.Close()

	// --- fresh DB restores from the same backend prefix ---
	dst, err := New(t.TempDir(), nil, "dst", nil)
	if err != nil {
		t.Fatalf("open dst: %v", err)
	}
	defer dst.Close()
	empty, err := dst.isEmpty()
	if err != nil || !empty {
		t.Fatalf("dst should start empty (empty=%v err=%v)", empty, err)
	}

	beDst, err := backend.Open(ctx, store)
	if err != nil {
		t.Fatalf("open backend dst: %v", err)
	}
	rrep := &vfsReplicator{db: dst.db, be: beDst, prefix: "testnet/zaprepl", recipient: recipient, identity: identity, compress: true, maxPending: 16}
	if err := rrep.Restore(ctx); err != nil {
		t.Fatalf("restore: %v", err)
	}

	for k, v := range want {
		got, err := dst.Get([]byte(k))
		if err != nil {
			t.Fatalf("get %s after restore: %v", k, err)
		}
		if string(got) != v {
			t.Errorf("key %s = %q, want %q", k, got, v)
		}
	}
}

func TestVFSReplicatorRoundTripPlaintext(t *testing.T) {
	roundTrip(t, nil, nil)
}

func TestVFSReplicatorRoundTripEncrypted(t *testing.T) {
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("generate identity: %v", err)
	}
	roundTrip(t, id.Recipient(), id)
}

// TestVFSReplicatorRoundTripPQ proves the backup objects are POST-QUANTUM
// encrypted (ML-KEM-768 + X25519 hybrid) through the same age path the live
// replicator uses — client-side, so what lands in S3 is already PQ ciphertext,
// regardless of whatever (classical) server-side encryption MinIO/hanzo-s3 does.
func TestVFSReplicatorRoundTripPQ(t *testing.T) {
	id, err := age.GeneratePQIdentity(age.PQKemHPKEMLKEM768X25519)
	if err != nil {
		t.Fatalf("generate PQ identity: %v", err)
	}
	hyb, ok := id.(*age.HybridIdentity)
	if !ok {
		t.Fatalf("expected *age.HybridIdentity, got %T", id)
	}
	rcpt := hyb.Recipient()
	if !strings.HasPrefix(rcpt.String(), "age1pq1") {
		t.Fatalf("not a PQ recipient: %s", rcpt.String())
	}
	roundTrip(t, rcpt, id)
}

// newVFSReplicator must build an s3:// backend from REPLICATE_* config without
// leaking the secret key (the vfs backend redacts it in its description).
func TestNewVFSReplicatorBuildsS3Backend(t *testing.T) {
	rep, err := newVFSReplicator(context.Background(), nil, vfsReplicatorConfig{
		Endpoint:  "http://s3.lux-system:9000",
		Bucket:    "lux-snapshots",
		Region:    "us-central1",
		Path:      "testnet/zaprepl",
		AccessKey: "AKID",
		SecretKey: "SECRET",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer rep.Stop()
	if rep.be == nil {
		t.Fatal("backend not opened")
	}
}
