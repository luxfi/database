// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"context"
	"testing"
)

// isEmpty gates restore-on-boot: we only pull a snapshot over a fresh DB so we
// never clobber newer local state.
func TestIsEmpty(t *testing.T) {
	db, err := New(t.TempDir(), nil, "test", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	empty, err := db.isEmpty()
	if err != nil {
		t.Fatalf("isEmpty: %v", err)
	}
	if !empty {
		t.Fatal("a fresh DB must report empty")
	}

	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}
	empty, err = db.isEmpty()
	if err != nil {
		t.Fatalf("isEmpty: %v", err)
	}
	if empty {
		t.Fatal("a DB with a key must not report empty")
	}
}

// StartReplicator is a no-op (no error, no goroutine, no restore) when
// replication is not configured.
func TestStartReplicatorUnconfigured(t *testing.T) {
	t.Setenv("REPLICATE_S3_ENDPOINT", "")

	db, err := New(t.TempDir(), nil, "test", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	if err := db.StartReplicator(context.Background()); err != nil {
		t.Fatalf("StartReplicator with no endpoint must be a no-op: %v", err)
	}
	if db.repl != nil {
		t.Fatal("no replicator should be created when unconfigured")
	}
}

// StartReplicator must be idempotent: the node and the DB factory both call it
// on the base DB, and a second activation would run a duplicate backup loop
// writing the same versions to the same S3 path.
func TestStartReplicatorIdempotent(t *testing.T) {
	// Bogus endpoint + restore disabled: the loop runs in the background and
	// fails uploads harmlessly; we only care that a second call is a no-op.
	t.Setenv("REPLICATE_S3_ENDPOINT", "http://127.0.0.1:1")
	t.Setenv("REPLICATE_S3_BUCKET", "b")
	t.Setenv("REPLICATE_S3_ACCESS_KEY", "k")
	t.Setenv("REPLICATE_S3_SECRET_KEY", "s")
	t.Setenv("REPLICATE_RESTORE_ON_BOOT", "false")

	db, err := New(t.TempDir(), nil, "test", nil)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := db.StartReplicator(ctx); err != nil {
		t.Fatalf("first StartReplicator: %v", err)
	}
	first := db.repl
	if first == nil {
		t.Fatal("first call must activate a replicator")
	}
	if err := db.StartReplicator(ctx); err != nil {
		t.Fatalf("second StartReplicator: %v", err)
	}
	if db.repl != first {
		t.Fatal("second StartReplicator must be a no-op (replicator was replaced)")
	}
}
