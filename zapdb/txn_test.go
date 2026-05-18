// Copyright (C) 2020-2026, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"errors"
	"testing"
	"time"

	"github.com/luxfi/database"
)

func newTestDB(t *testing.T) *Database {
	t.Helper()
	dir := t.TempDir()
	db, err := New(dir, nil, "test", nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestUpdateCommit(t *testing.T) {
	db := newTestDB(t)

	if err := db.Update(func(txn database.Txn) error {
		if err := txn.Put([]byte("k1"), []byte("v1")); err != nil {
			return err
		}
		if err := txn.Put([]byte("k2"), []byte("v2")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, err := db.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get k1: %v", err)
	}
	if string(got) != "v1" {
		t.Fatalf("k1 = %q want v1", got)
	}
}

func TestUpdateRollback(t *testing.T) {
	db := newTestDB(t)
	if err := db.Put([]byte("k"), []byte("before")); err != nil {
		t.Fatal(err)
	}

	want := errors.New("user error")
	err := db.Update(func(txn database.Txn) error {
		_ = txn.Put([]byte("k"), []byte("after"))
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("Update err = %v want %v", err, want)
	}

	got, err := db.Get([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "before" {
		t.Fatalf("k = %q want before (rolled back)", got)
	}
}

func TestViewReadOnly(t *testing.T) {
	db := newTestDB(t)
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}

	err := db.View(func(txn database.Txn) error {
		v, err := txn.Get([]byte("k"))
		if err != nil {
			return err
		}
		if string(v) != "v" {
			t.Fatalf("View Get: %q want v", v)
		}
		if err := txn.Put([]byte("k"), []byte("v2")); !errors.Is(err, database.ErrReadOnlyTxn) {
			t.Fatalf("Put in View err = %v want ErrReadOnlyTxn", err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("View: %v", err)
	}
}

func TestPutWithTTL(t *testing.T) {
	db := newTestDB(t)
	// Use 1 second TTL; verify present immediately then absent after expiry.
	if err := db.PutWithTTL([]byte("ephemeral"), []byte("x"), 2*time.Second); err != nil {
		t.Fatal(err)
	}

	got, err := db.Get([]byte("ephemeral"))
	if err != nil {
		t.Fatalf("immediate Get: %v", err)
	}
	if string(got) != "x" {
		t.Fatalf("immediate Get: %q want x", got)
	}

	time.Sleep(3 * time.Second)

	_, err = db.Get([]byte("ephemeral"))
	if !errors.Is(err, database.ErrNotFound) {
		t.Fatalf("post-TTL Get err = %v want ErrNotFound", err)
	}
}

func TestTxnPutWithTTL(t *testing.T) {
	db := newTestDB(t)

	if err := db.Update(func(txn database.Txn) error {
		return txn.PutWithTTL([]byte("k"), []byte("v"), 2*time.Second)
	}); err != nil {
		t.Fatal(err)
	}

	got, _ := db.Get([]byte("k"))
	if string(got) != "v" {
		t.Fatalf("k = %q want v", got)
	}

	time.Sleep(3 * time.Second)
	_, err := db.Get([]byte("k"))
	if !errors.Is(err, database.ErrNotFound) {
		t.Fatalf("expired Get err = %v want ErrNotFound", err)
	}
}

func TestTxnInterfaceAssertion(t *testing.T) {
	db := newTestDB(t)
	var _ database.Transactional = db
	var _ database.TTLWriter = db
}
