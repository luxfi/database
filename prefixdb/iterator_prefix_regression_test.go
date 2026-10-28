// Copyright (C) 2019-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package prefixdb

import (
	"errors"
	"testing"

	"github.com/luxfi/database"
	"github.com/luxfi/database/dbtest"
	"github.com/luxfi/database/memdb"
)

// TestIteratorPrefixOnlyWithInterveningSubPrefixes is the regression for the
// native D-Chain "0 fills" bug. A prefix-ONLY scan (nil start) over a prefixdb
// whose partition also holds keys under OTHER sub-prefixes that sort BEFORE the
// target prefix must still return exactly the target rows. The bug synthesized
// start = p.prefix for a nil start, seeking the underlying iterator to the FRONT
// of the partition (p.prefix sorts before p.prefix+prefix); a backing store that
// stops at the first key not matching the prefix after a start-seek (zapdb) then
// returned ZERO rows. dchain.rebuildBookFromDB iterates "order:<pool>" while the
// same partition holds asset:/balance:/locked:/meta: rows (all sorting before
// "order:"), so the order book folded EMPTY and every taker cross produced no
// fills.
func TestIteratorPrefixOnlyWithInterveningSubPrefixes(t *testing.T) {
	db := New([]byte("vm"), memdb.New())

	// Sub-prefixes that sort BEFORE "order:", plus the target rows.
	puts := map[string]string{
		"asset:a":        "1",
		"balance:b":      "2",
		"locked:c":       "3",
		"meta:height":    "4",
		"order:\x00\x01": "ask1",
		"order:\x00\x02": "ask2",
		"order:\x00\x03": "ask3",
	}
	for k, v := range puts {
		if err := db.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("put %q: %v", k, err)
		}
	}

	it := db.NewIteratorWithPrefix([]byte("order:"))
	defer it.Release()
	got := map[string]string{}
	for it.Next() {
		got[string(it.Key())] = string(it.Value())
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	want := map[string]string{
		"order:\x00\x01": "ask1",
		"order:\x00\x02": "ask2",
		"order:\x00\x03": "ask3",
	}
	if len(got) != len(want) {
		t.Fatalf("prefix-only scan returned %d rows, want %d: %v", len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("row %q = %q, want %q", k, got[k], v)
		}
	}
}

// TestIteratorNilStartDoesNotSynthesizePrefixStart pins the fix at the unit
// level: NewIteratorWithStartAndPrefix(nil, prefix) must pass a NIL start to the
// underlying store (begin at the prefix), never start = p.prefix. A recording
// backing store captures the start it received. A real (non-nil) start is still
// correctly prefixed.
func TestIteratorNilStartDoesNotSynthesizePrefixStart(t *testing.T) {
	rec := &startRecorder{Database: memdb.New()}
	db := New([]byte("vm"), rec)

	db.NewIteratorWithPrefix([]byte("order:")).Release()
	if rec.gotStart != nil {
		t.Fatalf("nil start was synthesized to %q; must stay nil", rec.gotStart)
	}

	rec.gotStart = []byte("sentinel")
	db.NewIteratorWithStartAndPrefix([]byte("order:\x00\x05"), []byte("order:")).Release()
	if want := "vmorder:\x00\x05"; string(rec.gotStart) != want {
		t.Fatalf("real start prefixed to %q, want %q", rec.gotStart, want)
	}
}

// startRecorder wraps a Database and records the start passed to
// NewIteratorWithStartAndPrefix.
type startRecorder struct {
	database.Database
	gotStart []byte
}

func (r *startRecorder) NewIteratorWithStartAndPrefix(start, prefix []byte) database.Iterator {
	r.gotStart = start
	return r.Database.NewIteratorWithStartAndPrefix(start, prefix)
}

// TestCloseReleasesViewWithoutClosingParent verifies issue #6: closing a
// prefixdb view marks only that view as closed while leaving the parent
// database and sibling prefixed views open.
func TestCloseReleasesViewWithoutClosingParent(t *testing.T) {
	parent := memdb.New()
	viewA := New([]byte("a:"), parent)
	viewB := New([]byte("b:"), parent)

	if err := viewA.Put([]byte("k"), []byte("va")); err != nil {
		t.Fatalf("viewA.Put: %v", err)
	}
	if err := viewB.Put([]byte("k"), []byte("vb")); err != nil {
		t.Fatalf("viewB.Put: %v", err)
	}

	if err := viewA.Close(); err != nil {
		t.Fatalf("viewA.Close: %v", err)
	}
	if err := viewA.Close(); !errors.Is(err, database.ErrClosed) {
		t.Fatalf("second viewA.Close = %v, want %v", err, database.ErrClosed)
	}
	if _, err := viewA.Get([]byte("k")); !errors.Is(err, database.ErrClosed) {
		t.Fatalf("viewA.Get after close = %v, want %v", err, database.ErrClosed)
	}

	// Sibling view and parent store must remain open.
	gotB, err := viewB.Get([]byte("k"))
	if err != nil {
		t.Fatalf("viewB.Get after viewA.Close: %v", err)
	}
	if string(gotB) != "vb" {
		t.Fatalf("viewB.Get = %q, want %q", gotB, "vb")
	}
	if err := parent.Put([]byte("ungraceful-shutdown"), []byte("0")); err != nil {
		t.Fatalf("parent.Put after viewA.Close: %v", err)
	}
	if err := parent.Delete([]byte("ungraceful-shutdown")); err != nil {
		t.Fatalf("parent.Delete after viewA.Close: %v", err)
	}
	if err := parent.Close(); err != nil {
		t.Fatalf("parent.Close: %v", err)
	}
}

func TestCloseLifecycleSuiteOnSharedParent(t *testing.T) {
	lifecycleTests := map[string]func(t *testing.T, db database.Database){
		"SimpleKeyValueClosed":      dbtest.TestSimpleKeyValueClosed,
		"NewBatchClosed":            dbtest.TestNewBatchClosed,
		"BatchPut":                  dbtest.TestBatchPut,
		"IteratorClosed":            dbtest.TestIteratorClosed,
		"IteratorError":             dbtest.TestIteratorError,
		"IteratorErrorAfterRelease": dbtest.TestIteratorErrorAfterRelease,
		"CompactNoPanic":            dbtest.TestCompactNoPanic,
	}
	for name, test := range lifecycleTests {
		t.Run(name, func(t *testing.T) {
			db := memdb.New()
			test(t, New([]byte("hello"), db))
			test(t, New([]byte("world"), db))
			test(t, New([]byte("wor"), New([]byte("ld1"), db)))
			test(t, New([]byte("ld1"), New([]byte("wor"), db)))
			test(t, NewNested([]byte("wor"), New([]byte("ld2"), db)))
			test(t, NewNested([]byte("ld2"), New([]byte("wor"), db)))
		})
	}
}
