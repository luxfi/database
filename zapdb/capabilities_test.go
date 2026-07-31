// Copyright (C) 2020-2026, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build test

package zapdb

import (
	"testing"

	"github.com/luxfi/metric"
	"github.com/stretchr/testify/require"

	"github.com/luxfi/database"
)

// The property that makes a Sequence worth having over an in-memory counter:
// ids handed out after a reopen are strictly above every id handed out before
// it, WITHOUT re-reading the data. A counter rebuilt from "highest key seen"
// cannot promise that once the highest key has been consumed and deleted.
func TestSequenceSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	open := func() *Database {
		db, err := New(dir, nil, "test", metric.NewRegistry())
		require.NoError(t, err)
		return db
	}

	db := open()
	seqr, ok := any(db).(database.Sequencer)
	require.True(t, ok, "zapdb must implement database.Sequencer")

	seq, err := seqr.GetSequence([]byte("q/seq"), 100)
	require.NoError(t, err)

	// The sequence is 0-based: the first Next is 0, so compare pairwise rather
	// than against the zero value.
	var ids []uint64
	for range 5 {
		n, err := seq.Next()
		require.NoError(t, err)
		ids = append(ids, n)
	}
	for i := 1; i < len(ids); i++ {
		require.Greater(t, ids[i], ids[i-1], "ids must increase")
	}
	last := ids[len(ids)-1]
	// Release returns the unused band; without it the reserved ids are skipped.
	require.NoError(t, seq.Release())
	require.NoError(t, db.Close())

	db = open()
	defer db.Close()
	seq2, err := any(db).(database.Sequencer).GetSequence([]byte("q/seq"), 100)
	require.NoError(t, err)
	defer seq2.Release()

	n, err := seq2.Next()
	require.NoError(t, err)
	require.Greater(t, n, last, "an id was re-issued across a reopen")
}

// A closed database must refuse both capabilities rather than panic on a nil
// handle — same contract as every other method on this backend.
func TestCapabilitiesRefuseAfterClose(t *testing.T) {
	db, err := New(t.TempDir(), nil, "test", metric.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, db.Close())

	_, err = db.GetSequence([]byte("k"), 10)
	require.ErrorIs(t, err, database.ErrClosed)
	require.ErrorIs(t, db.DropPrefix([]byte("p")), database.ErrClosed)
}

// DropPrefix removes exactly its range and leaves neighbouring keys alone.
func TestDropPrefixIsScoped(t *testing.T) {
	db, err := New(t.TempDir(), nil, "test", metric.NewRegistry())
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.Put([]byte("q/i/1"), []byte("a")))
	require.NoError(t, db.Put([]byte("q/i/2"), []byte("b")))
	require.NoError(t, db.Put([]byte("q/u/1"), []byte("c")))
	require.NoError(t, db.Put([]byte("other"), []byte("d")))

	var dropper database.PrefixDropper = db
	require.NoError(t, dropper.DropPrefix([]byte("q/i/")))

	for _, gone := range [][]byte{[]byte("q/i/1"), []byte("q/i/2")} {
		has, err := db.Has(gone)
		require.NoError(t, err)
		require.False(t, has, "%s should have been dropped", gone)
	}
	for _, kept := range [][]byte{[]byte("q/u/1"), []byte("other")} {
		has, err := db.Has(kept)
		require.NoError(t, err)
		require.True(t, has, "%s should have survived", kept)
	}
}
