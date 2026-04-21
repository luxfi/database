// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/luxfi/metric"
	"github.com/stretchr/testify/require"
)

func TestOpenWithStaleMemFiles(t *testing.T) {
	dir := t.TempDir()

	// Open and close cleanly to create a valid database.
	db, err := New(dir, nil, "test", metric.NewRegistry())
	require.NoError(t, err)
	require.NoError(t, db.Put([]byte("k"), []byte("v")))
	require.NoError(t, db.Close())

	// Plant stale .mem files (simulates unclean shutdown).
	dbDir := filepath.Join(dir, "db")
	// The db subdirectory may or may not exist depending on zapdb internals;
	// create it if needed so the stale files land in the right place.
	_ = os.MkdirAll(dbDir, 0o755)

	// Also plant them in the root dir (BadgerDB stores memtables there).
	for _, name := range []string{"00099.mem", "00100.mem"} {
		require.NoError(t, os.WriteFile(filepath.Join(dir, name), []byte("stale"), 0o644))
	}

	// Re-open — should succeed after auto-cleaning stale files.
	db2, err := New(dir, nil, "test", metric.NewRegistry())
	require.NoError(t, err, "New() should recover from stale .mem files")
	t.Cleanup(func() { _ = db2.Close() })

	// Verify data survived.
	val, err := db2.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v"), val)
}

func TestRemoveStaleMemFiles(t *testing.T) {
	dir := t.TempDir()

	// Create some .mem files and a non-.mem file.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00001.mem"), []byte{}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00002.mem"), []byte{}, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "MANIFEST"), []byte("keep"), 0o644))

	require.NoError(t, removeStaleMemFiles(dir))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "MANIFEST", entries[0].Name())
}

func TestRemoveStaleMemFilesNonExistentDir(t *testing.T) {
	require.NoError(t, removeStaleMemFiles("/nonexistent/path/that/does/not/exist"))
}
