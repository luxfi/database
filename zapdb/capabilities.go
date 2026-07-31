// Copyright (C) 2020-2026, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"github.com/luxfi/database"
)

// Compile-time proof that this backend carries the optional capabilities, so
// dropping one is a build failure here rather than a failed type assertion in
// a consumer.
var (
	_ database.Sequencer     = (*Database)(nil)
	_ database.PrefixDropper = (*Database)(nil)
)

// GetSequence implements database.Sequencer.
//
// The returned *zapdb.Sequence already has Next() (uint64, error) and
// Release() error, so it satisfies database.Sequence as-is.
func (d *Database) GetSequence(key []byte, bandwidth uint64) (database.Sequence, error) {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return nil, database.ErrClosed
	}

	// Empty keys go through the same placeholder as every other operation, or a
	// sequence stored under "" would collide with the empty-key record.
	if len(key) == 0 {
		key = emptyKeyPlaceholder
	}

	seq, err := d.db.GetSequence(key, bandwidth)
	if err != nil {
		return nil, err
	}
	return seq, nil
}

// DropPrefix implements database.PrefixDropper.
func (d *Database) DropPrefix(prefixes ...[]byte) error {
	d.closeMu.RLock()
	defer d.closeMu.RUnlock()

	if d.closed {
		return database.ErrClosed
	}
	return d.db.DropPrefix(prefixes...)
}
