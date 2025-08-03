// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build !rocksdb
// +build !rocksdb

package leveldb

import (
	"github.com/luxfi/database"
	"github.com/syndtr/goleveldb/leveldb"
)

// updateError converts a leveldb-specific error to its Lux equivalent, if applicable.
func updateError(err error) error {
	switch err {
	case nil:
		return nil
	case leveldb.ErrClosed:
		return database.ErrClosed
	case leveldb.ErrNotFound:
		return database.ErrNotFound
	default:
		return err
	}
}
