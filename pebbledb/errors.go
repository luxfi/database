// Copyright (C) 2019-2025, Lux Partners Limited. All rights reserved.
// See the file LICENSE for licensing terms.

//go:build pebbledb
// +build pebbledb

package pebbledb

import (
	"errors"

	"github.com/cockroachdb/pebble"
	database "github.com/luxfi/db"
)

var (
	errInvalidOperation = errors.New("invalid operation")
)

// updateError converts a pebble-specific error to its Lux equivalent, if applicable.
func updateError(err error) error {
	switch err {
	case pebble.ErrClosed:
		return database.ErrClosed
	case pebble.ErrNotFound:
		return database.ErrNotFound
	default:
		return err
	}
}