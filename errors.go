// Copyright (C) 2019-2025, Lux Partners Limited. All rights reserved.
// See the file LICENSE for licensing terms.

package db

import "errors"

// Common database errors.
var (
	ErrClosed   = errors.New("database closed")
	ErrNotFound = errors.New("not found")
)