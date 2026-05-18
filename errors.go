// Copyright (C) 2019-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package database

import (
	"errors"
	"fmt"
)

// common errors
var (
	ErrClosed      = errors.New("closed")
	ErrNotFound    = errors.New("not found")
	ErrReadOnlyTxn = errors.New("write attempted in read-only transaction")
)

// ErrBackendDisabled is returned when a database backend is disabled at compile time
type ErrBackendDisabled struct {
	Backend string
}

func (e *ErrBackendDisabled) Error() string {
	return fmt.Sprintf("database backend %q disabled", e.Backend)
}

// NewErrBackendDisabled creates a new ErrBackendDisabled error
func NewErrBackendDisabled(backend string) error {
	return &ErrBackendDisabled{Backend: backend}
}
