// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package database

import (
	"errors"
	"fmt"
)

// common errors
var (
	ErrClosed   = errors.New("closed")
	ErrNotFound = errors.New("not found")
)

// ErrBackendDisabled is returned when a database backend is not compiled in
type ErrBackendDisabled struct {
	Backend string
}

func (e *ErrBackendDisabled) Error() string {
	return fmt.Sprintf("%s support not compiled in. Build with -tags %s", e.Backend, e.Backend)
}

func NewErrBackendDisabled(backend string) error {
	return &ErrBackendDisabled{Backend: backend}
}
