// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package rpcdb

import (
	"github.com/luxfi/db"

	rpcdbpb "github.com/luxfi/db/proto/pb/rpcdb"
)

var (
	ErrEnumToError = map[rpcdbpb.Error]error{
		rpcdbpb.Error_ERROR_CLOSED:    db.ErrClosed,
		rpcdbpb.Error_ERROR_NOT_FOUND: db.ErrNotFound,
	}
	ErrorToErrEnum = map[error]rpcdbpb.Error{
		db.ErrClosed:   rpcdbpb.Error_ERROR_CLOSED,
		db.ErrNotFound: rpcdbpb.Error_ERROR_NOT_FOUND,
	}
)

func ErrorToRPCError(err error) error {
	if _, ok := ErrorToErrEnum[err]; ok {
		return nil
	}
	return err
}
