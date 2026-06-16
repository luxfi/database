// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package factory

import (
	"os"
	"testing"

	"github.com/luxfi/database/memdb"
)

// startReplicationIfBase must be a pure pass-through unless this is the base DB
// (meterDBRegName=="all") AND replication is configured. This is the guard that
// keeps VM-plugin subprocesses — which pass "meterdb" and inherit the same
// REPLICATE_* env — from replicating their chain DB onto the base node's S3 path.
func TestStartReplicationGate(t *testing.T) {
	t.Setenv("REPLICATE_S3_ENDPOINT", "http://s3.invalid:9000")

	// Plugin DB name must never trigger replication, even with env set.
	db := memdb.New()
	if got := startReplicationIfBase(db, "meterdb"); got != db {
		t.Fatal("plugin DB (meterdb) must pass through untouched")
	}

	// memdb is not Replicatable, so even the base name is a safe no-op (the
	// UnwrapTo assertion fails and we return the db unchanged).
	if got := startReplicationIfBase(db, "all"); got != db {
		t.Fatal("base DB pass-through must return the same db")
	}
}

func TestStartReplicationDisabledWithoutEnv(t *testing.T) {
	os.Unsetenv("REPLICATE_S3_ENDPOINT")
	db := memdb.New()
	if got := startReplicationIfBase(db, "all"); got != db {
		t.Fatal("no REPLICATE_S3_ENDPOINT must be a pure pass-through")
	}
}
