// Copyright (C) 2026, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"testing"

	zdb "github.com/luxfi/zapdb"
)

// TestSyncWritesConfigIsThreeValued pins the distinction the old plain-bool
// field could not express. SyncWrites was `bool`, so an absent key and an
// explicit `false` both arrived as false, and applyConfig could therefore only
// ever turn syncing ON. `"syncWrites": false` was accepted and silently
// ignored — the worst kind of configuration bug, because the operator has
// every reason to believe it took effect.
//
// This matters beyond tidiness: fsync-per-commit is the dominant cost of a
// metadata-heavy workload on network-attached storage, measured at roughly
// 10 ms per write, so whether this knob works decides an order of magnitude.
func TestSyncWritesConfigIsThreeValued(t *testing.T) {
	tru, fls := true, false

	for _, tc := range []struct {
		name string
		cfg  *Config
		want bool
	}{
		{"absent keeps the safe default", &Config{}, true},
		{"explicit true", &Config{SyncWrites: &tru}, true},
		{"explicit false is HONOURED", &Config{SyncWrites: &fls}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opts := zdb.DefaultOptions("")
			opts.SyncWrites = true // the built-in default applyConfig starts from
			applyConfig(&opts, tc.cfg)
			if opts.SyncWrites != tc.want {
				t.Fatalf("SyncWrites = %v, want %v", opts.SyncWrites, tc.want)
			}
		})
	}
}

// TestSyncWritesParsesFromJSON checks the wire form an operator actually
// writes, since the config reaches this package as JSON bytes and never as a
// Go literal.
func TestSyncWritesParsesFromJSON(t *testing.T) {
	for _, tc := range []struct {
		name string
		json string
		want *bool
	}{
		{"absent", `{}`, nil},
		{"false", `{"syncWrites": false}`, boolPtr(false)},
		{"true", `{"syncWrites": true}`, boolPtr(true)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseConfig([]byte(tc.json))
			if err != nil {
				t.Fatalf("parseConfig: %v", err)
			}
			switch {
			case tc.want == nil && cfg.SyncWrites != nil:
				t.Fatalf("SyncWrites = %v, want nil (unset)", *cfg.SyncWrites)
			case tc.want != nil && cfg.SyncWrites == nil:
				t.Fatalf("SyncWrites = nil, want %v", *tc.want)
			case tc.want != nil && *cfg.SyncWrites != *tc.want:
				t.Fatalf("SyncWrites = %v, want %v", *cfg.SyncWrites, *tc.want)
			}
		})
	}
}

func boolPtr(b bool) *bool { return &b }
