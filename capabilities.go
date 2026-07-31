// Copyright (C) 2020-2026, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package database

// Optional capabilities.
//
// Database stays the floor — the set every backend can honour, so memdb,
// leveldb, pebbledb and the wrappers keep implementing it unchanged. Anything a
// backend CAN do but not all of them can is its own small interface, reached by
// type assertion:
//
//	if s, ok := db.(database.Sequencer); ok { ... }
//
// The alternative is widening Database, which forces every backend to grow a
// method it cannot honour and pushes the failure to runtime. This way a
// consumer that needs more asks for exactly that, at compile time, and a
// backend that cannot provide it simply does not assert.

// Sequence hands out monotonically increasing uint64 ids that survive a
// restart. It is the crash-safe alternative to an in-memory counter: a counter
// re-initialised from "highest key seen" can re-issue an id that is already on
// disk after an unclean shutdown, silently overwriting the record stored under
// it. A Sequence reserves a band on disk before handing any id out, so the ids
// after a restart are always above the ids before it.
//
// Release returns the unused remainder of the reserved band. Not calling it
// leaks that band — the ids are skipped, never reused — so it is a waste, not a
// correctness problem.
type Sequence interface {
	// Next returns the next id in the sequence.
	Next() (uint64, error)

	// Release returns the unused remainder of the reserved band.
	Release() error
}

// Sequencer is implemented by backends that can persist a Sequence.
//
// bandwidth is how many ids are reserved on disk per refill: larger amortises
// the write across more Next calls, and costs that many skipped ids on an
// unclean shutdown.
type Sequencer interface {
	GetSequence(key []byte, bandwidth uint64) (Sequence, error)
}

// PrefixDropper is implemented by backends that can delete every key under a
// prefix as one operation, rather than iterating and deleting key by key.
//
// Worth asking for rather than emulating: the loop has to hold or re-open an
// iterator while mutating the range it is iterating, which is the shape that
// backends define least consistently.
type PrefixDropper interface {
	DropPrefix(prefixes ...[]byte) error
}
