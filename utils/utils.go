// Copyright (C) 2019-2025, Lux Partners Limited. All rights reserved.
// See the file LICENSE for licensing terms.

package utils

import (
	"encoding/binary"
	"time"

	"github.com/luxfi/db"
	"github.com/luxfi/geth/common"
)

// PutID stores [id] -> [value]
func PutID(db db.KeyValueWriter, key []byte, val []byte) error {
	return db.Put(key, val)
}

// GetID reads [id] -> [value]
func GetID(db db.KeyValueReader, key []byte) ([]byte, error) {
	return db.Get(key)
}

// ParseID reads [id] -> [value]
func ParseID(bytes []byte) ([]byte, error) {
	return bytes, nil
}

// PutUInt64 stores [prefix] + [key] -> [value]
func PutUInt64(db db.KeyValueWriter, key []byte, val uint64) error {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, val)
	return db.Put(key, bytes)
}

// GetUInt64 reads [prefix] + [key] -> [value]
func GetUInt64(db db.KeyValueReader, key []byte) (uint64, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	return ParseUInt64(bytes)
}

// ParseUInt64 parses a uint64 from bytes
func ParseUInt64(bytes []byte) (uint64, error) {
	if len(bytes) != 8 {
		return 0, db.ErrNotFound
	}
	return binary.BigEndian.Uint64(bytes), nil
}

// PutUInt32 stores [prefix] + [key] -> [value]
func PutUInt32(db db.KeyValueWriter, key []byte, val uint32) error {
	bytes := make([]byte, 4)
	binary.BigEndian.PutUint32(bytes, val)
	return db.Put(key, bytes)
}

// GetUInt32 reads [prefix] + [key] -> [value]
func GetUInt32(db db.KeyValueReader, key []byte) (uint32, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return 0, err
	}
	return ParseUInt32(bytes)
}

// ParseUInt32 parses a uint32 from bytes
func ParseUInt32(bytes []byte) (uint32, error) {
	if len(bytes) != 4 {
		return 0, db.ErrNotFound
	}
	return binary.BigEndian.Uint32(bytes), nil
}

// PutTimestamp stores [prefix] + [key] -> [timestamp]
func PutTimestamp(db db.KeyValueWriter, key []byte, val time.Time) error {
	return PutUInt64(db, key, uint64(val.Unix()))
}

// GetTimestamp reads [prefix] + [key] -> [timestamp]
func GetTimestamp(db db.KeyValueReader, key []byte) (time.Time, error) {
	unix, err := GetUInt64(db, key)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(unix), 0), nil
}

// ParseTimestamp parses a timestamp from bytes
func ParseTimestamp(bytes []byte) (time.Time, error) {
	unix, err := ParseUInt64(bytes)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(unix), 0), nil
}

// PutBool stores [prefix] + [key] -> [value]
func PutBool(db db.KeyValueWriter, key []byte, val bool) error {
	if val {
		return db.Put(key, []byte{1})
	}
	return db.Put(key, []byte{0})
}

// GetBool reads [prefix] + [key] -> [value]
func GetBool(db db.KeyValueReader, key []byte) (bool, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return false, err
	}
	return ParseBool(bytes)
}

// ParseBool parses a bool from bytes
func ParseBool(bytes []byte) (bool, error) {
	if len(bytes) != 1 {
		return false, db.ErrNotFound
	}
	return bytes[0] != 0, nil
}

// PutHash stores [prefix] + [key] -> [hash]
func PutHash(db db.KeyValueWriter, key []byte, hash common.Hash) error {
	return db.Put(key, hash[:])
}

// GetHash reads [prefix] + [key] -> [hash]
func GetHash(db db.KeyValueReader, key []byte) (common.Hash, error) {
	bytes, err := db.Get(key)
	if err != nil {
		return common.Hash{}, err
	}
	return ParseHash(bytes)
}

// ParseHash parses a hash from bytes
func ParseHash(bytes []byte) (common.Hash, error) {
	if len(bytes) != common.HashLength {
		return common.Hash{}, db.ErrNotFound
	}
	return common.BytesToHash(bytes), nil
}

// Clear removes all keys with the given prefix
func Clear(db db.Database, prefix []byte) error {
	iter := db.NewIteratorWithPrefix(prefix)
	defer iter.Release()

	batch := db.NewBatch()
	for iter.Next() {
		if err := batch.Delete(iter.Key()); err != nil {
			return err
		}
	}
	return batch.Write()
}

// Count returns the number of keys with the given prefix
func Count(db db.Database, prefix []byte) (int, error) {
	iter := db.NewIteratorWithPrefix(prefix)
	defer iter.Release()

	count := 0
	for iter.Next() {
		count++
	}
	return count, iter.Error()
}