# Lux Database Package

[![Go Reference](https://pkg.go.dev/badge/github.com/luxfi/database.svg)](https://pkg.go.dev/github.com/luxfi/database)
[![Go Report Card](https://goreportcard.com/badge/github.com/luxfi/database)](https://goreportcard.com/report/github.com/luxfi/database)

## Overview

The `database` package provides a unified interface for key-value storage with multiple backend implementations. It's designed for high-performance blockchain applications requiring reliable, efficient data persistence.

## Features

- **Unified Interface**: Common API across all database backends
- **Multiple Backends**: LevelDB, BadgerDB, PebbleDB, and in-memory implementations
- **Atomic Operations**: Batch writes with ACID guarantees
- **Iterator Support**: Efficient range queries and prefix scans
- **Nested Databases**: Create isolated sub-databases with prefixes
- **Metrics & Monitoring**: Prometheus metrics integration
- **Thread-Safe**: Safe for concurrent use

## Installation

```bash
go get github.com/luxfi/database
```

## Supported Backends

### BadgerDB (Default)
High-performance, embeddable key-value store with excellent write performance.

```go
db, err := badgerdb.New(dbPath, nil, log)
```

### LevelDB
Google's LevelDB - mature and stable with good all-around performance.

```go
db, err := leveldb.New(dbPath, nil, log)
```

### PebbleDB
CockroachDB's fork of RocksDB - optimized for SSDs with excellent performance.

```go
db, err := pebbledb.New(dbPath, nil, log)
```

### MemDB
In-memory implementation for testing and temporary storage.

```go
db := memdb.New()
```

## Usage

### Basic Operations

```go
import (
    "github.com/luxfi/database"
    "github.com/luxfi/database/badgerdb"
)

// Open a database
db, err := badgerdb.New("/path/to/db", nil, log)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Put a key-value pair
err = db.Put([]byte("key"), []byte("value"))

// Get a value
value, err := db.Get([]byte("key"))
if err == database.ErrNotFound {
    // Key doesn't exist
}

// Delete a key
err = db.Delete([]byte("key"))

// Check if key exists
exists, err := db.Has([]byte("key"))
```

### Batch Operations

```go
// Create a batch for atomic writes
batch := db.NewBatch()

// Add operations to the batch
batch.Put([]byte("key1"), []byte("value1"))
batch.Put([]byte("key2"), []byte("value2"))
batch.Delete([]byte("key3"))

// Write atomically
err = batch.Write()

// Reset and reuse
batch.Reset()
```

### Iterators

```go
// Iterate over all keys
iter := db.NewIterator()
defer iter.Release()

for iter.Next() {
    key := iter.Key()
    value := iter.Value()
    // Process key-value pair
}

if err := iter.Error(); err != nil {
    log.Fatal(err)
}

// Iterate with prefix
iter = db.NewIteratorWithPrefix([]byte("prefix:"))
defer iter.Release()

// Iterate from a starting point
iter = db.NewIteratorWithStart([]byte("start"))
defer iter.Release()
```

### Prefix Databases

```go
// Create an isolated sub-database
userDB := database.NewPrefixDB(db, []byte("users:"))

// All operations are automatically prefixed
userDB.Put([]byte("alice"), []byte("data"))
// Actually stores: "users:alice" -> "data"

// Nesting is supported
adminDB := database.NewPrefixDB(userDB, []byte("admin:"))
// Operations use combined prefix: "users:admin:"
```

## Performance Considerations

### Backend Selection

- **BadgerDB**: Best for write-heavy workloads, LSM-tree based
- **LevelDB**: Balanced performance, mature and stable
- **PebbleDB**: Optimized for SSDs, excellent for large datasets
- **MemDB**: Fastest but volatile, use for testing only

### Best Practices

1. **Batch Writes**: Group multiple operations for better performance
2. **Iterator Management**: Always release iterators to free resources
3. **Key Design**: Use efficient key schemas for range queries
4. **Compaction**: Configure automatic compaction for optimal performance
5. **Memory Usage**: Monitor and tune cache sizes based on workload

## Configuration

### BadgerDB Options

```go
config := badgerdb.DefaultConfig
config.CacheSize = 100 * 1024 * 1024  // 100MB cache
config.LogLevel = "info"

db, err := badgerdb.NewWithConfig(dbPath, config, log)
```

### Metrics

Enable Prometheus metrics:

```go
import (
    "github.com/prometheus/client_golang/prometheus"
)

registry := prometheus.NewRegistry()
db, err := badgerdb.New(dbPath, registry, log)
```

## Error Handling

Common errors:

```go
switch err {
case database.ErrNotFound:
    // Key doesn't exist
case database.ErrClosed:
    // Database is closed
case nil:
    // Success
default:
    // Other error
}
```

## Testing

The package includes comprehensive tests:

```bash
# Run all tests
go test ./...

# Run benchmarks
go test -bench=. ./...

# Test specific backend
go test ./badgerdb
```

## Benchmarks

Performance comparison across backends (example results):

```
BenchmarkPut/badgerdb-8      50000    30125 ns/op
BenchmarkPut/leveldb-8       30000    45231 ns/op
BenchmarkPut/pebbledb-8      50000    32456 ns/op
BenchmarkPut/memdb-8        500000     3254 ns/op

BenchmarkGet/badgerdb-8     100000    15234 ns/op
BenchmarkGet/leveldb-8      100000    12456 ns/op
BenchmarkGet/pebbledb-8     100000    14325 ns/op
BenchmarkGet/memdb-8       1000000     1234 ns/op
```

## Migration Guide

Migrating between backends:

```go
import (
    "github.com/luxfi/database/migration"
)

// Migrate from LevelDB to BadgerDB
err := migration.Migrate(sourceDB, targetDB, migration.Options{
    BatchSize: 1000,
    OnProgress: func(count int) {
        log.Printf("Migrated %d keys", count)
    },
})
```

## Contributing

We welcome contributions! Please see our [Contributing Guidelines](../CONTRIBUTING.md).

## License

This project is licensed under the BSD 3-Clause License. See the [LICENSE](../LICENSE) file for details.

## Support

- [Documentation](https://docs.lux.network)
- [GitHub Issues](https://github.com/luxfi/database/issues)
- [Discord Community](https://discord.gg/luxnetwork)