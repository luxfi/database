# database

## Overview

Go package for Lux blockchain - database

## Package Information

- **Type**: go
- **Module**: github.com/luxfi/database
- **Repository**: github.com/luxfi/database

## Directory Structure

```
.
bin
codec
codec/linear
databasemock
heightindexdb
heightindexdb/dbtest
heightindexdb/memdb
leveldb
linkeddb
memdb
pebbledb
prefixdb
state
state/syncapi
```

## Key Files

- batch.go
- database.go
- errors.go
- go.mod
- height_index_test.go
- helpers_test.go
- helpers.go
- iterator.go

## Development

### Prerequisites

- Go 1.21+

### Build

```bash
go build ./...
```

### Test

```bash
go test -v ./...
```

## Integration with Lux Ecosystem

This package is part of the Lux blockchain ecosystem. See the main documentation at:
- GitHub: https://github.com/luxfi
- Docs: https://docs.lux.network

## Codec Blob Cap

`codec/linear` enforces a per-Manager blob ceiling to bound allocations on
adversarial inputs while still admitting legitimate large values (FHE
bootstrap keys, state sync chunks, Verkle proofs).

- `DefaultMaxBlobSize = 256 MiB` — 4x headroom over largest known caller
  (FHE STD128 / PN10QP27 / PN11QP54 bootstrap keys ~50 MiB).
- Pass `maxSize > 0` to `linear.NewManager` to override; values are clamped
  to `MaxInt` (math.MaxInt32, ~2 GiB) since on-disk length prefixes are uint32.
- `maxSize <= 0` selects the default. Call sites that previously passed a
  value but were ignored (the parameter was a no-op) now actually take effect.
- `Marshal` and `Unmarshal` both enforce the cap and return
  `ErrMaxSliceLenExceeded` wrapped with the offending size.

Streaming write path for blobs > MaxInt is tracked as future work; today
we clamp to a single allocation.

---

*Auto-generated for AI assistants. Last updated: 2026-04-28*
