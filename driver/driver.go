// Package driver provides a database driver interface for storage backends
package driver

import "github.com/luxfi/geth/ethdb"

// Driver interface for database backends
type Driver interface {
	Open(path string, cfg any) (ethdb.Database, error)
}