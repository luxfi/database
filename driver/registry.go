package driver

import (
	"fmt"
	"sync"

	"github.com/luxfi/geth/ethdb"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]Driver)
)

// Register registers a driver for a given name
func Register(name string, driver Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	
	if driver == nil {
		panic("db: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("db: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

// Open opens a database using the named driver
func Open(name, path string, cfg any) (ethdb.Database, error) {
	driversMu.RLock()
	driver, ok := drivers[name]
	driversMu.RUnlock()
	
	if !ok {
		return nil, fmt.Errorf("db: unknown driver %q (forgotten import?)", name)
	}
	
	return driver.Open(path, cfg)
}