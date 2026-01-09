// Package database provides memory management utilities inspired by VictoriaMetrics
package database

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/luxfi/metric"
)

// MemoryManager provides memory management similar to VictoriaMetrics
type MemoryManager struct {
	allowedMemory   int64 // Total allowed memory in bytes
	usedMemory      int64 // Currently used memory
	maxMemory       uint64 // Max memory based on system
	allowedPercent  float64
	memoryMu        sync.RWMutex
	reserveChan     chan struct{}
	memoryReserve   int64 // Amount of memory reserved for critical operations
	usageGauge      *metric.OptimizedGauge
	availableGauge  *metric.OptimizedGauge
	remainingGauge  *metric.OptimizedGauge
}

// NewMemoryManager creates a new memory manager with the specified percentage of system memory
func NewMemoryManager(allowedPercent float64, reg *metric.MetricsRegistry) (*MemoryManager, error) {
	if allowedPercent <= 0 || allowedPercent > 100 {
		return nil, fmt.Errorf("allowedPercent must be between 0 and 100, got %f", allowedPercent)
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	systemTotalMemory := m.Sys

	allowedMemory := int64(float64(systemTotalMemory) * allowedPercent / 100.0)

	mm := &MemoryManager{
		allowedMemory:  allowedMemory,
		maxMemory:      systemTotalMemory,
		allowedPercent: allowedPercent,
		memoryReserve:  allowedMemory / 10, // Reserve 10% for critical operations
		reserveChan:    make(chan struct{}, 1),
	}

	// Initialize metrics
	if reg != nil {
		mm.usageGauge = metric.NewOptimizedGauge("memory_manager_used_bytes", "Amount of memory currently used by the memory manager")
		mm.availableGauge = metric.NewOptimizedGauge("memory_manager_available_bytes", "Amount of memory available for allocation")
		mm.remainingGauge = metric.NewOptimizedGauge("memory_manager_remaining_bytes", "Amount of memory remaining after current usage")

		reg.RegisterGauge("memory_used", mm.usageGauge)
		reg.RegisterGauge("memory_available", mm.availableGauge)
		reg.RegisterGauge("memory_remaining", mm.remainingGauge)
	}

	// Initialize reserve channel
	mm.reserveChan <- struct{}{}

	return mm, nil
}

// Allowed returns the amount of memory allowed for use
func (mm *MemoryManager) Allowed() int64 {
	return mm.allowedMemory
}

// Used returns the amount of memory currently in use
func (mm *MemoryManager) Used() int64 {
	return atomic.LoadInt64(&mm.usedMemory)
}

// Available returns the amount of memory available for allocation
func (mm *MemoryManager) Available() int64 {
	used := atomic.LoadInt64(&mm.usedMemory)
	return mm.allowedMemory - used
}

// TryReserve attempts to reserve memory without blocking
// Returns true if successful, false otherwise
func (mm *MemoryManager) TryReserve(size int64) bool {
	mm.memoryMu.Lock()
	defer mm.memoryMu.Unlock()

	currentUsed := atomic.LoadInt64(&mm.usedMemory)
	newUsed := currentUsed + size

	if newUsed > mm.allowedMemory {
		return false
	}

	atomic.AddInt64(&mm.usedMemory, size)

	if mm.usageGauge != nil {
		mm.usageGauge.Set(float64(atomic.LoadInt64(&mm.usedMemory)))
	}
	if mm.availableGauge != nil {
		mm.availableGauge.Set(float64(mm.Available()))
	}
	if mm.remainingGauge != nil {
		mm.remainingGauge.Set(float64(mm.Available()))
	}

	return true
}

// Reserve reserves memory, blocking until it's available
func (mm *MemoryManager) Reserve(size int64) {
	for !mm.TryReserve(size) {
		// Small sleep to avoid busy waiting
		runtime.Gosched()
	}
}

// Release releases previously reserved memory
func (mm *MemoryManager) Release(size int64) {
	atomic.AddInt64(&mm.usedMemory, -size)

	if mm.usageGauge != nil {
		mm.usageGauge.Set(float64(atomic.LoadInt64(&mm.usedMemory)))
	}
	if mm.availableGauge != nil {
		mm.availableGauge.Set(float64(mm.Available()))
	}
	if mm.remainingGauge != nil {
		mm.remainingGauge.Set(float64(mm.Available()))
	}
}

// WithReserve executes a function with reserved memory
func (mm *MemoryManager) WithReserve(size int64, fn func() error) error {
	mm.Reserve(size)
	defer mm.Release(size)
	return fn()
}

// ReserveCritical reserves memory from the critical reserve pool
// This is for operations that must not fail due to memory limits
func (mm *MemoryManager) ReserveCritical(size int64) {
	// For critical operations, we allow exceeding the normal limit
	// up to the critical reserve
	select {
	case <-mm.reserveChan:
		// We have access to critical reserve
		atomic.AddInt64(&mm.usedMemory, size)
	case <-mm.reserveChan:
		// Double-check and release if we didn't actually need it
		// This is a safeguard to ensure the channel stays populated
		if len(mm.reserveChan) == 0 {
			mm.reserveChan <- struct{}{}
		}
		// Fall back to normal reservation
		mm.Reserve(size)
	}
}

// ReleaseCritical releases memory from the critical reserve pool
func (mm *MemoryManager) ReleaseCritical(size int64) {
	atomic.AddInt64(&mm.usedMemory, -size)
	// Restore the critical reserve token if needed
	if len(mm.reserveChan) == 0 {
		mm.reserveChan <- struct{}{}
	}
}

// GetMemoryStats returns current memory statistics
func (mm *MemoryManager) GetMemoryStats() MemoryStats {
	return MemoryStats{
		Allowed:   mm.Allowed(),
		Used:      mm.Used(),
		Available: mm.Available(),
		System:    mm.maxMemory,
		Percent:   mm.allowedPercent,
	}
}

// MemoryStats holds memory statistics
type MemoryStats struct {
	Allowed   int64   // Allowed memory in bytes
	Used      int64   // Used memory in bytes
	Available int64   // Available memory in bytes
	System    uint64  // Total system memory in bytes
	Percent   float64 // Percentage of system memory allowed
}

// ObjectPool provides an optimized object pool similar to VictoriaMetrics
type ObjectPool[T any] struct {
	pool *sync.Pool
	size int64 // Approximate size of objects in the pool
	mm   *MemoryManager
	// Metrics
	acquireCounter *metric.OptimizedCounter
	releaseCounter *metric.OptimizedCounter
	poolSizeGauge  *metric.OptimizedGauge
	poolSize       int64
	poolMu         sync.RWMutex
}

// NewObjectPool creates a new object pool with memory management
func NewObjectPool[T any](newFn func() T, size int64, mm *MemoryManager, reg *metric.MetricsRegistry) *ObjectPool[T] {
	op := &ObjectPool[T]{
		pool: &sync.Pool{
			New: func() interface{} { return newFn() },
		},
		size: size,
		mm:   mm,
	}

	// Initialize metrics
	if reg != nil {
		op.acquireCounter = metric.NewOptimizedCounter("object_pool_acquire_total", "Total number of object acquisitions from pool")
		op.releaseCounter = metric.NewOptimizedCounter("object_pool_release_total", "Total number of object releases to pool")
		op.poolSizeGauge = metric.NewOptimizedGauge("object_pool_size", "Current size of object pool")

		reg.RegisterCounter("object_pool_acquire", op.acquireCounter)
		reg.RegisterCounter("object_pool_release", op.releaseCounter)
		reg.RegisterGauge("object_pool_size", op.poolSizeGauge)
	}

	return op
}

// Get acquires an object from the pool
func (op *ObjectPool[T]) Get() T {
	if op.acquireCounter != nil {
		op.acquireCounter.Inc()
	}

	obj := op.pool.Get().(T)

	// Update pool size metrics
	op.poolMu.Lock()
	op.poolSize--
	if op.poolSize < 0 {
		op.poolSize = 0
	}
	op.poolMu.Unlock()

	if op.poolSizeGauge != nil {
		op.poolSizeGauge.Set(float64(op.getPoolSize()))
	}

	return obj
}

// Put returns an object to the pool
func (op *ObjectPool[T]) Put(obj T) {
	if op.releaseCounter != nil {
		op.releaseCounter.Inc()
	}

	// Check if we have enough memory before putting back
	if op.mm != nil && op.mm.Available() < op.size {
		// If memory is tight, don't put back in pool to free up memory
		return
	}

	op.pool.Put(obj)

	// Update pool size metrics
	op.poolMu.Lock()
	op.poolSize++
	op.poolMu.Unlock()

	if op.poolSizeGauge != nil {
		op.poolSizeGauge.Set(float64(op.getPoolSize()))
	}
}

// getPoolSize returns the current pool size (thread-safe)
func (op *ObjectPool[T]) getPoolSize() int64 {
	op.poolMu.RLock()
	defer op.poolMu.RUnlock()
	return op.poolSize
}

// GetPoolSize returns the approximate number of objects in the pool
func (op *ObjectPool[T]) GetPoolSize() int64 {
	return op.getPoolSize()
}

// Clear clears the object pool
func (op *ObjectPool[T]) Clear() {
	// Clear the underlying pool
	oldPool := op.pool
	op.pool = &sync.Pool{New: oldPool.New}
	
	op.poolMu.Lock()
	op.poolSize = 0
	op.poolMu.Unlock()
	
	if op.poolSizeGauge != nil {
		op.poolSizeGauge.Set(0)
	}
}