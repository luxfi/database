// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import "encoding/json"

// GPUCacheConfig configures opt-in GPU-accelerated caching for ZapDB.
// When GPUMemoryBudget > 0, the database keeps hot key-value pairs in GPU
// memory for fast access. Reads check GPU first, writes go through to disk.
//
// Set GPUMemoryBudget to 0 (default) to disable GPU caching entirely.
type GPUCacheConfig struct {
	// GPUMemoryBudget is the maximum GPU memory to use for caching in bytes.
	// Set to 0 to disable GPU caching (default).
	// Typical values: 256MB for light use, 1-4GB for consensus/DEX workloads.
	GPUMemoryBudget uint64 `json:"gpuMemoryBudget,omitempty"`

	// Backend selects the GPU backend: "auto", "metal", "cuda", "webgpu", "cpu".
	// Default: "auto" (best available).
	Backend string `json:"gpuBackend,omitempty"`

	// DeviceIndex selects which GPU device to use (-1 for default).
	DeviceIndex int `json:"gpuDeviceIndex,omitempty"`

	// InitialCapacity is the initial number of hash table slots (power of 2).
	// Default: 1048576 (1M slots). Each slot is 64 bytes.
	InitialCapacity uint32 `json:"gpuInitialCapacity,omitempty"`

	// EvictionThreshold triggers cache eviction when load factor exceeds this.
	// Default: 0.90. Range: 0.5 to 0.95.
	EvictionThreshold float32 `json:"gpuEvictionThreshold,omitempty"`

	// PromoteOnMiss controls whether disk reads are promoted into GPU cache.
	// Default: true. Set to false for write-heavy workloads where reads are rare.
	PromoteOnMiss bool `json:"gpuPromoteOnMiss,omitempty"`

	// WriteThrough ensures every Put is immediately persisted to disk.
	// Default: true. Set to false for higher write throughput at risk of
	// data loss on crash (dirty entries are flushed periodically).
	WriteThrough bool `json:"gpuWriteThrough,omitempty"`

	// MaxKeySize is the maximum key size in bytes. Default: 1024.
	MaxKeySize uint32 `json:"gpuMaxKeySize,omitempty"`

	// MaxValueSize is the maximum value size in bytes. Default: 65536.
	MaxValueSize uint32 `json:"gpuMaxValueSize,omitempty"`
}

// DefaultGPUCacheConfig returns a GPUCacheConfig with GPU caching disabled.
// To enable, set GPUMemoryBudget > 0.
func DefaultGPUCacheConfig() GPUCacheConfig {
	return GPUCacheConfig{
		GPUMemoryBudget:   0, // Disabled by default
		Backend:           "auto",
		DeviceIndex:       -1,
		InitialCapacity:   1 << 20,
		EvictionThreshold: 0.90,
		PromoteOnMiss:     true,
		WriteThrough:      true,
		MaxKeySize:        1024,
		MaxValueSize:      65536,
	}
}

// Enabled returns true if GPU caching is configured.
func (c *GPUCacheConfig) Enabled() bool {
	return c != nil && c.GPUMemoryBudget > 0
}

// ParseGPUConfig extracts GPU cache configuration from the database config
// JSON bytes. If the config contains gpuMemoryBudget > 0, GPU caching
// is enabled. This allows GPU mode to be activated via the standard
// database config mechanism (--db-config-content or --db-config-file).
//
// Example config JSON:
//
//	{
//	  "syncWrites": false,
//	  "gpuMemoryBudget": 536870912,
//	  "gpuBackend": "auto",
//	  "gpuPromoteOnMiss": true
//	}
func ParseGPUConfig(configBytes []byte) GPUCacheConfig {
	if len(configBytes) == 0 {
		return DefaultGPUCacheConfig()
	}

	cfg := DefaultGPUCacheConfig()
	// Use encoding/json to parse only GPU-related fields
	// Unknown fields are silently ignored
	_ = json.Unmarshal(configBytes, &cfg)
	return cfg
}
