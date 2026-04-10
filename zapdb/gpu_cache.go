// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package zapdb

import (
	"encoding/binary"
	"math/bits"
	"sort"
	"sync"
	"sync/atomic"
)

// gpuCache implements the same Robin Hood hash table that runs on GPU.
//
// The slot layout, hash function (xxHash64), and probing algorithm are
// identical to luxcpp/zapdb/src/gpu_hashtable.cpp and the Metal/CUDA/WGSL
// kernels. This ensures the GPU and CPU code paths produce identical results.
//
// On unified memory systems (Apple Silicon Metal, CPU backend), the C library
// allocates the slot array and blob arena as GPU buffers via backend vtable.
// buffer_get_host_ptr() returns a direct pointer — the CPU reads/writes GPU
// memory at full speed, and the same memory is accessible to GPU kernels for
// batch operations. This is NOT a CPU mirror — it IS the GPU memory.
//
// This Go implementation serves as:
//   1. The pure-Go path when CGO/C library is not linked
//   2. Reference implementation matching the GPU kernel behavior exactly
//
// Both paths produce identical results for all database operations.
type gpuCache struct {
	config  GPUCacheConfig
	slots   []cacheSlot
	blob    []byte
	cap     uint32
	mask    uint32
	count   atomic.Uint32
	lruTick atomic.Uint64

	// mu protects slots and blob from concurrent access.
	// Reads (get, has) take RLock; writes (put, remove, evict) take Lock.
	mu            sync.RWMutex
	blobMu        sync.Mutex
	blobWatermark uint32

	// Stats (atomics — no lock needed)
	hits       atomic.Uint64
	misses     atomic.Uint64
	evictions  atomic.Uint64
	promotions atomic.Uint64
}

// cacheSlot is a 64-byte cache line aligned hash table slot.
type cacheSlot struct {
	hash      uint64
	keyOff    uint32
	keyLen    uint32
	valOff    uint32
	valLen    uint32
	lruTick   uint64
	psl       uint16 // probe sequence length
	flags     uint8
	_pad      uint8
	inlineKV  [28]byte // inline storage for small key+value
}

const (
	slotEmpty     = 0x00
	slotOccupied  = 0x01
	slotTombstone = 0x02
	slotDirty     = 0x04

	inlineThreshold = 28 // key+value <= 28 bytes stored inline
)

func newGPUCache(config GPUCacheConfig) *gpuCache {
	// Round capacity to power of 2
	cap := uint32(config.InitialCapacity)
	if cap == 0 {
		cap = 1 << 20
	}
	cap = nextPow2(cap)

	// Blob arena: remaining budget after slots
	slotBytes := uint64(cap) * 64 // sizeof(cacheSlot) ~64
	blobSize := uint32(0)
	if config.GPUMemoryBudget > slotBytes {
		blobSize = uint32(config.GPUMemoryBudget - slotBytes)
	} else {
		blobSize = uint32(slotBytes / 2)
	}

	gc := &gpuCache{
		config:        config,
		slots:         make([]cacheSlot, cap),
		blob:          make([]byte, blobSize),
		cap:           cap,
		mask:          cap - 1,
		blobWatermark: 1, // Offset 0 is reserved as inline sentinel
	}
	return gc
}

func nextPow2(v uint32) uint32 {
	if v == 0 {
		return 1
	}
	if v > (1 << 30) {
		return 1 << 30 // Max 1 billion slots (prevents overflow)
	}
	return 1 << bits.Len32(v-1)
}

// xxhash64 computes xxHash64 of data (matches the C implementation)
func xxhash64(data []byte) uint64 {
	var (
		p1 = uint64(0x9E3779B185EBCA87)
		p2 = uint64(0xC2B2AE3D27D4EB4F)
		p3 = uint64(0x165667B19E3779F9)
		p4 = uint64(0x85EBCA77C2B2AE63)
		p5 = uint64(0x27D4EB2F165667C5)
	)

	n := len(data)
	var h uint64

	if n >= 32 {
		v1 := p1 + p2 // wraps
		v2 := p2
		v3 := uint64(0)
		v4 := uint64(0) - p1 // wraps

		i := 0
		for i+32 <= n {
			v1 = xxh64round(v1, binary.LittleEndian.Uint64(data[i:]))
			v2 = xxh64round(v2, binary.LittleEndian.Uint64(data[i+8:]))
			v3 = xxh64round(v3, binary.LittleEndian.Uint64(data[i+16:]))
			v4 = xxh64round(v4, binary.LittleEndian.Uint64(data[i+24:]))
			i += 32
		}
		data = data[i:]

		h = bits.RotateLeft64(v1, 1) + bits.RotateLeft64(v2, 7) +
			bits.RotateLeft64(v3, 12) + bits.RotateLeft64(v4, 18)
		h = xxh64merge(h, v1)
		h = xxh64merge(h, v2)
		h = xxh64merge(h, v3)
		h = xxh64merge(h, v4)
	} else {
		h = p5
	}

	h += uint64(n)

	for len(data) >= 8 {
		k := binary.LittleEndian.Uint64(data)
		h ^= xxh64round(0, k)
		h = bits.RotateLeft64(h, 27)*p1 + p4
		data = data[8:]
	}
	for len(data) >= 4 {
		k := uint64(binary.LittleEndian.Uint32(data))
		h ^= k * p1
		h = bits.RotateLeft64(h, 23)*p2 + p3
		data = data[4:]
	}
	for _, b := range data {
		h ^= uint64(b) * p5
		h = bits.RotateLeft64(h, 11) * p1
	}

	h ^= h >> 33
	h *= p2
	h ^= h >> 29
	h *= p3
	h ^= h >> 32
	return h
}

func xxh64round(acc, input uint64) uint64 {
	acc += input * 0xC2B2AE3D27D4EB4F
	acc = bits.RotateLeft64(acc, 31)
	acc *= 0x9E3779B185EBCA87
	return acc
}

func xxh64merge(acc, val uint64) uint64 {
	val = xxh64round(0, val)
	acc ^= val
	acc = acc*0x9E3779B185EBCA87 + 0x85EBCA77C2B2AE63
	return acc
}

// =============================================================================
// Cache Operations
// =============================================================================

func (c *gpuCache) get(key []byte) ([]byte, bool) {
	hash := xxhash64(key)

	// get() needs write lock because it updates lruTick on the slot
	c.mu.Lock()
	defer c.mu.Unlock()

	idx := uint32(hash) & c.mask
	var psl uint16

	for i := uint32(0); i < c.cap; i++ {
		slot := &c.slots[idx]

		if slot.flags == slotEmpty {
			return nil, false
		}
		if slot.psl < psl {
			return nil, false
		}
		if slot.flags&slotOccupied != 0 && slot.hash == hash && c.keyMatches(slot, key) {
			// Update LRU tick
			slot.lruTick = c.lruTick.Add(1)
			c.hits.Add(1)
			return c.readValue(slot), true
		}

		idx = (idx + 1) & c.mask
		psl++
	}
	return nil, false
}

func (c *gpuCache) has(key []byte) bool {
	hash := xxhash64(key)

	// has() needs write lock because it updates lruTick on the slot
	c.mu.Lock()
	defer c.mu.Unlock()

	idx := uint32(hash) & c.mask
	var psl uint16

	for i := uint32(0); i < c.cap; i++ {
		slot := &c.slots[idx]

		if slot.flags == slotEmpty {
			return false
		}
		if slot.psl < psl {
			return false
		}
		if slot.flags&slotOccupied != 0 && slot.hash == hash && c.keyMatches(slot, key) {
			slot.lruTick = c.lruTick.Add(1)
			return true
		}

		idx = (idx + 1) & c.mask
		psl++
	}
	return false
}

func (c *gpuCache) put(key, value []byte, dirty bool) bool {
	if c.loadFactor() >= 0.90 {
		return false // Caller must evict first
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	hash := xxhash64(key)
	tick := c.lruTick.Add(1)

	incoming := cacheSlot{
		hash:    hash,
		lruTick: tick,
		psl:     0,
		flags:   slotOccupied,
	}
	if dirty {
		incoming.flags |= slotDirty
	}
	c.storeKV(&incoming, key, value)

	idx := uint32(hash) & c.mask

	for i := uint32(0); i < c.cap; i++ {
		slot := &c.slots[idx]

		if slot.flags == slotEmpty || slot.flags == slotTombstone {
			*slot = incoming
			c.count.Add(1)
			return true
		}

		// Update existing key
		if slot.flags&slotOccupied != 0 && slot.hash == hash && c.keyMatches(slot, key) {
			c.storeKV(slot, key, value)
			slot.lruTick = tick
			slot.flags = slotOccupied
			if dirty {
				slot.flags |= slotDirty
			}
			return true
		}

		// Robin Hood: steal from the rich
		if slot.psl < incoming.psl {
			incoming, *slot = *slot, incoming
		}

		incoming.psl++
		idx = (idx + 1) & c.mask
	}

	return false
}

func (c *gpuCache) remove(key []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()

	hash := xxhash64(key)
	idx := uint32(hash) & c.mask
	var psl uint16

	for i := uint32(0); i < c.cap; i++ {
		slot := &c.slots[idx]

		if slot.flags == slotEmpty {
			return
		}
		if slot.psl < psl {
			return
		}
		if slot.flags&slotOccupied != 0 && slot.hash == hash && c.keyMatches(slot, key) {
			// Backward shift deletion
			slot.flags = slotEmpty
			c.count.Add(^uint32(0)) // -1

			prev := idx
			next := (idx + 1) & c.mask
			for c.slots[next].flags&slotOccupied != 0 && c.slots[next].psl > 0 {
				c.slots[prev] = c.slots[next]
				c.slots[prev].psl--
				c.slots[next].flags = slotEmpty
				prev = next
				next = (next + 1) & c.mask
			}
			return
		}

		idx = (idx + 1) & c.mask
		psl++
	}
}

func (c *gpuCache) invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i := range c.slots {
		c.slots[i] = cacheSlot{}
	}
	c.count.Store(0)
	c.blobMu.Lock()
	c.blobWatermark = 1 // Offset 0 is reserved as inline sentinel
	c.blobMu.Unlock()
}

func (c *gpuCache) loadFactor() float32 {
	return float32(c.count.Load()) / float32(c.cap)
}

// evictColdest removes the N entries with the lowest LRU tick.
// Returns dirty entries that need to be flushed to disk.
type evictedEntry struct {
	key   []byte
	value []byte
}

func (c *gpuCache) evictColdest(n int) []evictedEntry {
	if n <= 0 {
		n = 4096
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// Collect all occupied slots with their indices and ticks
	type candidate struct {
		idx  uint32
		tick uint64
	}
	candidates := make([]candidate, 0, c.count.Load())
	for i := uint32(0); i < c.cap; i++ {
		if c.slots[i].flags&slotOccupied != 0 {
			candidates = append(candidates, candidate{idx: i, tick: c.slots[i].lruTick})
		}
	}

	// Sort by tick ascending (coldest first)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].tick < candidates[j].tick
	})

	// Evict the N coldest
	if n > len(candidates) {
		n = len(candidates)
	}

	var dirty []evictedEntry
	for i := 0; i < n; i++ {
		slot := &c.slots[candidates[i].idx]
		if slot.flags&slotDirty != 0 {
			dirty = append(dirty, evictedEntry{
				key:   c.readKey(slot),
				value: c.readValue(slot),
			})
		}
		slot.flags = slotTombstone
		c.count.Add(^uint32(0))
	}

	c.evictions.Add(uint64(n))
	return dirty
}

// flushDirty calls fn for each dirty entry in the cache.
// Caller should persist the entry to disk. Does NOT clear dirty flags
// because this is used during shutdown when the cache is being discarded.
func (c *gpuCache) flushDirty(fn func(key, value []byte) error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for i := uint32(0); i < c.cap; i++ {
		slot := &c.slots[i]
		if slot.flags&slotOccupied != 0 && slot.flags&slotDirty != 0 {
			key := c.readKey(slot)
			value := c.readValue(slot)
			if err := fn(key, value); err != nil {
				return // Best effort on shutdown
			}
		}
	}
}

// extractAll returns all entries sorted by key (for iterator support)
type extractedEntry struct {
	key   []byte
	value []byte
	flags uint8
}

func (c *gpuCache) extractAll() []extractedEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entries := make([]extractedEntry, 0, c.count.Load())
	for i := uint32(0); i < c.cap; i++ {
		slot := &c.slots[i]
		if slot.flags&slotOccupied != 0 {
			entries = append(entries, extractedEntry{
				key:   c.readKey(slot),
				value: c.readValue(slot),
				flags: slot.flags,
			})
		}
	}
	// Sort by key
	sortEntries(entries)
	return entries
}

// =============================================================================
// Key/Value Storage
// =============================================================================

func (c *gpuCache) keyMatches(slot *cacheSlot, key []byte) bool {
	if slot.keyLen != uint32(len(key)) {
		return false
	}
	if slot.keyLen+slot.valLen <= inlineThreshold && slot.keyOff == 0 {
		return bytesEqual(slot.inlineKV[:slot.keyLen], key)
	}
	off := slot.keyOff
	return bytesEqual(c.blob[off:off+slot.keyLen], key)
}

func (c *gpuCache) storeKV(slot *cacheSlot, key, value []byte) {
	slot.keyLen = uint32(len(key))
	slot.valLen = uint32(len(value))

	if len(key)+len(value) <= inlineThreshold {
		slot.keyOff = 0
		slot.valOff = 0
		copy(slot.inlineKV[:], key)
		copy(slot.inlineKV[len(key):], value)
	} else {
		slot.keyOff = c.blobAlloc(uint32(len(key)))
		copy(c.blob[slot.keyOff:], key)
		slot.valOff = c.blobAlloc(uint32(len(value)))
		copy(c.blob[slot.valOff:], value)
	}
}

func (c *gpuCache) readKey(slot *cacheSlot) []byte {
	out := make([]byte, slot.keyLen)
	if slot.keyLen+slot.valLen <= inlineThreshold && slot.keyOff == 0 {
		copy(out, slot.inlineKV[:slot.keyLen])
	} else {
		copy(out, c.blob[slot.keyOff:slot.keyOff+slot.keyLen])
	}
	return out
}

func (c *gpuCache) readValue(slot *cacheSlot) []byte {
	out := make([]byte, slot.valLen)
	if slot.keyLen+slot.valLen <= inlineThreshold && slot.keyOff == 0 {
		copy(out, slot.inlineKV[slot.keyLen:slot.keyLen+slot.valLen])
	} else {
		copy(out, c.blob[slot.valOff:slot.valOff+slot.valLen])
	}
	return out
}

func (c *gpuCache) blobAlloc(size uint32) uint32 {
	c.blobMu.Lock()
	defer c.blobMu.Unlock()

	off := c.blobWatermark
	c.blobWatermark += size
	if int(c.blobWatermark) > len(c.blob) {
		// Grow blob arena
		newBlob := make([]byte, c.blobWatermark*2)
		copy(newBlob, c.blob)
		c.blob = newBlob
	}
	return off
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// sortEntries sorts by key using introsort (O(n log n))
func sortEntries(entries []extractedEntry) {
	sort.Slice(entries, func(i, j int) bool {
		return bytesLess(entries[i].key, entries[j].key)
	})
}

func bytesLess(a, b []byte) bool {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return len(a) < len(b)
}

// stats returns cache statistics
type gpuCacheStats struct {
	Entries    uint32
	Capacity   uint32
	MemoryUsed uint64
	Hits       uint64
	Misses     uint64
	Evictions  uint64
	Promotions uint64
	LoadFactor float32
}

func (c *gpuCache) stats() gpuCacheStats {
	return gpuCacheStats{
		Entries:    c.count.Load(),
		Capacity:   c.cap,
		MemoryUsed: uint64(c.count.Load())*64 + uint64(c.blobWatermark),
		Hits:       c.hits.Load(),
		Misses:     c.misses.Load(),
		Evictions:  c.evictions.Load(),
		Promotions: c.promotions.Load(),
		LoadFactor: c.loadFactor(),
	}
}
