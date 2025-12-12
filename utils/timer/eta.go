// Copyright (C) 2019-2025, Lux Industries, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package timer

import (
	"encoding/binary"
	"time"
)

// ProgressFromHash returns the progress out of MaxUint64 assuming [b] is a key
// in a uniformly distributed sequence that is being iterated lexicographically.
func ProgressFromHash(b []byte) uint64 {
	// binary.BigEndian.Uint64 will panic if the input length is less than 8, so
	// pad 0s as needed.
	var progress [8]byte
	copy(progress[:], b)
	return binary.BigEndian.Uint64(progress[:])
}

// EstimateETA attempts to estimate the remaining time for a job to finish given
// the [startTime] and it's current progress.
func EstimateETA(startTime time.Time, progress, end uint64) time.Duration {
	timeSpent := time.Since(startTime)

	percentExecuted := float64(progress) / float64(end)
	estimatedTotalDuration := time.Duration(float64(timeSpent) / percentExecuted)
	eta := estimatedTotalDuration - timeSpent
	return eta.Round(time.Second)
}

// sample represents a single progress sample
type sample struct {
	completed uint64
	timestamp time.Time
}

// rateInterval represents a rate measurement over a time interval
type rateInterval struct {
	rate     float64
	duration float64 // in seconds
}

// EtaTracker provides time-weighted average ETA estimates
type EtaTracker struct {
	minSamples    int
	alpha         float64 // unused, kept for API compatibility
	sampleHistory []sample
	rateHistory   []rateInterval
}

// NewEtaTracker creates a new ETA tracker with the given number of samples and alpha
func NewEtaTracker(minSamples int, alpha float64) *EtaTracker {
	return &EtaTracker{
		minSamples:    minSamples,
		alpha:         alpha,
		sampleHistory: make([]sample, 0, minSamples),
		rateHistory:   make([]rateInterval, 0, minSamples),
	}
}

// Update updates the ETA tracker with new progress
func (e *EtaTracker) Update(progress, total uint64) {
	e.AddSample(progress, total, time.Now())
}

// AddSample adds a sample to the ETA tracker and returns ETA pointer and progress percentage
func (e *EtaTracker) AddSample(completed, total uint64, sampleTime time.Time) (*time.Duration, float64) {
	if total == 0 {
		return nil, 0
	}

	// Check if we're at or past the target
	if completed >= total {
		zero := time.Duration(0)
		return &zero, 100.0
	}

	// Check for valid sample (must be later than previous and have more progress)
	if len(e.sampleHistory) > 0 {
		lastSample := e.sampleHistory[len(e.sampleHistory)-1]
		// Reject samples that go backwards in time or don't show progress
		if !sampleTime.After(lastSample.timestamp) || completed <= lastSample.completed {
			return nil, 0
		}

		// Calculate rate for this new interval
		duration := sampleTime.Sub(lastSample.timestamp).Seconds()
		if duration > 0 {
			rate := float64(completed-lastSample.completed) / duration
			e.rateHistory = append(e.rateHistory, rateInterval{
				rate:     rate,
				duration: duration,
			})
		}
	}

	// Add the new sample
	e.sampleHistory = append(e.sampleHistory, sample{
		completed: completed,
		timestamp: sampleTime,
	})

	// Need at least minSamples to estimate (minSamples samples = minSamples-1 intervals)
	if len(e.sampleHistory) < e.minSamples {
		return nil, 0
	}

	// Calculate time-weighted average rate
	// Only use intervals starting from minSamples-1 (skip early warm-up intervals)
	var totalWeightedRate, totalDuration float64
	startIdx := e.minSamples - 2 // Start from interval that ends at minSamples-1
	if startIdx < 0 {
		startIdx = 0
	}
	for i := startIdx; i < len(e.rateHistory); i++ {
		ri := e.rateHistory[i]
		totalWeightedRate += ri.rate * ri.duration
		totalDuration += ri.duration
	}

	if totalDuration <= 0 {
		return nil, 0
	}

	avgRate := totalWeightedRate / totalDuration

	// Calculate ETA based on time-weighted average rate
	remaining := total - completed
	etaSeconds := float64(remaining) / avgRate
	eta := time.Duration(etaSeconds * float64(time.Second))
	progressPercent := float64(completed) / float64(total) * 100

	return &eta, progressPercent
}

// ETA returns the estimated time remaining
func (e *EtaTracker) ETA(progress, total uint64) time.Duration {
	eta, _ := e.AddSample(progress, total, time.Now())
	if eta == nil {
		return 0
	}
	return *eta
}
