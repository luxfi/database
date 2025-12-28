// Copyright (C) 2020-2025, Lux Industries Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package hookdb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// IndexerNotifier sends database events to an indexer service
type IndexerNotifier struct {
	endpoint string
	client   *http.Client

	// Batching
	mu       sync.Mutex
	batch    []IndexerEvent
	batchMax int
	ticker   *time.Ticker
	done     chan struct{}
}

// IndexerEvent is sent to the indexer service
type IndexerEvent struct {
	ChainID   string    `json:"chain_id"`
	Type      string    `json:"type"` // "put" or "delete"
	Prefix    string    `json:"prefix"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// NotifierConfig configures the indexer notifier
type NotifierConfig struct {
	// Endpoint is the indexer service URL (e.g., "http://localhost:8090/api/v2/events")
	Endpoint string
	// ChainID identifies this chain
	ChainID string
	// BatchSize is the max events per batch (default: 100)
	BatchSize int
	// FlushInterval is how often to flush batches (default: 100ms)
	FlushInterval time.Duration
	// Timeout for HTTP requests (default: 5s)
	Timeout time.Duration
}

// NewNotifier creates a new indexer notifier
func NewNotifier(cfg NotifierConfig) *IndexerNotifier {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 100 * time.Millisecond
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}

	n := &IndexerNotifier{
		endpoint: cfg.Endpoint,
		client:   &http.Client{Timeout: cfg.Timeout},
		batchMax: cfg.BatchSize,
		batch:    make([]IndexerEvent, 0, cfg.BatchSize),
		ticker:   time.NewTicker(cfg.FlushInterval),
		done:     make(chan struct{}),
	}

	go n.flusher()
	return n
}

// flusher periodically flushes batched events
func (n *IndexerNotifier) flusher() {
	for {
		select {
		case <-n.ticker.C:
			n.flush()
		case <-n.done:
			n.flush() // Final flush
			return
		}
	}
}

// flush sends batched events to the indexer
func (n *IndexerNotifier) flush() {
	n.mu.Lock()
	if len(n.batch) == 0 {
		n.mu.Unlock()
		return
	}
	events := n.batch
	n.batch = make([]IndexerEvent, 0, n.batchMax)
	n.mu.Unlock()

	// Send to indexer
	if err := n.send(events); err != nil {
		log.Printf("[hookdb] Failed to send %d events to indexer: %v", len(events), err)
	}
}

// send posts events to the indexer service
func (n *IndexerNotifier) send(events []IndexerEvent) error {
	if n.endpoint == "" {
		return nil // No endpoint configured
	}

	data, err := json.Marshal(events)
	if err != nil {
		return fmt.Errorf("marshal events: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", n.endpoint, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("indexer returned status %d", resp.StatusCode)
	}
	return nil
}

// Handler returns a hook handler for a specific chain
func (n *IndexerNotifier) Handler(chainID string) Handler {
	return func(e Event) {
		eventType := "put"
		if e.Type == EventDelete {
			eventType = "delete"
		}

		event := IndexerEvent{
			ChainID:   chainID,
			Type:      eventType,
			Prefix:    string(e.Prefix),
			Key:       string(e.Key),
			Value:     e.Value,
			Timestamp: time.Now(),
		}

		n.mu.Lock()
		n.batch = append(n.batch, event)
		shouldFlush := len(n.batch) >= n.batchMax
		n.mu.Unlock()

		if shouldFlush {
			go n.flush()
		}
	}
}

// Close stops the notifier and flushes remaining events
func (n *IndexerNotifier) Close() {
	n.ticker.Stop()
	close(n.done)
}

// BlockPrefixes returns common block-related key prefixes to watch
func BlockPrefixes() [][]byte {
	return [][]byte{
		[]byte("b"),          // blocks
		[]byte("H"),          // block headers
		[]byte("h"),          // block hashes
		[]byte("n"),          // block numbers
		[]byte("l"),          // transaction lookups
		[]byte("r"),          // receipts
	}
}

// EVMPrefixes returns EVM-specific key prefixes to watch
func EVMPrefixes() [][]byte {
	return [][]byte{
		[]byte("B"),          // block body
		[]byte("H"),          // block header
		[]byte("h"),          // header hash
		[]byte("n"),          // header number
		[]byte("b"),          // block hash -> number
		[]byte("l"),          // tx lookup
		[]byte("r"),          // receipts
		[]byte("LastBlock"),  // last block
		[]byte("LastAccepted"), // last accepted
	}
}
