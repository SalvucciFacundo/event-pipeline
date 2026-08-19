// Package sse implements the SSE hub with a two-phase replay barrier:
// a client is registered with a bounded queue, replay entries are enqueued
// from the stream via exclusive XRANGE, and only then does the gate open.
// Replay and live delivery share one Event envelope.
package sse

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"event-pipeline/internal/stream"
)

// Event is the SSE envelope: the Redis stream ID is the event id, Kind is
// the SSE event type, and Data is the JSON payload.
type Event struct {
	ID   string
	Kind string
	Data string
}

// Options configures the hub.
type Options struct {
	// ClientBuffer is the bounded per-client queue size; defaults to 256.
	ClientBuffer int
	// ReplayLimit bounds how many entries a reconnecting client may replay;
	// defaults to 1000.
	ReplayLimit int
}

// Client is one connected SSE subscriber. The hub owns the queue; the HTTP
// handler owns the response writer and reads from Events().
type Client struct {
	id        string
	ch        chan Event
	gate      chan struct{}
	sendMu    sync.Mutex
	closeOnce sync.Once
}

// ID returns the client identifier.
func (c *Client) ID() string { return c.id }

// Events returns the bounded queue the handler reads from.
func (c *Client) Events() <-chan Event { return c.ch }

// ReplayDone is closed once the replay barrier opens.
func (c *Client) ReplayDone() <-chan struct{} { return c.gate }

// send enqueues an event without blocking. It returns false when the queue
// is full (slow client). Sends and close are serialized so no send happens
// on a closed channel.
func (c *Client) send(ev Event) bool {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	select {
	case c.ch <- ev:
		return true
	default:
		return false
	}
}

func (c *Client) close() {
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.closeOnce.Do(func() { close(c.ch) })
}

// Hub fans events out to all subscribed clients and owns their lifecycle.
type Hub struct {
	mu      sync.Mutex
	clients map[string]*Client
	stream  stream.StreamClient
	opts    Options
}

// NewHub creates a hub backed by the stream client used for XRANGE replay.
func NewHub(sc stream.StreamClient, opts Options) *Hub {
	if opts.ClientBuffer <= 0 {
		opts.ClientBuffer = 256
	}
	if opts.ReplayLimit <= 0 {
		opts.ReplayLimit = 1000
	}
	return &Hub{
		clients: map[string]*Client{},
		stream:  sc,
		opts:    opts,
	}
}

// Subscribe registers a client and runs the replay barrier: if lastID is
// non-empty it enqueues every entry strictly after it (exclusive XRANGE),
// then opens the gate. Fresh clients open the gate immediately. Live
// broadcasts may enqueue while the gate is closed; queue order still
// guarantees replay-before-live.
func (h *Hub) Subscribe(ctx context.Context, lastID string) (*Client, error) {
	c := &Client{
		id:   randHex(8),
		ch:   make(chan Event, h.opts.ClientBuffer),
		gate: make(chan struct{}),
	}

	h.mu.Lock()
	h.clients[c.id] = c
	h.mu.Unlock()

	if lastID != "" {
		msgs, err := h.stream.Range(ctx, lastID, "+", h.opts.ReplayLimit)
		if err != nil {
			h.Unsubscribe(c)
			return nil, fmt.Errorf("replay xrange: %w", err)
		}
		for _, m := range msgs {
			data, err := json.Marshal(m)
			if err != nil {
				continue
			}
			if !c.send(Event{ID: m.ID, Kind: "event", Data: string(data)}) {
				h.Unsubscribe(c)
				return nil, fmt.Errorf("client %s overflowed during replay", c.id)
			}
		}
	}

	close(c.gate)
	return c, nil
}

// Broadcast fans one event out to every client. Slow clients (full queue)
// are disconnected; they may reconnect from their last acknowledged ID.
func (h *Hub) Broadcast(ev Event) {
	h.mu.Lock()
	clients := make([]*Client, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()

	for _, c := range clients {
		if !c.send(ev) {
			h.Unsubscribe(c)
		}
	}
}

// Unsubscribe removes the client and closes its queue. Idempotent.
func (h *Hub) Unsubscribe(c *Client) {
	h.mu.Lock()
	if h.clients[c.id] == c {
		delete(h.clients, c.id)
	}
	h.mu.Unlock()
	c.close()
}

// ClientCount reports the number of registered clients.
func (h *Hub) ClientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// Close disconnects every client. Idempotent.
func (h *Hub) Close() {
	h.mu.Lock()
	clients := make([]*Client, 0, len(h.clients))
	for _, c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		c.close()
	}
}

func randHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(buf)
}
