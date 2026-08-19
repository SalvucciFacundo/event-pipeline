// Package stream abstracts the Redis Streams transport behind a narrow
// interface so the pipeline is testable without a Redis instance.
package stream

import (
	"context"
	"time"
)

// Stream key and consumer group names are stable for the whole service.
const (
	// StreamKey is the single canonical stream of events.
	StreamKey = "event-pipeline:events"
	// GroupName is the single consumer group used by the worker pool.
	GroupName = "event-pipeline-workers"
)

// Message is one event as stored in the stream. Redis-generated IDs are
// the sole replay cursor and SSE id; fields carry the bounded event data.
type Message struct {
	// ID is the Redis stream ID (for example "1728000000000-0"). It is
	// assigned by the transport on Add and filled on reads.
	ID string
	// Type is the bounded event type label.
	Type string
	// Payload is the bounded JSON payload string.
	Payload string
	// OccurredAt is the RFC3339 UTC timestamp of the event.
	OccurredAt string
	// Source is an optional bounded producer identifier.
	Source string
}

// StreamClient is the transport contract consumed by workers and the SSE
// hub. Implementations must be safe for concurrent use.
type StreamClient interface {
	// EnsureGroup creates the stream and consumer group if missing and is
	// idempotent.
	EnsureGroup(ctx context.Context) error
	// Add appends a message to the stream and returns its ID.
	Add(ctx context.Context, msg Message) (string, error)
	// ReadGroup reads up to count new messages for a consumer, blocking
	// up to block when the stream is empty.
	ReadGroup(ctx context.Context, consumer string, count int, block time.Duration) ([]Message, error)
	// Ack acknowledges processed message IDs so they leave the pending set.
	Ack(ctx context.Context, ids ...string) error
	// Range replays messages with ID strictly greater than fromExclusive
	// (exclusive), up to to ("+" for tail), limited to count.
	Range(ctx context.Context, fromExclusive, to string, count int) ([]Message, error)
	// Claim reclaims pending messages owned by other (possibly dead)
	// consumers and assigns them to consumer, returning up to count.
	Claim(ctx context.Context, consumer string, minIdle time.Duration, count int) ([]Message, error)
	// Close releases the underlying connection.
	Close() error
}