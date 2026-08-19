//go:build integration

package stream

import (
	"context"
	"os"
	"testing"
	"time"
)

// redisURLForIntegration returns the configured Redis URL or skips.
func redisURLForIntegration(t *testing.T) string {
	t.Helper()
	url := os.Getenv("REDIS_URL")
	if url == "" {
		t.Skip("REDIS_URL not set; skipping Redis integration test")
	}
	return url
}

func TestRedisStreamEndToEnd(t *testing.T) {
	ctx := context.Background()
	client, err := NewRedis(redisURLForIntegration(t))
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer client.Close()

	if err := client.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	msg := Message{
		Type:       "integration",
		Payload:    `{"value":1}`,
		OccurredAt: "2026-01-01T00:00:00Z",
		Source:     "test",
	}
	id, err := client.Add(ctx, msg)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if id == "" {
		t.Fatal("Add returned empty id")
	}

	// XREADGROUP delivers the entry to a consumer.
	msgs, err := client.ReadGroup(ctx, "it-consumer", 10, 2*time.Second)
	if err != nil {
		t.Fatalf("ReadGroup: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("ReadGroup got %d messages, want 1", len(msgs))
	}
	if msgs[0].ID != id {
		t.Errorf("message id = %q, want %q", msgs[0].ID, id)
	}
	if msgs[0].Type != "integration" {
		t.Errorf("type = %q, want integration", msgs[0].Type)
	}

	// XACK removes it from the pending set.
	if err := client.Ack(ctx, id); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// XRANGE replay from the exclusive cursor returns the entry.
	replayed, err := client.Range(ctx, id, "+", 10)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(replayed) != 0 {
		t.Errorf("Range after %q got %d, want 0 (exclusive cursor)", id, len(replayed))
	}
	replayed, err = client.Range(ctx, "", "+", 10)
	if err != nil {
		t.Fatalf("Range empty-from: %v", err)
	}
	if len(replayed) != 1 || replayed[0].ID != id {
		t.Errorf("Range from beginning got %+v, want exactly %q", replayed, id)
	}

	// Ping reports health.
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}

func TestRedisPendingRecoveryViaClaim(t *testing.T) {
	ctx := context.Background()
	client, err := NewRedis(redisURLForIntegration(t))
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer client.Close()
	if err := client.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	// A "dead" consumer reads but never acks.
	id, err := client.Add(ctx, Message{Type: "recovery", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := client.ReadGroup(ctx, "dead-consumer", 10, 2*time.Second); err != nil {
		t.Fatalf("ReadGroup dead-consumer: %v", err)
	}

	// A replacement worker claims the pending entry.
	claimed, err := client.Claim(ctx, "replacement", time.Millisecond, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	found := false
	for _, m := range claimed {
		if m.ID == id {
			found = true
		}
	}
	if !found {
		t.Errorf("claimed messages %+v do not include %q", claimed, id)
	}
}
