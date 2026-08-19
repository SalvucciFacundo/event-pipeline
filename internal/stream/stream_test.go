package stream

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestFake_Contract exercises the StreamClient contract against the
// deterministic in-memory fake used by unit tests.
func TestFake_Contract(t *testing.T) {
	ctx := context.Background()
	f := NewFake()

	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	// EnsureGroup is idempotent.
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup twice: %v", err)
	}

	id1, err := f.Add(ctx, Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatalf("Add 1: %v", err)
	}
	id2, err := f.Add(ctx, Message{Type: "view", Payload: "{}", OccurredAt: "2026-01-01T00:00:01Z"})
	if err != nil {
		t.Fatalf("Add 2: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("IDs must be unique: %q == %q", id1, id2)
	}

	// ReadGroup assigns new entries to the requesting consumer.
	msgs, err := f.ReadGroup(ctx, "worker-1", 10, time.Second)
	if err != nil {
		t.Fatalf("ReadGroup: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("ReadGroup returned %d messages, want 2", len(msgs))
	}
	if msgs[0].ID != id1 || msgs[1].ID != id2 {
		t.Errorf("message order wrong: got %q then %q, want %q then %q", msgs[0].ID, msgs[1].ID, id1, id2)
	}
	if msgs[0].Type != "click" || msgs[1].Type != "view" {
		t.Errorf("message types wrong: %q, %q", msgs[0].Type, msgs[1].Type)
	}

	// A second read with nothing new returns nothing immediately.
	msgs, err = f.ReadGroup(ctx, "worker-1", 10, time.Millisecond)
	if err != nil {
		t.Fatalf("ReadGroup empty: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("ReadGroup empty returned %d messages, want 0", len(msgs))
	}

	// XACK makes entries no longer pending.
	if err := f.Ack(ctx, id1, id2); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	// XRANGE replays entries after an exclusive cursor.
	replayed, err := f.Range(ctx, id1, "+", 10)
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(replayed) != 1 || replayed[0].ID != id2 {
		t.Errorf("Range after %q = %+v, want exactly %q", id1, replayed, id2)
	}

	// XAUTOCLAIM reclaims pending entries owned by another consumer.
	f3, err := f.Add(ctx, Message{Type: "view", Payload: "{}", OccurredAt: "2026-01-01T00:00:02Z"})
	if err != nil {
		t.Fatalf("Add 3: %v", err)
	}
	if _, err := f.ReadGroup(ctx, "dead-worker", 10, time.Second); err != nil {
		t.Fatalf("ReadGroup dead-worker: %v", err)
	}
	claimed, err := f.Claim(ctx, "worker-2", 0, 10)
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != f3 {
		t.Errorf("Claim = %+v, want exactly %q", claimed, f3)
	}
}

func TestFake_ReadGroupBlocksUntilMessage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	f := NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var got []Message
	var gotErr error
	go func() {
		defer wg.Done()
		got, gotErr = f.ReadGroup(ctx, "worker-1", 10, time.Second)
	}()

	time.Sleep(50 * time.Millisecond)
	if _, err := f.Add(ctx, Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	wg.Wait()
	if gotErr != nil {
		t.Fatalf("ReadGroup after Add: %v", gotErr)
	}
	if len(got) != 1 || got[0].Type != "click" {
		t.Errorf("ReadGroup = %+v, want one click message", got)
	}
}

func TestFake_ReadGroupRespectsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	f := NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := f.ReadGroup(ctx, "worker-1", 10, time.Second)
		done <- err
	}()

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("ReadGroup should return an error on cancellation, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ReadGroup did not return after cancellation")
	}
}