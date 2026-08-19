package worker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"go.uber.org/goleak"

	"event-pipeline/internal/stream"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestWorkerProcessesAndAcks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	id1, err := f.Add(ctx, stream.Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatalf("Add 1: %v", err)
	}
	id2, err := f.Add(ctx, stream.Message{Type: "view", Payload: "{}", OccurredAt: "2026-01-01T00:00:01Z"})
	if err != nil {
		t.Fatalf("Add 2: %v", err)
	}

	out := make(chan stream.Message, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, f, "worker-test", out, slog.New(slog.DiscardHandler))
	}()

	var got []stream.Message
	for len(got) < 2 {
		select {
		case m := <-out:
			got = append(got, m)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for messages; got %d", len(got))
		}
	}

	cancel()
	<-done

	if got[0].ID != id1 || got[1].ID != id2 {
		t.Errorf("processed order = %q, %q; want %q, %q", got[0].ID, got[1].ID, id1, id2)
	}
	if !f.IsAcked(id1) || !f.IsAcked(id2) {
		t.Errorf("expected both entries acked after successful dispatch: %v %v", f.IsAcked(id1), f.IsAcked(id2))
	}
}

func TestWorkerClaimsPendingBeforeReadingNew(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	id, err := f.Add(ctx, stream.Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	// Simulate a crashed worker: the entry is delivered to "dead-worker"
	// but never acked.
	if _, err := f.ReadGroup(ctx, "dead-worker", 10, time.Second); err != nil {
		t.Fatalf("ReadGroup dead-worker: %v", err)
	}
	if f.IsAcked(id) {
		t.Fatal("test setup broken: entry should not be acked")
	}

	out := make(chan stream.Message, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, f, "replacement", out, slog.New(slog.DiscardHandler))
	}()

	select {
	case m := <-out:
		if m.ID != id {
			t.Errorf("claimed message %q, want %q", m.ID, id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for claimed pending entry")
	}

	cancel()
	<-done
	if !f.IsAcked(id) {
		t.Error("claimed entry must be acked after dispatch")
	}
}

func TestWorkerExitsOnCancellation(t *testing.T) {
	f := stream.NewFake()
	if err := f.EnsureGroup(context.Background()); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan stream.Message, 4)
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, f, "worker-exit", out, slog.New(slog.DiscardHandler))
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit after cancellation")
	}
}
