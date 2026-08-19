package sse

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"event-pipeline/internal/stream"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newTestHub(t *testing.T, f *stream.Fake, buffer int) *Hub {
	t.Helper()
	opts := Options{
		ClientBuffer: buffer,
		ReplayLimit:  100,
	}
	return NewHub(f, opts)
}

func TestHubReplayThenLiveOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	id1, _ := f.Add(ctx, stream.Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})
	id2, _ := f.Add(ctx, stream.Message{Type: "view", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})
	id3, _ := f.Add(ctx, stream.Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})

	hub := newTestHub(t, f, 16)
	c, err := hub.Subscribe(ctx, id1)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer hub.Unsubscribe(c)

	// Replay must deliver exactly the entries after the cursor, in order.
	for i, want := range []string{id2, id3} {
		select {
		case ev := <-c.Events():
			if ev.ID != want {
				t.Errorf("replay[%d] id = %q, want %q", i, ev.ID, want)
			}
			if ev.Kind != "event" {
				t.Errorf("replay[%d] kind = %q, want event", i, ev.Kind)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for replay[%d]", i)
		}
	}

	// Live events follow after replay.
	id4, _ := f.Add(ctx, stream.Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})
	hub.Broadcast(Event{ID: id4, Kind: "event", Data: "{}"})

	select {
	case ev := <-c.Events():
		if ev.ID != id4 {
			t.Errorf("live id = %q, want %q", ev.ID, id4)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live event")
	}
}

func TestHubFreshClientNoReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	if _, err := f.Add(ctx, stream.Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	hub := newTestHub(t, f, 16)
	c, err := hub.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer hub.Unsubscribe(c)

	// A fresh client gets no replay; the first thing it sees is live data.
	id2, _ := f.Add(ctx, stream.Message{Type: "view", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"})
	hub.Broadcast(Event{ID: id2, Kind: "event", Data: "{}"})

	select {
	case ev := <-c.Events():
		if ev.ID != id2 {
			t.Errorf("first live id = %q, want %q", ev.ID, id2)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live event")
	}
}

func TestHubSlowClientDisconnected(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	hub := newTestHub(t, f, 2)
	c, err := hub.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if got := hub.ClientCount(); got != 1 {
		t.Fatalf("ClientCount = %d, want 1", got)
	}

	// Overflow the bounded queue: the hub must disconnect the client.
	for i := 0; i < 5; i++ {
		hub.Broadcast(Event{ID: "x", Kind: "event", Data: "{}"})
	}

	// Drain the buffered events first; the channel must close afterwards.
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-c.Events():
			if !ok {
				goto disconnected
			}
		case <-deadline:
			t.Fatal("client channel was not closed after slow-client disconnect")
		}
	}
disconnected:

	if got := hub.ClientCount(); got != 0 {
		t.Errorf("ClientCount after disconnect = %d, want 0", got)
	}
}

func TestHubConcurrentBroadcast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}

	hub := newTestHub(t, f, 256)

	const clients = 5
	var subs []*Client
	for i := 0; i < clients; i++ {
		c, err := hub.Subscribe(ctx, "")
		if err != nil {
			t.Fatalf("Subscribe %d: %v", i, err)
		}
		subs = append(subs, c)
		defer hub.Unsubscribe(c)
	}

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				hub.Broadcast(Event{ID: "live", Kind: "event", Data: "{}"})
			}
		}(i)
	}
	wg.Wait()

	for _, c := range subs {
		deadline := time.After(time.Second)
		for {
			select {
			case ev, ok := <-c.Events():
				if !ok {
					t.Fatal("client disconnected unexpectedly")
				}
				if ev.ID != "live" {
					t.Fatalf("unexpected event id %q", ev.ID)
				}
				if len(c.Events()) == 0 {
					goto next
				}
			case <-deadline:
				t.Fatal("timed out draining client events")
			}
		}
	next:
	}
}
