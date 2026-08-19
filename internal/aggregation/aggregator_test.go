package aggregation

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

// fakeClock is a controllable clock for deterministic window tests.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newTestAggregator(t *testing.T, clock func() time.Time) (*Aggregator, chan stream.Message) {
	t.Helper()
	input := make(chan stream.Message, 16)
	opts := Options{
		Clock:        clock,
		TickInterval: 10 * time.Millisecond,
		// Retention must be >= 1s: cleanup works on second-granularity
		// window keys.
		Retention: 2 * time.Second,
	}
	return New(input, opts), input
}

func msg(t string) stream.Message {
	return stream.Message{Type: t, Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"}
}

// waitSnapshot reads snapshots until the predicate holds. The aggregator
// may publish intermediate states (ticker + coalescing), so callers must
// wait for the state they actually care about.
func waitSnapshot(t *testing.T, agg *Aggregator, pred func(Snapshot) bool) Snapshot {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case snap := <-agg.Snapshots():
			if pred(snap) {
				return snap
			}
		case <-deadline:
			t.Fatal("timed out waiting for snapshot predicate")
		}
	}
}

func TestAggregatorCounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	agg, input := newTestAggregator(t, clock.Now)

	done := make(chan struct{})
	go func() {
		defer close(done)
		agg.Run(ctx)
	}()

	input <- msg("click")
	input <- msg("click")
	input <- msg("view")

	snap := waitSnapshot(t, agg, func(s Snapshot) bool {
		return s.Totals["click"] == 2 && s.Totals["view"] == 1
	})

	sec := clock.Now().UTC().Unix()
	min := clock.Now().UTC().Unix() / 60

	if got := snap.PerSecond[sec]["click"]; got != 2 {
		t.Errorf("per-second click = %d, want 2", got)
	}
	if got := snap.PerSecond[sec]["view"]; got != 1 {
		t.Errorf("per-second view = %d, want 1", got)
	}
	if got := snap.PerMinute[min]["click"]; got != 2 {
		t.Errorf("per-minute click = %d, want 2", got)
	}
	if snap.Totals["click"] != 2 || snap.Totals["view"] != 1 {
		t.Errorf("totals = %+v, want click=2 view=1", snap.Totals)
	}

	cancel()
	<-done
}

func TestAggregatorWindowExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	agg, input := newTestAggregator(t, clock.Now)

	done := make(chan struct{})
	go func() {
		defer close(done)
		agg.Run(ctx)
	}()

	sec := clock.Now().UTC().Unix()
	input <- msg("click")

	// Wait for the window to be visible in a published snapshot.
	waitSnapshot(t, agg, func(s Snapshot) bool {
		return s.PerSecond[sec] != nil
	})

	// Advance past retention and wait for cleanup to publish an empty set.
	clock.Advance(3 * time.Second)
	waitSnapshot(t, agg, func(s Snapshot) bool {
		return len(s.PerSecond) == 0
	})

	cancel()
	<-done
}

func TestAggregatorFlushesOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	agg, input := newTestAggregator(t, clock.Now)

	done := make(chan struct{})
	go func() {
		defer close(done)
		agg.Run(ctx)
	}()

	input <- msg("click")
	// Prove the message was recorded before canceling.
	waitSnapshot(t, agg, func(s Snapshot) bool {
		return s.Totals["click"] == 1
	})

	cancel()
	<-done

	// The final flush after cancellation must still carry the totals.
	waitSnapshot(t, agg, func(s Snapshot) bool {
		return s.Totals["click"] == 1
	})
}

func TestAggregatorCoalescesWhenConsumerSlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	agg, input := newTestAggregator(t, clock.Now)

	done := make(chan struct{})
	go func() {
		defer close(done)
		agg.Run(ctx)
	}()

	// No one reads Snapshots(); publish must drop rather than block.
	for i := 0; i < 50; i++ {
		input <- msg("click")
		clock.Advance(20 * time.Millisecond)
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done
}
