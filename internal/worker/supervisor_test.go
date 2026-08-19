package worker

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"event-pipeline/internal/metrics"
	"event-pipeline/internal/stream"

	"github.com/prometheus/client_golang/prometheus"
)

func newTestSupervisor(t *testing.T, f *stream.Fake) (*Supervisor, chan stream.Message) {
	t.Helper()
	out := make(chan stream.Message, 32)
	reg := prometheus.NewRegistry()
	m := metrics.New(reg)
	s := NewSupervisor(f, out, slog.New(slog.DiscardHandler), m, SupervisorOptions{NamePrefix: "test"})
	return s, out
}

func TestSupervisorScaleUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	s, out := newTestSupervisor(t, f)

	go s.Run(ctx)
	defer func() {
		cancel()
		<-s.Done()
	}()

	desired, active, err := s.Resize(ctx, 3)
	if err != nil {
		t.Fatalf("Resize(3): %v", err)
	}
	if desired != 3 || active != 3 {
		t.Errorf("after resize up: desired=%d active=%d, want 3/3", desired, active)
	}

	// The pool must process every seeded entry (a single worker may drain
	// a burst under XREADGROUP semantics; the supervisor still manages the
	// requested number of workers).
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	const total = 25
	for i := 0; i < total; i++ {
		if _, err := f.Add(ctx, stream.Message{Type: "click", Payload: "{}", OccurredAt: "2026-01-01T00:00:00Z"}); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}

	for i := 0; i < total; i++ {
		select {
		case <-out:
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}

	names := s.WorkerNames()
	if len(names) != 3 {
		t.Errorf("expected exactly 3 active workers, got %v", names)
	}
	seen := map[string]bool{}
	for _, n := range names {
		if seen[n] {
			t.Errorf("duplicate worker name %q", n)
		}
		seen[n] = true
	}
	if len(f.DeliveryConsumers()) == 0 {
		t.Error("no worker delivered any entry")
	}
}

func TestSupervisorScaleDown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	s, _ := newTestSupervisor(t, f)

	go s.Run(ctx)
	defer func() {
		cancel()
		<-s.Done()
	}()

	if _, _, err := s.Resize(ctx, 4); err != nil {
		t.Fatalf("Resize(4): %v", err)
	}
	desired, active, err := s.Resize(ctx, 1)
	if err != nil {
		t.Fatalf("Resize(1): %v", err)
	}
	if desired != 1 || active != 1 {
		t.Errorf("after resize down: desired=%d active=%d, want 1/1", desired, active)
	}
}

func TestSupervisorRejectsInvalidTarget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	s, _ := newTestSupervisor(t, f)
	go s.Run(ctx)
	defer func() {
		cancel()
		<-s.Done()
	}()

	for _, target := range []int32{0, 65, -1} {
		if _, _, err := s.Resize(ctx, target); err == nil {
			t.Errorf("Resize(%d) expected error, got nil", target)
		}
	}
}
