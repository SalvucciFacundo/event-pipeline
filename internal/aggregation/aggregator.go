// Package aggregation owns the windowed counters. A single goroutine owns
// all counter state; workers send immutable values over the input channel
// and receive immutable snapshots, so no mutex is needed on the hot path.
package aggregation

import (
	"context"
	"maps"
	"time"

	"event-pipeline/internal/stream"
)

// Options configures the aggregator. Tests use tiny intervals and a fake
// clock for deterministic window behavior.
type Options struct {
	// Clock returns the current time; defaults to time.Now.
	Clock func() time.Time
	// TickInterval is the snapshot+cleanup cadence; defaults to 1s.
	TickInterval time.Duration
	// Retention is how long windows are kept before cleanup; defaults to 5m.
	Retention time.Duration
	// SnapshotBuffer is the snapshots channel buffer; defaults to 8.
	SnapshotBuffer int
}

// Snapshot is an immutable copy of the current windowed counters.
type Snapshot struct {
	At        time.Time
	PerSecond map[int64]map[string]int64 // unix-second -> type -> count
	PerMinute map[int64]map[string]int64 // unix-minute -> type -> count
	Totals    map[string]int64
}

// Aggregator receives stream messages and publishes windowed snapshots.
// Only its Run goroutine touches the counter maps.
type Aggregator struct {
	input     <-chan stream.Message
	snapshots chan Snapshot
	done      chan struct{}
	clock     func() time.Time
	tickEvery time.Duration
	retention time.Duration

	secCounts map[int64]map[string]int64
	minCounts map[int64]map[string]int64
	totals    map[string]int64
}

// New creates an aggregator over the given input channel.
func New(input <-chan stream.Message, opts Options) *Aggregator {
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if opts.TickInterval <= 0 {
		opts.TickInterval = time.Second
	}
	if opts.Retention <= 0 {
		opts.Retention = 5 * time.Minute
	}
	if opts.SnapshotBuffer <= 0 {
		opts.SnapshotBuffer = 8
	}
	return &Aggregator{
		input:     input,
		snapshots: make(chan Snapshot, opts.SnapshotBuffer),
		done:      make(chan struct{}),
		clock:     opts.Clock,
		tickEvery: opts.TickInterval,
		retention: opts.Retention,
		secCounts: map[int64]map[string]int64{},
		minCounts: map[int64]map[string]int64{},
		totals:    map[string]int64{},
	}
}

// Snapshots returns the immutable snapshot stream.
func (a *Aggregator) Snapshots() <-chan Snapshot {
	return a.snapshots
}

// Done is closed when Run returns.
func (a *Aggregator) Done() <-chan struct{} {
	return a.done
}

// Run owns the counter state until ctx is canceled, then flushes one final
// snapshot and closes Done. Callers must not call Run more than once.
func (a *Aggregator) Run(ctx context.Context) {
	defer close(a.done)
	ticker := time.NewTicker(a.tickEvery)
	defer ticker.Stop()

	for {
		select {
		case m, ok := <-a.input:
			if !ok {
				return
			}
			a.record(m)
		case <-ticker.C:
			a.cleanup()
			a.publish()
		case <-ctx.Done():
			a.publish()
			return
		}
	}
}

func (a *Aggregator) record(m stream.Message) {
	now := a.clock().UTC()
	sec := now.Unix()
	min := now.Unix() / 60

	inc(a.secCounts, sec, m.Type)
	inc(a.minCounts, min, m.Type)
	a.totals[m.Type]++
}

func inc(counts map[int64]map[string]int64, window int64, eventType string) {
	types := counts[window]
	if types == nil {
		types = map[string]int64{}
		counts[window] = types
	}
	types[eventType]++
}

func (a *Aggregator) cleanup() {
	now := a.clock().UTC()
	// Window keys have different granularities: seconds vs minutes.
	secCutoff := now.Unix() - int64(a.retention/time.Second)
	minCutoff := now.Unix()/60 - int64(a.retention/time.Minute)
	for window := range a.secCounts {
		if window < secCutoff {
			delete(a.secCounts, window)
		}
	}
	for window := range a.minCounts {
		if window < minCutoff {
			delete(a.minCounts, window)
		}
	}
}

func (a *Aggregator) publish() {
	snap := Snapshot{
		At:        a.clock().UTC(),
		PerSecond: deepCopy(a.secCounts),
		PerMinute: deepCopy(a.minCounts),
		Totals:    maps.Clone(a.totals),
	}
	// Coalesce: if the consumer is slow, drop the intermediate snapshot
	// rather than blocking the pipeline.
	select {
	case a.snapshots <- snap:
	default:
	}
}

func deepCopy(src map[int64]map[string]int64) map[int64]map[string]int64 {
	dst := make(map[int64]map[string]int64, len(src))
	for window, types := range src {
		dst[window] = maps.Clone(types)
	}
	return dst
}
