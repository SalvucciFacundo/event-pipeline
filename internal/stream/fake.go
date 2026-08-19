package stream

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fake is a deterministic, concurrency-safe in-memory StreamClient used by
// unit tests. It does not spawn goroutines; blocking reads wait on a
// notification channel owned by the caller's goroutine.
type Fake struct {
	mu        sync.Mutex
	entries   []Message
	acked     map[string]bool
	owners    map[string]string
	consumers map[string]bool
	group     bool
	seq       int
	notify    chan struct{}
}

// NewFake returns an empty fake transport.
func NewFake() *Fake {
	return &Fake{
		acked:     map[string]bool{},
		owners:    map[string]string{},
		consumers: map[string]bool{},
		notify:    make(chan struct{}),
	}
}

// EnsureGroup is idempotent.
func (f *Fake) EnsureGroup(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.group = true
	return nil
}

// Add appends a message with the next sequential ID ("N-0").
func (f *Fake) Add(_ context.Context, msg Message) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.group {
		return "", fmt.Errorf("stream group not created")
	}
	f.seq++
	id := fmt.Sprintf("%d-0", f.seq)
	msg.ID = id
	f.entries = append(f.entries, msg)
	f.wakeLocked()
	return id, nil
}

// ReadGroup delivers new (never-delivered) entries to consumer, matching
// XREADGROUP ">" semantics. It blocks on the notification channel when the
// stream is empty, honoring block and ctx.
func (f *Fake) ReadGroup(ctx context.Context, consumer string, count int, block time.Duration) ([]Message, error) {
	var timerC <-chan time.Time
	if block > 0 {
		timer := time.NewTimer(block)
		defer timer.Stop()
		timerC = timer.C
	}

	for {
		f.mu.Lock()
		if !f.group {
			f.mu.Unlock()
			return nil, fmt.Errorf("stream group not created")
		}
		var candidates []Message
		for _, m := range f.entries {
			if f.acked[m.ID] {
				continue
			}
			if _, owned := f.owners[m.ID]; owned {
				continue
			}
			candidates = append(candidates, m)
			if len(candidates) == count {
				break
			}
		}
		if len(candidates) > 0 {
			for _, m := range candidates {
				f.owners[m.ID] = consumer
				f.consumers[consumer] = true
			}
			f.mu.Unlock()
			return candidates, nil
		}
		notify := f.notify
		f.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timerC:
			return nil, nil
		case <-notify:
		}
	}
}

// Ack marks IDs as processed.
func (f *Fake) Ack(_ context.Context, ids ...string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range ids {
		f.acked[id] = true
		delete(f.owners, id)
	}
	return nil
}

// Range replays messages with ID strictly greater than fromExclusive,
// up to to ("+" for tail), limited to count.
func (f *Fake) Range(_ context.Context, fromExclusive, to string, count int) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []Message
	for _, m := range f.entries {
		if fromExclusive != "" && idSeq(m.ID) <= idSeq(fromExclusive) {
			continue
		}
		if to != "+" && idSeq(m.ID) > idSeq(to) {
			break
		}
		out = append(out, m)
		if len(out) == count {
			break
		}
	}
	return out, nil
}

// Claim reassigns pending messages owned by other consumers to consumer.
func (f *Fake) Claim(_ context.Context, consumer string, _ time.Duration, count int) ([]Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []Message
	for _, m := range f.entries {
		if f.acked[m.ID] {
			continue
		}
		owner, owned := f.owners[m.ID]
		if owned && owner != consumer {
			f.owners[m.ID] = consumer
			f.consumers[consumer] = true
			out = append(out, m)
			if len(out) == count {
				break
			}
		}
	}
	return out, nil
}

// Close is a no-op for the fake.
func (f *Fake) Close() error { return nil }

// Ping reports health for the fake transport.
func (f *Fake) Ping(context.Context) error { return nil }

// IsAcked reports whether an ID has been acknowledged (test helper).
func (f *Fake) IsAcked(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.acked[id]
}

// DeliveryConsumers returns the distinct consumers that have read at least
// one entry (test helper). Names persist even after the entries are acked.
func (f *Fake) DeliveryConsumers() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for consumer := range f.consumers {
		out = append(out, consumer)
	}
	sort.Strings(out)
	return out
}

// idSeq parses the numeric part of a fake stream ID ("N-0") for ordering.
func idSeq(id string) int {
	part, _, _ := strings.Cut(id, "-")
	n, _ := strconv.Atoi(part)
	return n
}

// wakeLocked notifies blocked readers that new data may be available.
// Callers must hold f.mu.
func (f *Fake) wakeLocked() {
	close(f.notify)
	f.notify = make(chan struct{})
}
