// Package worker owns the pipeline's concurrency: cancellable consumers
// over Redis Streams and the supervisor that resizes them live.
package worker

import (
	"context"
	"log/slog"
	"time"

	"event-pipeline/internal/stream"
)

const (
	// batchSize is the maximum number of entries read per XREADGROUP call.
	batchSize = 10
	// blockDuration is how long a worker blocks waiting for new entries.
	blockDuration = 5 * time.Second
	// claimMinIdle is the minimum idle time before a pending entry is
	// considered reclaimable via XAUTOCLAIM.
	claimMinIdle = 10 * time.Second
)

// Run executes one worker: it reclaims pending entries owned by dead
// consumers first (claim-before-read), then loops reading new entries,
// dispatching immutable copies to out, and acknowledging each entry only
// after a successful dispatch. It returns when ctx is canceled; entries
// dispatched-but-unacked at that point stay pending for recovery.
func Run(ctx context.Context, client stream.StreamClient, name string, out chan<- stream.Message, logger *slog.Logger) {
	if claimed, err := client.Claim(ctx, name, claimMinIdle, batchSize); err != nil {
		if ctx.Err() == nil {
			logger.Warn("pending recovery claim failed", "worker", name, "error", err)
		}
	} else {
		for _, m := range claimed {
			if !dispatch(ctx, out, m) {
				return
			}
			if err := client.Ack(ctx, m.ID); err != nil && ctx.Err() == nil {
				logger.Warn("ack failed during recovery", "id", m.ID, "error", err)
			}
		}
	}

	for {
		msgs, err := client.ReadGroup(ctx, name, batchSize, blockDuration)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logger.Error("read failed", "worker", name, "error", err)
			continue
		}
		for _, m := range msgs {
			if !dispatch(ctx, out, m) {
				return
			}
			if err := client.Ack(ctx, m.ID); err != nil && ctx.Err() == nil {
				logger.Warn("ack failed", "id", m.ID, "error", err)
			}
		}
	}
}

// dispatch forwards one message to the aggregation input, or returns false
// when ctx is canceled before the send completes.
func dispatch(ctx context.Context, out chan<- stream.Message, m stream.Message) bool {
	select {
	case out <- m:
		return true
	case <-ctx.Done():
		return false
	}
}