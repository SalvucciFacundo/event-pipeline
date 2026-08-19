package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"sync/atomic"

	"event-pipeline/internal/metrics"
	"event-pipeline/internal/stream"
)

const (
	// MinWorkers and MaxWorkers mirror the config validation bounds.
	MinWorkers = 1
	MaxWorkers = 64
)

// SupervisorOptions configures a Supervisor.
type SupervisorOptions struct {
	// NamePrefix overrides the worker consumer-name prefix. Tests set it
	// to a deterministic value; production defaults to the hostname.
	NamePrefix string
}

// Supervisor is the lifecycle owner of the worker pool. It tracks a
// desired target and the actually active count, starts workers until the
// target is reached, and cancels the newest workers (waiting for exit)
// when the target shrinks. All resize requests are serialized through a
// control channel consumed by Run.
type Supervisor struct {
	client  stream.StreamClient
	out     chan<- stream.Message
	logger  *slog.Logger
	metrics *metrics.Metrics
	prefix  string

	target atomic.Int32
	active atomic.Int32
	seq    atomic.Int32

	mu      sync.Mutex
	workers map[string]*managedWorker
	order   []string

	control chan resizeReq
	done    chan struct{}
}

type managedWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type resizeReq struct {
	target int32
	done   chan struct{}
}

// NewSupervisor creates a supervisor with zero workers. Call Run to start
// the control loop, then Resize to reach the desired pool size.
func NewSupervisor(client stream.StreamClient, out chan<- stream.Message, logger *slog.Logger, m *metrics.Metrics, opts SupervisorOptions) *Supervisor {
	prefix := opts.NamePrefix
	if prefix == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "event-pipeline"
		}
		prefix = host
	}
	return &Supervisor{
		client:  client,
		out:     out,
		logger:  logger,
		metrics: m,
		prefix:  prefix,
		workers: map[string]*managedWorker{},
		control: make(chan resizeReq),
		done:    make(chan struct{}),
	}
}

// Run starts the control loop and blocks until ctx is canceled, at which
// point it cancels every worker and waits for them to exit.
func (s *Supervisor) Run(ctx context.Context) {
	defer close(s.done)

	if err := s.client.EnsureGroup(ctx); err != nil {
		s.logger.Error("ensure stream group failed", "error", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			s.shutdown()
			return
		case req := <-s.control:
			s.reconcile(ctx, req.target)
			close(req.done)
		}
	}
}

// Done is closed when Run returns.
func (s *Supervisor) Done() <-chan struct{} {
	return s.done
}

// Resize sets a new target and waits until the pool has reconciled to it.
// It returns the desired and active counts. Targets outside the bounds
// are rejected.
func (s *Supervisor) Resize(ctx context.Context, target int32) (int32, int32, error) {
	if target < MinWorkers || target > MaxWorkers {
		return s.target.Load(), s.active.Load(),
			fmt.Errorf("worker target must be within [%d, %d], got %d", MinWorkers, MaxWorkers, target)
	}
	s.target.Store(target)

	req := resizeReq{target: target, done: make(chan struct{})}
	select {
	case s.control <- req:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	select {
	case <-req.done:
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
	return s.target.Load(), s.active.Load(), nil
}

// Desired returns the current target count.
func (s *Supervisor) Desired() int32 { return s.target.Load() }

// Active returns the current active count.
func (s *Supervisor) Active() int32 { return s.active.Load() }

// WorkerNames returns the sorted consumer names of the active workers.
func (s *Supervisor) WorkerNames() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.workers))
	for name := range s.workers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (s *Supervisor) reconcile(ctx context.Context, target int32) {
	for s.active.Load() < target {
		s.startWorker(ctx)
	}
	for s.active.Load() > target {
		s.stopNewest(ctx)
	}
}

func (s *Supervisor) startWorker(ctx context.Context) {
	name := s.nextName()
	wctx, cancel := context.WithCancel(ctx)
	w := &managedWorker{cancel: cancel, done: make(chan struct{})}

	s.mu.Lock()
	s.workers[name] = w
	s.order = append(s.order, name)
	s.mu.Unlock()

	go func() {
		defer close(w.done)
		Run(wctx, s.client, name, s.out, s.logger)
	}()

	select {
	case <-ctx.Done():
		cancel()
		<-w.done
		s.removeWorker(name)
	default:
		s.active.Add(1)
		s.metrics.ActiveWorkers.Set(float64(s.active.Load()))
	}
}

func (s *Supervisor) stopNewest(ctx context.Context) {
	s.mu.Lock()
	if len(s.order) == 0 {
		s.mu.Unlock()
		return
	}
	last := len(s.order) - 1
	name := s.order[last]
	w := s.workers[name]
	s.order = s.order[:last]
	delete(s.workers, name)
	s.mu.Unlock()

	w.cancel()
	select {
	case <-w.done:
		s.active.Add(-1)
		s.metrics.ActiveWorkers.Set(float64(s.active.Load()))
	case <-ctx.Done():
	}
}

func (s *Supervisor) shutdown() {
	s.mu.Lock()
	workers := make([]*managedWorker, 0, len(s.workers))
	for _, w := range s.workers {
		workers = append(workers, w)
	}
	s.workers = map[string]*managedWorker{}
	s.order = nil
	s.mu.Unlock()

	for _, w := range workers {
		w.cancel()
	}
	for _, w := range workers {
		<-w.done
	}
	s.active.Store(0)
	s.metrics.ActiveWorkers.Set(0)
}

func (s *Supervisor) removeWorker(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.workers, name)
	for i, n := range s.order {
		if n == name {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
}

func (s *Supervisor) nextName() string {
	n := s.seq.Add(1)
	return fmt.Sprintf("%s-%d-%s", s.prefix, n, randHex(4))
}

func randHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		panic(err) // crypto/rand failure is unrecoverable
	}
	return hex.EncodeToString(buf)
}