package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"

	"event-pipeline/internal/stream"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func newTestServer(t *testing.T) (*Server, *stream.Fake, *httptest.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	f := stream.NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	reg := newRegistry()
	srv := New(Options{
		Redis:     f,
		Registry:  reg,
		EventTime: func() string { return "2026-01-01T00:00:00Z" },
	})

	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	go func() { srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		srv.Wait()
	})

	return srv, f, ts
}

func TestPostEventsValid(t *testing.T) {
	_, f, ts := newTestServer(t)

	body := `{"type":"click","payload":"{\"x\":1}","occurred_at":"2026-01-01T00:00:00Z","source":"test"}`
	resp, err := http.Post(ts.URL+"/events", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusAccepted)
	}

	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.ID == "" {
		t.Error("response id is empty")
	}

	// The event must have been XADD'd to the fake stream.
	msgs, err := f.ReadGroup(context.Background(), "verify", 10, time.Millisecond)
	if err != nil {
		t.Fatalf("ReadGroup: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 streamed event, got %d", len(msgs))
	}
	if msgs[0].Type != "click" {
		t.Errorf("type = %q, want click", msgs[0].Type)
	}
}

func TestPostEventsValidation(t *testing.T) {
	_, _, ts := newTestServer(t)

	tests := []struct {
		name string
		body string
		want int
	}{
		{name: "missing type", body: `{"payload":"{}"}`, want: http.StatusBadRequest},
		{name: "missing payload", body: `{"type":"click"}`, want: http.StatusBadRequest},
		{name: "missing occurred_at", body: `{"type":"click","payload":"{}"}`, want: http.StatusBadRequest},
		{name: "malformed json", body: `{not json`, want: http.StatusBadRequest},
		{name: "oversized body", body: `{"type":"` + strings.Repeat("a", maxEventBytes+1) + `","payload":"{}","occurred_at":"2026-01-01T00:00:00Z"}`, want: http.StatusRequestEntityTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := http.Post(ts.URL+"/events", "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("Post: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tt.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tt.want)
			}
		})
	}
}

func TestHealthzDegraded(t *testing.T) {
	f := &failingStream{Fake: stream.NewFake()}
	reg := newRegistry()
	srv := New(Options{Redis: f, Registry: reg})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestConfigGetAndPut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := stream.NewFake()
	if err := f.EnsureGroup(ctx); err != nil {
		t.Fatalf("EnsureGroup: %v", err)
	}
	reg := &workerRegistry{
		mu:   sync.Mutex{},
		done: make(chan struct{}),
	}
	var active int32
	reg.set = func(_ context.Context, count int32) (int32, int32, error) {
		if count < 1 || count > 64 {
			return 0, active, errInvalidWorkers
		}
		active = count
		return count, active, nil
	}
	reg.get = func() (int32, int32) { return active, active }

	srv := New(Options{Redis: f, Registry: reg})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/config/workers")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var got map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	resp.Body.Close()
	if got["desired"] != 0 || got["active"] != 0 {
		t.Errorf("initial config = %+v, want desired=0 active=0", got)
	}

	putBody := `{"count":3}`
	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/config/workers", strings.NewReader(putBody))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	putResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if putResp.StatusCode != http.StatusOK {
		t.Errorf("put status = %d, want 200", putResp.StatusCode)
	}
	var out map[string]int
	if err := json.NewDecoder(putResp.Body).Decode(&out); err != nil {
		t.Fatalf("decode put: %v", err)
	}
	putResp.Body.Close()
	if out["desired"] != 3 || out["active"] != 3 {
		t.Errorf("after put = %+v, want desired=3 active=3", out)
	}
}

// failingStream pings as unhealthy.
type failingStream struct {
	*stream.Fake
}

func (f *failingStream) Ping(context.Context) error {
	return context.DeadlineExceeded
}

// newRegistry builds an in-memory worker registry for tests (see server.go
// in the same package; this lets the handler tests run without a supervisor).
func newRegistry() *workerRegistry {
	return &workerRegistry{
		mu:   sync.Mutex{},
		done: make(chan struct{}),
	}
}
