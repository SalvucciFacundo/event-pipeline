package config

import "testing"

func TestLoadDefaults(t *testing.T) {
	t.Setenv("REDIS_URL", "redis://localhost:6379")
	t.Setenv("EVENT_WORKERS", "")
	t.Setenv("PORT", "")

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	if got.RedisURL != "redis://localhost:6379" {
		t.Errorf("RedisURL = %q, want %q", got.RedisURL, "redis://localhost:6379")
	}
	if got.EventWorkers != DefaultEventWorkers {
		t.Errorf("EventWorkers = %d, want default %d", got.EventWorkers, DefaultEventWorkers)
	}
	if got.Port != DefaultPort {
		t.Errorf("Port = %d, want default %d", got.Port, DefaultPort)
	}
}

func TestLoadOverride(t *testing.T) {
	t.Setenv("REDIS_URL", "redis://redis.internal:6379")
	t.Setenv("EVENT_WORKERS", "8")
	t.Setenv("PORT", "9090")

	got, err := Load()
	if err != nil {
		t.Fatalf("Load() unexpected error: %v", err)
	}
	if got.EventWorkers != 8 {
		t.Errorf("EventWorkers = %d, want 8", got.EventWorkers)
	}
	if got.Port != 9090 {
		t.Errorf("Port = %d, want 9090", got.Port)
	}
}

func TestLoadValidation(t *testing.T) {
	tests := []struct {
		name      string
		redisURL  string
		workers   string
		port      string
		wantError bool
	}{
		{
			name:      "missing REDIS_URL",
			redisURL:  "",
			workers:   "",
			port:      "",
			wantError: true,
		},
		{
			name:      "workers below minimum",
			redisURL:  "redis://localhost:6379",
			workers:   "0",
			port:      "",
			wantError: true,
		},
		{
			name:      "workers above maximum",
			redisURL:  "redis://localhost:6379",
			workers:   "65",
			port:      "",
			wantError: true,
		},
		{
			name:      "workers not an integer",
			redisURL:  "redis://localhost:6379",
			workers:   "many",
			port:      "",
			wantError: true,
		},
		{
			name:      "port not an integer",
			redisURL:  "redis://localhost:6379",
			workers:   "",
			port:      "http",
			wantError: true,
		},
		{
			name:      "minimum workers accepted",
			redisURL:  "redis://localhost:6379",
			workers:   "1",
			port:      "",
			wantError: false,
		},
		{
			name:      "maximum workers accepted",
			redisURL:  "redis://localhost:6379",
			workers:   "64",
			port:      "",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("REDIS_URL", tt.redisURL)
			t.Setenv("EVENT_WORKERS", tt.workers)
			t.Setenv("PORT", tt.port)

			_, err := Load()
			if tt.wantError && err == nil {
				t.Fatal("Load() expected an error, got nil")
			}
			if !tt.wantError && err != nil {
				t.Fatalf("Load() unexpected error: %v", err)
			}
		})
	}
}