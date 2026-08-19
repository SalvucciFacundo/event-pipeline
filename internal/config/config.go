// Package config loads and validates the event-pipeline configuration
// from the environment, failing fast on invalid values.
package config

import (
	"fmt"
	"os"
	"strconv"
)

const (
	// DefaultEventWorkers is the worker pool size used when EVENT_WORKERS
	// is not set.
	DefaultEventWorkers = 4
	// MinEventWorkers is the lower bound for the worker pool size.
	MinEventWorkers = 1
	// MaxEventWorkers is the upper bound for the worker pool size.
	MaxEventWorkers = 64
	// DefaultPort is the HTTP listen port used when PORT is not set.
	DefaultPort = 8080
)

// Config holds the validated runtime configuration for the service.
type Config struct {
	// RedisURL is the connection string for the shared Redis instance
	// (internal host in Dokploy). Required.
	RedisURL string
	// EventWorkers is the initial worker pool size (1..64).
	EventWorkers int
	// Port is the HTTP listen port.
	Port int
}

// Load reads configuration from the environment and validates it.
func Load() (Config, error) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		return Config{}, fmt.Errorf("REDIS_URL is required")
	}

	workers, err := loadBoundedInt("EVENT_WORKERS", DefaultEventWorkers, MinEventWorkers, MaxEventWorkers)
	if err != nil {
		return Config{}, err
	}

	port, err := loadPort()
	if err != nil {
		return Config{}, err
	}

	return Config{
		RedisURL:     redisURL,
		EventWorkers: workers,
		Port:         port,
	}, nil
}

func loadBoundedInt(name string, def, min, max int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer, got %q", name, raw)
	}
	if v < min || v > max {
		return 0, fmt.Errorf("%s must be within [%d, %d], got %d", name, min, max, v)
	}
	return v, nil
}

func loadPort() (int, error) {
	raw := os.Getenv("PORT")
	if raw == "" {
		return DefaultPort, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("PORT must be an integer, got %q", raw)
	}
	if v < 1 || v > 65535 {
		return 0, fmt.Errorf("PORT must be within [1, 65535], got %d", v)
	}
	return v, nil
}
