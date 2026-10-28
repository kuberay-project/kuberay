package util

import "time"

const (
	// Max retry times for HTTP Client
	HTTPClientDefaultMaxRetry = 3

	// Retry backoff settings
	HTTPClientDefaultBackoffFactor = float64(2)
	HTTPClientDefaultInitBackoff   = 500 * time.Millisecond
	HTTPClientDefaultMaxBackoff    = 10 * time.Second

	// Overall timeout for retries
	HTTPClientDefaultOverallTimeout = 30 * time.Second
)

type RetryConfig struct {
	MaxRetry       int
	BackoffFactor  float64
	InitBackoff    time.Duration
	MaxBackoff     time.Duration
	OverallTimeout time.Duration
}
