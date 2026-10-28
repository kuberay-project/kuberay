package util

import (
	"context"
	"math"
	"net/http"
	"time"
)

func Sleep(ctx context.Context, sleepDuration time.Duration) error {
	select {
	case <-time.After(sleepDuration):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func CheckContextDeadline(ctx context.Context, sleepDuration time.Duration) bool {
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if sleepDuration > remaining {
			return false
		}
	}
	return true
}

func GetRetryBackoff(attempt int, initBackoff time.Duration, backoffFactor float64, maxBackoff time.Duration) time.Duration {
	sleepDuration := min(initBackoff*time.Duration(math.Pow(backoffFactor, float64(attempt))), maxBackoff)
	return sleepDuration
}

func IsSuccessfulStatusCode(statusCode int) bool {
	return 200 <= statusCode && statusCode < 300
}

func IsRetryableHTTPStatusCodes(statusCode int) bool {
	switch statusCode {
	case http.StatusRequestTimeout, // 408
		http.StatusTooManyRequests,     // 429
		http.StatusInternalServerError, // 500
		http.StatusBadGateway,          // 502
		http.StatusServiceUnavailable,  // 503
		http.StatusGatewayTimeout:      // 504
		return true
	default:
		return false
	}
}
