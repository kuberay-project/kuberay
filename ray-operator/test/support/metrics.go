package support

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/require"
)

// GetMetricsResponseAndCode simulates an HTTP GET request to the /metrics endpoint,
// processes it using a Prometheus handler built from the provided registry,
// and returns the resulting response body as a string along with the HTTP status code.
func GetMetricsResponseAndCode(t *testing.T, reg *prometheus.Registry) (string, int) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "/metrics", nil)
	require.NoError(t, err)

	rr := httptest.NewRecorder()
	handler := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
	handler.ServeHTTP(rr, req)

	return rr.Body.String(), rr.Code
}
