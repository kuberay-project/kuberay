package metrics

import (
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

func init() {
	ctrlmetrics.RegisterRESTClientMetrics(
		ctrlmetrics.MetricRequestLatency,
		ctrlmetrics.MetricDNSResolutionLatency,
		ctrlmetrics.MetricRequestSize,
		ctrlmetrics.MetricResponseSize,
		ctrlmetrics.MetricRateLimiterLatency,
		ctrlmetrics.MetricRequestRetry,
	)
}
