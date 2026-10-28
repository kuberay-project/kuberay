package logcollector

// Startup endpoints are Ray Dashboard endpoints fetched once on startup
// with exponential-backoff retry. They are stored per session under
// fetched_endpoints/ in storage.
//
// To add a new startup endpoint, define the constant and wrapper here,
// then register it with "go r.FetchAndStore...()" in Run().

const (
	clusterMetadataEndpoint = "/api/v0/cluster_metadata"
	timezoneEndpoint        = "/timezone"
)

// FetchAndStoreClusterMetadata fetches /api/v0/cluster_metadata from the Ray Dashboard
// once on startup and stores the result in storage per session. It retries with exponential
// backoff until the fetch succeeds or the collector is shut down.
//
// The metadata is stored per session (not per cluster) because different sessions can use
// different Ray images, resulting in different rayVersion / pythonVersion values.
func (r *RayLogHandler) FetchAndStoreClusterMetadata() {
	r.fetchAndStoreEndpoint(endpointFetchConfig{
		endpoint:         clusterMetadataEndpoint,
		requestTimeout:   defaultRequestTimeout,
		retryInterval:    defaultRetryInterval,
		maxRetryInterval: defaultMaxRetryInterval,
	})
}

// FetchAndStoreTimezone fetches /timezone from the Ray Dashboard once on startup
// and stores the result in storage per session. It retries with exponential
// backoff until the fetch succeeds or the collector is shut down.
func (r *RayLogHandler) FetchAndStoreTimezone() {
	r.fetchAndStoreEndpoint(endpointFetchConfig{
		endpoint:         timezoneEndpoint,
		requestTimeout:   defaultRequestTimeout,
		retryInterval:    defaultRetryInterval,
		maxRetryInterval: defaultMaxRetryInterval,
	})
}
