# KubeRay APIServer Retry Behavior

The KubeRay APIServer automatically retries failed requests to the Kubernetes API when transient errors occur.
This built-in mechanism uses exponential backoff to improve reliability without requiring manual intervention.
As of `v1.5.0`, the retry configuration is hard-coded and cannot be customized.
This guide describes the default retry behavior.

## Default Retry Behavior

The KubeRay APIServer automatically retries with exponential backoff for these HTTP status codes:

- 408 (Request Timeout)
- 429 (Too Many Requests)
- 500 (Internal Server Error)
- 502 (Bad Gateway)
- 503 (Service Unavailable)
- 504 (Gateway Timeout)

Note that non-retryable errors (4xx except 408/429) fail immediately without retries.

The following default configuration explains how retry works:

- **MaxRetry**: 3 retries (4 total attempts including the initial one)
- **InitBackoff**: 500ms (initial wait time)
- **BackoffFactor**: 2.0 (exponential multiplier)
- **MaxBackoff**: 10s (maximum wait time between retries)
- **OverallTimeout**: 30s (total timeout for all attempts)

which means $$\text{Backoff}_i = \min(\text{InitBackoff} \times \text{BackoffFactor}^i, \text{MaxBackoff})$$

where $i$ is the attempt number (starting from 0).
The retries will stop if the total time exceeds the `OverallTimeout`.
