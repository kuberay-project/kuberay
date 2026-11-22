package logcollector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const (
	defaultRetryInterval    = 5 * time.Second
	defaultMaxRetryInterval = 60 * time.Second
	defaultRequestTimeout   = 30 * time.Second
)

// endpointFetchConfig holds the configuration for fetching and storing a
// single Ray Dashboard endpoint with exponential-backoff retry.
type endpointFetchConfig struct {
	// endpoint is the URL path on the Ray Dashboard, e.g. "/timezone".
	endpoint string

	// requestTimeout is the per-request HTTP timeout.
	requestTimeout time.Duration

	// retryInterval is the initial wait between retries.
	retryInterval time.Duration

	// maxRetryInterval caps the exponential backoff.
	maxRetryInterval time.Duration
}

// fetchAndStoreEndpoint fetches the given dashboard endpoint once on startup
// and stores the raw response body in storage under the current session.
// It retries with exponential backoff until success or shutdown.
func (r *RayLogHandler) fetchAndStoreEndpoint(cfg endpointFetchConfig) {
	url := r.DashboardAddress + cfg.endpoint
	retryInterval := cfg.retryInterval

	// Resolve the session name first so we can store data under the correct session path.
	// Note: session name staleness is not a concern here because the log collector runs as a
	// sidecar in the Ray head pod. If the Ray head process restarts (creating a new session),
	// the entire pod — including this sidecar container — restarts, so resolveSessionName()
	// always runs in a fresh container lifecycle with the current session.
	sessionName, err := r.resolveSessionName()
	if err != nil {
		logrus.Errorf("Failed to resolve session name for %s: %v", cfg.endpoint, err)
		return
	}
	logrus.Infof("Resolved session name for %s: %s", cfg.endpoint, sessionName)

	for {
		logrus.Infof("Fetching %s from %s", cfg.endpoint, url)

		ctx, cancel := context.WithTimeout(context.Background(), cfg.requestTimeout)
		// Listen for shutdown to cancel in-flight request.
		go func() {
			select {
			case <-r.ShutdownChan:
				cancel()
			case <-ctx.Done():
			}
		}()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			cancel()
			logrus.Errorf("Failed to create request for %s: %v", cfg.endpoint, err)
			return
		}

		if err := utils.SetRayAuthHeader(req); err != nil {
			cancel()
			logrus.Errorf("Failed to authenticate request for %s: %v", cfg.endpoint, err)
			return
		}

		client := r.HttpClient
		if client == nil {
			client = http.DefaultClient
		}

		resp, err := client.Do(req)
		if err != nil {
			cancel()
			logrus.Warnf("Failed to fetch %s from %s: %v, retrying in %v", cfg.endpoint, url, err, retryInterval)
			if !r.sleepOrShutdown(retryInterval) {
				return
			}
			retryInterval = nextBackoff(retryInterval, cfg.maxRetryInterval)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()

		if err != nil {
			logrus.Warnf("Failed to read %s response body: %v, retrying in %v", cfg.endpoint, err, retryInterval)
			if !r.sleepOrShutdown(retryInterval) {
				return
			}
			retryInterval = nextBackoff(retryInterval, cfg.maxRetryInterval)
			continue
		}

		// Unlike a Dashboard that is still starting up, a rejected credential never resolves on
		// its own. Retrying until shutdown would bury the cause under an endless warning loop.
		if utils.IsAuthFailure(resp.StatusCode) {
			logrus.Errorf("%s returned status %d: the collector is not authenticated against the Ray Dashboard, giving up on this endpoint. Check that token auth is configured for the collector container.", cfg.endpoint, resp.StatusCode)
			return
		}

		if resp.StatusCode != http.StatusOK {
			logrus.Warnf("%s returned status %d, retrying in %v", cfg.endpoint, resp.StatusCode, retryInterval)
			if !r.sleepOrShutdown(retryInterval) {
				return
			}
			retryInterval = nextBackoff(retryInterval, cfg.maxRetryInterval)
			continue
		}

		if len(body) == 0 {
			logrus.Warnf("%s response from %s is empty, retrying in %v", cfg.endpoint, url, retryInterval)
			if !r.sleepOrShutdown(retryInterval) {
				return
			}
			retryInterval = nextBackoff(retryInterval, cfg.maxRetryInterval)
			continue
		}

		// Successfully fetched — store it under the session path
		storageKey := utils.EndpointPathToStorageKey(cfg.endpoint)
		objectKey := path.Join(clusterlogs.FetchedEndpointsDir(r.ClusterDir, sessionName), storageKey)
		if err := r.Writer.WriteFile(objectKey, bytes.NewReader(body)); err != nil {
			logrus.Errorf("Failed to store %s at %s: %v", cfg.endpoint, objectKey, err)
			if !r.sleepOrShutdown(retryInterval) {
				return
			}
			retryInterval = nextBackoff(retryInterval, cfg.maxRetryInterval)
			continue
		}

		logrus.Infof("Successfully stored %s at %s (%d bytes)", cfg.endpoint, objectKey, len(body))
		return
	}
}

// resolveSessionName waits for the session_latest symlink to appear and resolves
// the session name from it. It retries with exponential backoff.
func (r *RayLogHandler) resolveSessionName() (string, error) {
	sessionLatestDir := utils.GetRaySessionLatestPath()
	retryInterval := defaultRetryInterval

	for {
		sessionRealDir, err := filepath.EvalSymlinks(sessionLatestDir)
		if err == nil {
			return filepath.Base(sessionRealDir), nil
		}

		logrus.Warnf("session_latest symlink not ready: %v, retrying in %v", err, retryInterval)
		if !r.sleepOrShutdown(retryInterval) {
			return "", fmt.Errorf("shutdown signaled while waiting for session_latest")
		}
		retryInterval = nextBackoff(retryInterval, defaultMaxRetryInterval)
	}
}

// sleepOrShutdown sleeps for the given duration or returns false if shutdown is signaled.
func (r *RayLogHandler) sleepOrShutdown(d time.Duration) bool {
	select {
	case <-r.ShutdownChan:
		logrus.Info("Shutdown signaled, aborting endpoint fetch")
		return false
	case <-time.After(d):
		return true
	}
}

// nextBackoff doubles the interval up to maxInterval.
func nextBackoff(current, maxInterval time.Duration) time.Duration {
	next := min(current*2, maxInterval)
	return next
}
