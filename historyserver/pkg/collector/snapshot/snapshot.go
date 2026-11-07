package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

type Config struct {
	DashboardAddress    string
	StorageRootDir      string
	RayClusterName      string
	RayClusterNamespace string
	OwnerKind           string
	OwnerName           string
	AdditionalEndpoints []string
}

// Endpoints whose stored responses the history server serves via its
// catch-all getFetchedEndpoint handler.
var servedEndpoints = []string{
	"/api/v0/cluster_metadata",
	"/timezone",
	"/api/serve/applications/",
	"/api/v0/placement_groups?detail=1&limit=10000",
}

// Endpoints stored for data preservation. The history server has specific
// handlers for these paths that currently read from event JSONL, so the
// stored responses are not served yet but the data is preserved.
var preservationEndpoints = []string{
	jobsEndpoint,
	"/api/v0/tasks?detail=1&limit=10000",
	"/logical/actors",
	"/nodes?view=summary",
}

const (
	jobsEndpoint               = "/api/jobs/"
	dataDatasetsEndpointPrefix = "/api/data/datasets/"
	requestTimeout             = 30 * time.Second
)

// Run performs a one-shot scrape of the Ray Dashboard API and writes all
// responses to object storage. It returns nil on full success, or an error
// describing which endpoints failed.
func Run(ctx context.Context, cfg Config, writer storage.StorageWriter) (retErr error) {
	start := time.Now()

	log := logrus.WithFields(logrus.Fields{
		"cluster":   cfg.RayClusterName,
		"namespace": cfg.RayClusterNamespace,
	})

	var (
		storedCount, skippedCount, failedCount int
		markerWritten                          bool
	)
	defer func() {
		outcome := "complete"
		if retErr != nil {
			outcome = "failed"
			if markerWritten {
				outcome = "partial"
			}
		}
		log.WithFields(logrus.Fields{
			"outcome":        outcome,
			"stored":         storedCount,
			"skipped":        skippedCount,
			"failed":         failedCount,
			"marker_written": markerWritten,
			"duration":       time.Since(start).Round(time.Millisecond).String(),
		}).Info("Snapshot finished")
	}()

	log.Info("Snapshot starting")

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:    10,
			IdleConnTimeout: 30 * time.Second,
		},
	}

	sessionName, err := discoverSessionName(ctx, client, cfg.DashboardAddress)
	if err != nil {
		return fmt.Errorf("failed to discover session name: %w", err)
	}
	log = log.WithField("session", sessionName)
	log.Info("Session discovered")

	clusterDir := clusterlogs.Prefix(cfg.StorageRootDir, cfg.OwnerKind, cfg.OwnerName, cfg.RayClusterNamespace, cfg.RayClusterName)

	endpoints := dedup(servedEndpoints, preservationEndpoints, cfg.AdditionalEndpoints)

	var fetchErrors []string
	for _, endpoint := range endpoints {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		stored, err := fetchAndStore(ctx, client, cfg.DashboardAddress, endpoint, clusterDir, sessionName, writer)
		if err != nil {
			failedCount++
			log.WithField("endpoint", endpoint).Warnf("Endpoint fetch failed: %v", err)
			fetchErrors = append(fetchErrors, fmt.Sprintf("%s: %v", endpoint, err))
		} else if stored {
			storedCount++
		} else {
			skippedCount++
		}
	}

	dsStored, dsSkipped, dsErrors := fetchDataDatasets(ctx, client, cfg.DashboardAddress, clusterDir, sessionName, log, writer)
	storedCount += dsStored
	skippedCount += dsSkipped
	failedCount += len(dsErrors)
	fetchErrors = append(fetchErrors, dsErrors...)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if storedCount == 0 {
		return fmt.Errorf("snapshot failed: no endpoints were stored successfully (errors: %s)", strings.Join(fetchErrors, "; "))
	}

	info := utils.ClusterInfo{
		Name:      cfg.RayClusterName,
		Namespace: cfg.RayClusterNamespace,
		OwnerKind: cfg.OwnerKind,
		OwnerName: cfg.OwnerName,
	}
	markerPath := clustermetadata.EncodePath(info, cfg.StorageRootDir, sessionName)
	if err := writer.WriteFile(markerPath, bytes.NewReader([]byte{})); err != nil {
		return fmt.Errorf("failed to write cluster metadata marker at %s: %w", markerPath, err)
	}
	markerWritten = true
	log.WithField("path", markerPath).Info("Session marker written")

	if len(fetchErrors) > 0 {
		return fmt.Errorf("snapshot completed with %d endpoint errors: %s", len(fetchErrors), strings.Join(fetchErrors, "; "))
	}

	return nil
}

const versionEndpoint = "/api/version"

func discoverSessionName(ctx context.Context, client *http.Client, dashboardAddress string) (string, error) {
	body, err := fetchEndpoint(ctx, client, dashboardAddress, versionEndpoint)
	if err != nil {
		return "", fmt.Errorf("dashboard unreachable: %w", err)
	}

	var resp struct {
		SessionName string `json:"session_name"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("failed to parse %s response: %w", versionEndpoint, err)
	}

	if resp.SessionName == "" {
		return "", fmt.Errorf("%s response did not include session_name", versionEndpoint)
	}
	return resp.SessionName, nil
}

func fetchAndStore(ctx context.Context, client *http.Client, dashboardAddress, endpoint, clusterDir, sessionName string, writer storage.StorageWriter) (bool, error) {
	body, err := fetchEndpoint(ctx, client, dashboardAddress, endpoint)
	if err != nil {
		return false, err
	}
	if len(body) == 0 {
		logrus.Debugf("Skipping empty response from %s", endpoint)
		return false, nil
	}

	storageKey := utils.EndpointPathToStorageKey(endpoint)
	objectKey := path.Join(clusterlogs.FetchedEndpointsDir(clusterDir, sessionName), storageKey)
	if err := writer.WriteFile(objectKey, bytes.NewReader(body)); err != nil {
		return false, fmt.Errorf("failed to store at %s: %w", objectKey, err)
	}
	logrus.Infof("Stored %s at %s (%d bytes)", endpoint, objectKey, len(body))
	return true, nil
}

func fetchDataDatasets(ctx context.Context, client *http.Client, dashboardAddress, clusterDir, sessionName string, log *logrus.Entry, writer storage.StorageWriter) (stored, skipped int, errs []string) {
	body, err := fetchEndpoint(ctx, client, dashboardAddress, jobsEndpoint)
	if err != nil {
		errs = append(errs, fmt.Sprintf("dataset discovery: failed to fetch job list: %v", err))
		return
	}

	var jobs []struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(body, &jobs); err != nil {
		errs = append(errs, fmt.Sprintf("dataset discovery: failed to parse job list: %v", err))
		return
	}

	for _, job := range jobs {
		if job.JobID == "" {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		endpoint := dataDatasetsEndpointPrefix + job.JobID
		ok, fetchErr := fetchAndStore(ctx, client, dashboardAddress, endpoint, clusterDir, sessionName, writer)
		if fetchErr != nil {
			log.WithField("endpoint", endpoint).Warnf("Dataset fetch failed: %v", fetchErr)
			errs = append(errs, fmt.Sprintf("%s: %v", endpoint, fetchErr))
		} else if ok {
			stored++
		} else {
			skipped++
		}
	}
	return
}

func fetchEndpoint(ctx context.Context, client *http.Client, dashboardAddress, endpoint string) ([]byte, error) {
	url := dashboardAddress + endpoint

	reqCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if err := utils.SetRayAuthHeader(req); err != nil {
		return nil, fmt.Errorf("failed to authenticate request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if utils.IsAuthFailure(resp.StatusCode) {
		return nil, fmt.Errorf("authentication failed with status %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from %s", resp.StatusCode, endpoint)
	}
	return body, nil
}

func dedup(lists ...[]string) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, list := range lists {
		for _, s := range list {
			if _, ok := seen[s]; !ok {
				seen[s] = struct{}{}
				result = append(result, s)
			}
		}
	}
	return result
}
