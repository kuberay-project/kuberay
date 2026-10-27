package snapshot

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
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
	"/nodes",
}

const (
	jobsEndpoint               = "/api/jobs/"
	dataDatasetsEndpointPrefix = "/api/data/datasets/"
	requestTimeout             = 30 * time.Second
)

// Run performs a one-shot scrape of the Ray Dashboard API and writes all
// responses to object storage. It returns nil on full success, or an error
// describing which endpoints failed.
func Run(ctx context.Context, cfg Config, writer storage.StorageWriter) error {
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
	logrus.Infof("Using session name: %s", sessionName)

	clusterDir := clusterlogs.Prefix(cfg.StorageRootDir, cfg.OwnerKind, cfg.OwnerName, cfg.RayClusterNamespace, cfg.RayClusterName)

	endpoints := dedup(servedEndpoints, preservationEndpoints, cfg.AdditionalEndpoints)

	var fetchErrors []string
	storedCount := 0
	for _, endpoint := range endpoints {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		stored, err := fetchAndStore(ctx, client, cfg.DashboardAddress, endpoint, clusterDir, sessionName, writer)
		if err != nil {
			logrus.Warnf("Failed to fetch endpoint %s: %v", endpoint, err)
			fetchErrors = append(fetchErrors, fmt.Sprintf("%s: %v", endpoint, err))
		} else if stored {
			storedCount++
		}
	}

	if err := fetchDataDatasets(ctx, client, cfg.DashboardAddress, clusterDir, sessionName, writer); err != nil {
		logrus.Warnf("Failed to fetch data datasets: %v", err)
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	if storedCount == 0 {
		return fmt.Errorf("snapshot failed: no endpoints were stored successfully (errors: %s)", strings.Join(fetchErrors, "; "))
	}

	if ctx.Err() != nil {
		return ctx.Err()
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
	logrus.Infof("Wrote cluster metadata marker at %s", markerPath)

	if len(fetchErrors) > 0 {
		return fmt.Errorf("snapshot completed with %d endpoint errors: %s", len(fetchErrors), strings.Join(fetchErrors, "; "))
	}

	logrus.Info("Snapshot completed successfully")
	return nil
}

const nodesViewSummaryEndpoint = "/nodes?view=summary"

func discoverSessionName(ctx context.Context, client *http.Client, dashboardAddress string) (string, error) {
	body, err := fetchEndpoint(ctx, client, dashboardAddress, nodesViewSummaryEndpoint)
	if err != nil {
		return "", fmt.Errorf("dashboard unreachable: %w", err)
	}

	var resp struct {
		Data struct {
			Summary []struct {
				Raylet struct {
					RayletSocketName string `json:"rayletSocketName"`
				} `json:"raylet"`
			} `json:"summary"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", fmt.Errorf("failed to parse %s response: %w", nodesViewSummaryEndpoint, err)
	}

	for _, node := range resp.Data.Summary {
		name := sessionNameFromSocketPath(node.Raylet.RayletSocketName)
		if name != "" {
			return name, nil
		}
	}
	return "", fmt.Errorf("no node in %s has a rayletSocketName containing a session_ directory", nodesViewSummaryEndpoint)
}

func sessionNameFromSocketPath(socketPath string) string {
	for dir := socketPath; dir != "/" && dir != "."; dir = filepath.Dir(dir) {
		base := filepath.Base(dir)
		if strings.HasPrefix(base, "session_") {
			return base
		}
	}
	return ""
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

func fetchDataDatasets(ctx context.Context, client *http.Client, dashboardAddress, clusterDir, sessionName string, writer storage.StorageWriter) error {
	body, err := fetchEndpoint(ctx, client, dashboardAddress, jobsEndpoint)
	if err != nil {
		return fmt.Errorf("failed to fetch %s: %w", jobsEndpoint, err)
	}

	var jobs []struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(body, &jobs); err != nil {
		return fmt.Errorf("failed to parse %s response: %w", jobsEndpoint, err)
	}

	for _, job := range jobs {
		if job.JobID == "" {
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		endpoint := dataDatasetsEndpointPrefix + job.JobID
		if _, err := fetchAndStore(ctx, client, dashboardAddress, endpoint, clusterDir, sessionName, writer); err != nil {
			logrus.Warnf("Failed to fetch dataset for job %s: %v", job.JobID, err)
		}
	}
	return nil
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
