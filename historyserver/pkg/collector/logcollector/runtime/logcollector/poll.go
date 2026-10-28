package logcollector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// Each string must match the frontend's request URI, query string included: the
// storage key is derived from it, so a mismatch makes the stored object unreachable.
const (
	serveApplicationsEndpoint = "/api/serve/applications/"
	placementGroupsEndpoint   = "/api/v0/placement_groups?detail=1&limit=10000"
	// Only used to discover job IDs; its response is not stored.
	jobsEndpoint = "/api/jobs/"
	// Per job, where job_id is the hex core job ID (e.g. "01000000"), not the submission ID.
	dataDatasetsEndpointPrefix = "/api/data/datasets/"
)

// defaultPolledEndpoints lists the fixed dashboard endpoints polled by every
// head collector. Ray Data is also built in, but its per-job endpoints are
// discovered through jobsEndpoint and polled separately by pollDataDatasets.
var defaultPolledEndpoints = []string{
	serveApplicationsEndpoint,
	placementGroupsEndpoint,
}

// polledEndpoints merges the built-in and configured endpoints, deduplicated.
func (r *RayLogHandler) polledEndpoints() []string {
	all := slices.Concat(defaultPolledEndpoints, r.AdditionalEndpoints)
	endpoints := make([]string, 0, len(all))
	seen := make(map[string]struct{}, len(all))
	for _, endpoint := range all {
		if _, ok := seen[endpoint]; ok {
			continue
		}
		seen[endpoint] = struct{}{}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints
}

// Statuses a job never leaves, so its datasets only need to be stored once.
// Ref: https://github.com/ray-project/ray/blob/ray-2.54.1/python/ray/dashboard/modules/job/common.py#L38-L50
var terminalJobStatuses = map[string]bool{
	"SUCCEEDED": true,
	"FAILED":    true,
	"STOPPED":   true,
}

// pollOutcome distinguishes "nothing worth storing" from "could not store".
type pollOutcome int

const (
	pollFailed pollOutcome = iota
	pollStored
	pollSkippedEmpty
)

// Stats can appear slightly after a job reports terminal, so one empty response is not final.
const terminalEmptyPollsBeforeGivingUp = 2

// The final poll shares the pod's termination grace period, so it gives up rather than overrun it.
const shutdownPollBudget = 10 * time.Second

// Caps the shutdown join on the periodic poller: a storage write in flight is not
// cancelable, and waiting it out could eat the grace period the final poll needs.
const periodicPollJoinTimeout = 5 * time.Second

// datasetPollState is owned by one goroutine at a time; no lock is needed.
type datasetPollState struct {
	// terminalStored contains terminal jobs whose dataset response was successfully written.
	terminalStored map[string]struct{}
	// emptyRuns counts consecutive empty responses observed after a job becomes terminal.
	emptyRuns map[string]int
}

func newDatasetPollState() *datasetPollState {
	return &datasetPollState{
		terminalStored: make(map[string]struct{}),
		emptyRuns:      make(map[string]int),
	}
}

// periodicPollResult transfers ownership of state after the periodic poller exits.
type periodicPollResult struct {
	sessionName string
	state       *datasetPollState
}

func (r *RayLogHandler) startPeriodicEndpointPolling(stop <-chan struct{}) <-chan periodicPollResult {
	// The core sends exactly once. A one-element buffer lets that send finish even if
	// the shutdown join times out and no receiver remains.
	results := make(chan periodicPollResult, 1)
	go func() {
		results <- r.pollAdditionalEndpointsPeriodically(stop)
	}()
	return results
}

func waitForPeriodicPollResult(results <-chan periodicPollResult, timeout time.Duration) (periodicPollResult, bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case result := <-results:
		return result, true
	case <-timer.C:
		return periodicPollResult{}, false
	}
}

func (r *RayLogHandler) pollAdditionalEndpointsPeriodically(stop <-chan struct{}) periodicPollResult {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	sessionName, err := waitForSessionName(ctx)
	if err != nil {
		logrus.Errorf("Failed to resolve session name for endpoint polling: %v", err)
		return periodicPollResult{}
	}
	logrus.Infof("Starting endpoint polling (interval=%v, endpoints=%v)", r.EndpointPollInterval, r.polledEndpoints())

	state := newDatasetPollState()
	r.pollAllEndpoints(ctx, sessionName, state, false)

	ticker := time.NewTicker(r.EndpointPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			logrus.Info("Shutdown signaled, stopping endpoint polling")
			return periodicPollResult{sessionName: sessionName, state: state}
		case <-ticker.C:
			sessionName, state = r.pollCycle(ctx, sessionName, state)
		}
	}
}

// waitForSessionName resolves session_latest, retrying until ctx is canceled:
// at startup the symlink appears only once Ray has bootstrapped.
func waitForSessionName(ctx context.Context) (string, error) {
	for {
		name, err := currentSessionName()
		if err == nil {
			return name, nil
		}
		logrus.Warnf("session_latest symlink not ready: %v, retrying in %v", err, defaultRetryInterval)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(defaultRetryInterval):
		}
	}
}

// pollCycle runs one polling pass, re-resolving the session first: the Ray head container
// can restart alone, starting a new session (and new job IDs) while this sidecar lives on.
func (r *RayLogHandler) pollCycle(ctx context.Context, sessionName string, state *datasetPollState) (string, *datasetPollState) {
	switch current, err := currentSessionName(); {
	case err != nil:
		logrus.Warnf("Failed to re-resolve session name, polling into %s: %v", sessionName, err)
	case current != sessionName:
		logrus.Infof("Session changed from %s to %s, resetting dataset polling state", sessionName, current)
		sessionName, state = current, newDatasetPollState()
	}

	r.pollAllEndpoints(ctx, sessionName, state, false)
	return sessionName, state
}

// currentSessionName resolves session_latest without retrying, for callers that must not block.
func currentSessionName() (string, error) {
	sessionRealDir, err := filepath.EvalSymlinks(utils.GetRaySessionLatestPath())
	if err != nil {
		return "", err
	}
	return filepath.Base(sessionRealDir), nil
}

// processAdditionalEndpoints performs one final poll before shutdown.
func (r *RayLogHandler) processAdditionalEndpoints(previous periodicPollResult) {
	logrus.Info("Processing polled endpoints before shutdown")

	sessionName, err := currentSessionName()
	if err != nil {
		logrus.Errorf("Failed to resolve session name for final endpoint poll: %v", err)
		return
	}

	state := newDatasetPollState()
	if previous.state != nil && previous.sessionName == sessionName {
		state = previous.state
	}

	// The budget cancels HTTP fetches. StorageWriter.WriteFile does not accept a
	// context, so a write already in progress may finish after the deadline.
	ctx, cancel := context.WithTimeout(context.Background(), shutdownPollBudget)
	defer cancel()

	r.pollAllEndpoints(ctx, sessionName, state, true)
	logrus.Info("Finished processing polled endpoints")
}

// pollAllEndpoints fetches the polled endpoints plus per-job dataset endpoints and stores
// their responses, stopping early once ctx is canceled.
// finalPoll marks the shutdown pass, which distrusts empty Serve responses (see isEmptyPayload).
func (r *RayLogHandler) pollAllEndpoints(ctx context.Context, sessionName string, state *datasetPollState, finalPoll bool) {
	for _, endpoint := range r.polledEndpoints() {
		if ctx.Err() != nil {
			logrus.Warnf("Stopped polling before %s: %v", endpoint, ctx.Err())
			return
		}
		r.pollSingleEndpoint(ctx, endpoint, sessionName, finalPoll)
	}
	r.pollDataDatasets(ctx, sessionName, state, finalPoll)
}

// pollDataDatasets stores one datasets object per job discovered via jobsEndpoint.
// Settled terminal jobs are skipped so the per-cycle cost does not grow with job count.
func (r *RayLogHandler) pollDataDatasets(ctx context.Context, sessionName string, state *datasetPollState, finalPoll bool) {
	body, err := r.fetchEndpoint(ctx, jobsEndpoint)
	if err != nil {
		logrus.Warnf("Failed to fetch %s for dataset polling: %v", jobsEndpoint, err)
		return
	}

	var jobs []struct {
		JobID  string `json:"job_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(body, &jobs); err != nil {
		logrus.Warnf("Failed to parse %s response: %v", jobsEndpoint, err)
		return
	}

	for i, job := range jobs {
		if ctx.Err() != nil {
			logrus.Warnf("Stopped dataset polling after %d/%d jobs: %v", i, len(jobs), ctx.Err())
			return
		}
		// job_id is empty for submission jobs whose driver has not started yet.
		if job.JobID == "" {
			continue
		}
		if _, ok := state.terminalStored[job.JobID]; ok {
			continue
		}
		if !finalPoll && terminalJobStatuses[job.Status] &&
			state.emptyRuns[job.JobID] >= terminalEmptyPollsBeforeGivingUp {
			continue
		}

		outcome := r.pollSingleEndpoint(ctx, dataDatasetsEndpointPrefix+job.JobID, sessionName, finalPoll)
		if !terminalJobStatuses[job.Status] {
			continue
		}
		switch outcome {
		case pollStored:
			state.terminalStored[job.JobID] = struct{}{}
			delete(state.emptyRuns, job.JobID)
		case pollSkippedEmpty:
			state.emptyRuns[job.JobID]++
		case pollFailed:
			// Retried next cycle.
		}
	}
}

func (r *RayLogHandler) pollSingleEndpoint(ctx context.Context, endpoint, sessionName string, finalPoll bool) pollOutcome {
	body, err := r.fetchEndpoint(ctx, endpoint)
	if err != nil {
		logrus.Warnf("Failed to poll endpoint %s: %v", endpoint, err)
		return pollFailed
	}

	// Do not start a storage write after the parent context is canceled.
	if ctx.Err() != nil {
		return pollFailed
	}

	if isEmptyPayload(endpoint, body, finalPoll) {
		logrus.Debugf("Skipping %s: nothing to store", endpoint)
		return pollSkippedEmpty
	}

	storageKey := utils.EndpointPathToStorageKey(endpoint)
	objectKey := path.Join(clusterlogs.FetchedEndpointsDir(r.ClusterDir, sessionName), storageKey)
	if err := r.Writer.WriteFile(objectKey, bytes.NewReader(body)); err != nil {
		logrus.Errorf("Failed to store endpoint %s at %s: %v", endpoint, objectKey, err)
		return pollFailed
	}

	logrus.Infof("Successfully stored endpoint %s at %s (%d bytes)", endpoint, objectKey, len(body))
	return pollStored
}

// isEmptyPayload reports whether a response carries nothing worth storing.
// Datasets: empty can mean stats-actor eviction, so it never overwrites a snapshot.
// Serve: empty from a healthy cluster is the live truth and is stored, but on the final
// shutdown poll it usually means the Serve controller died before the dashboard.
func isEmptyPayload(endpoint string, body []byte, finalPoll bool) bool {
	switch {
	case strings.HasPrefix(endpoint, dataDatasetsEndpointPrefix):
		return !hasDatasets(body)
	case finalPoll && endpoint == serveApplicationsEndpoint:
		return !hasServeApplications(body)
	default:
		return false
	}
}

// Unparsable bodies count as non-empty so unexpected shapes are stored, not dropped.
func hasServeApplications(body []byte) bool {
	var resp struct {
		Applications map[string]json.RawMessage `json:"applications"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return true
	}
	return len(resp.Applications) > 0
}

// Unparsable bodies count as non-empty so unexpected shapes are stored, not dropped.
func hasDatasets(body []byte) bool {
	var resp struct {
		Datasets []json.RawMessage `json:"datasets"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return true
	}
	return len(resp.Datasets) > 0
}

// fetchEndpoint GETs one dashboard endpoint with the parent and per-request deadlines.
func (r *RayLogHandler) fetchEndpoint(parent context.Context, endpoint string) ([]byte, error) {
	url := r.DashboardAddress + endpoint

	ctx, cancel := context.WithTimeout(parent, defaultRequestTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	if err := utils.SetRayAuthHeader(req); err != nil {
		return nil, fmt.Errorf("failed to authenticate request: %w", err)
	}

	resp, err := r.HttpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}
	// An auth failure drops this endpoint's data on every cycle for the lifetime of the Pod, so
	// it must not be reported at the same level as a transient non-200.
	if utils.IsAuthFailure(resp.StatusCode) {
		return nil, fmt.Errorf("authentication failed with status %d; check that token auth is configured for the collector container", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return body, nil
}
