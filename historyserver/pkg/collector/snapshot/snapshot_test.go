package snapshot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memWriter struct {
	files map[string][]byte
	dirs  map[string]bool
}

func newMemWriter() *memWriter {
	return &memWriter{
		files: make(map[string][]byte),
		dirs:  make(map[string]bool),
	}
}

func (m *memWriter) WriteFile(file string, reader io.ReadSeeker) error {
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}
	m.files[file] = data
	return nil
}

func (m *memWriter) CreateDirectory(path string) error {
	m.dirs[path] = true
	return nil
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(v))
}

func versionResponse(sessionName string) map[string]any {
	return map[string]any{
		"version":      "2.54.0",
		"session_name": sessionName,
	}
}

// testLogHook captures logrus entries for test assertions.
type testLogHook struct {
	mu      sync.Mutex
	entries []logrus.Entry
}

func (h *testLogHook) Levels() []logrus.Level { return logrus.AllLevels }

func (h *testLogHook) Fire(entry *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = append(h.entries, *entry)
	return nil
}

func (h *testLogHook) find(message string) *logrus.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i := range h.entries {
		if h.entries[i].Message == message {
			return &h.entries[i]
		}
	}
	return nil
}

func (h *testLogHook) findAll(message string) []logrus.Entry {
	h.mu.Lock()
	defer h.mu.Unlock()
	var result []logrus.Entry
	for i := range h.entries {
		if h.entries[i].Message == message {
			result = append(result, h.entries[i])
		}
	}
	return result
}

func installHook(t *testing.T) *testLogHook {
	t.Helper()
	hook := &testLogHook{}
	logrus.AddHook(hook)
	t.Cleanup(func() {
		logrus.StandardLogger().ReplaceHooks(make(logrus.LevelHooks))
	})
	return hook
}

func TestRunSnapshot(t *testing.T) {
	hook := installHook(t)

	clusterMetadata := map[string]any{
		"result": true,
		"data": map[string]any{
			"rayVersion":    "2.54.0",
			"pythonVersion": "3.11.0",
		},
	}
	timezone := map[string]any{"timezone": "UTC"}
	serveApps := map[string]any{"applications": map[string]any{}}
	placementGroups := map[string]any{"result": []any{}}
	jobs := []map[string]any{
		{"job_id": "01000000", "status": "SUCCEEDED"},
		{"job_id": "02000000", "status": "RUNNING"},
	}
	datasets01 := map[string]any{"datasets": []any{map[string]any{"id": "ds1"}}}
	tasks := map[string]any{"result": []any{}}
	actors := []any{}
	nodesSummary := map[string]any{"result": true, "data": map[string]any{"summary": []any{}}}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		query := r.URL.RawQuery
		full := p
		if query != "" {
			full = p + "?" + query
		}

		var resp any
		switch {
		case full == "/api/version":
			resp = versionResponse("session_2026-08-17_10-30-00_123456")
		case full == "/nodes?view=summary":
			resp = nodesSummary
		case full == "/api/v0/cluster_metadata":
			resp = clusterMetadata
		case full == "/timezone":
			resp = timezone
		case full == "/api/serve/applications/":
			resp = serveApps
		case full == "/api/v0/placement_groups?detail=1&limit=10000":
			resp = placementGroups
		case full == "/api/jobs/":
			resp = jobs
		case full == "/api/v0/tasks?detail=1&limit=10000":
			resp = tasks
		case full == "/logical/actors":
			resp = actors
		case strings.HasPrefix(p, "/api/data/datasets/"):
			jobID := strings.TrimPrefix(p, "/api/data/datasets/")
			if jobID == "01000000" {
				resp = datasets01
			} else {
				resp = map[string]any{"datasets": []any{}}
			}
		default:
			http.NotFound(w, r)
			return
		}

		writeJSON(t, w, resp)
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
	}

	err := Run(context.Background(), cfg, writer)
	require.NoError(t, err)

	sessionName := "session_2026-08-17_10-30-00_123456"

	assertStoredEndpoint(t, writer, sessionName, "/api/v0/cluster_metadata")
	assertStoredEndpoint(t, writer, sessionName, "/timezone")
	assertStoredEndpoint(t, writer, sessionName, "/api/serve/applications/")
	assertStoredEndpoint(t, writer, sessionName, "/api/v0/placement_groups?detail=1&limit=10000")
	assertStoredEndpoint(t, writer, sessionName, "/api/jobs/")
	assertStoredEndpoint(t, writer, sessionName, "/api/data/datasets/01000000")

	markerPath := "root/cluster-metadata/raycluster/default_my-cluster/" + sessionName
	_, markerExists := writer.files[markerPath]
	assert.True(t, markerExists, "cluster metadata marker should exist at %s; files: %v", markerPath, keys(writer.files))

	// Verify structured summary log.
	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary, "expected 'Snapshot finished' log entry")
	assert.Equal(t, "complete", summary.Data["outcome"])
	assert.Equal(t, "my-cluster", summary.Data["cluster"])
	assert.Equal(t, "default", summary.Data["namespace"])
	assert.Equal(t, sessionName, summary.Data["session"])
	assert.Equal(t, true, summary.Data["marker_written"])
	assert.EqualValues(t, 0, summary.Data["failed"])
}

func TestRunSnapshotPartialFailure(t *testing.T) {
	hook := installHook(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		full := p
		if r.URL.RawQuery != "" {
			full = p + "?" + r.URL.RawQuery
		}
		switch {
		case full == "/api/version":
			writeJSON(t, w, versionResponse("session_2026-08-17_10-30-00_123456"))
		case full == "/api/v0/cluster_metadata":
			writeJSON(t, w, map[string]any{"result": true})
		case p == "/api/jobs/":
			writeJSON(t, w, []any{})
		case full == "/timezone":
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		default:
			writeJSON(t, w, map[string]any{})
		}
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
	}

	err := Run(context.Background(), cfg, writer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "endpoint errors")
	assert.Contains(t, err.Error(), "/timezone")

	sessionName := "session_2026-08-17_10-30-00_123456"
	markerPath := "root/cluster-metadata/raycluster/default_my-cluster/" + sessionName
	_, markerExists := writer.files[markerPath]
	assert.True(t, markerExists, "marker should be written on partial failure; files: %v", keys(writer.files))

	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary)
	assert.Equal(t, "partial", summary.Data["outcome"])
	assert.Equal(t, true, summary.Data["marker_written"])
	stored, _ := summary.Data["stored"].(int)
	assert.Greater(t, stored, 0, "at least one endpoint should be stored")
	failed, _ := summary.Data["failed"].(int)
	assert.Greater(t, failed, 0, "at least one endpoint should have failed")
}

func TestRunSnapshotMissingSessionInfo(t *testing.T) {
	hook := installHook(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{"version": "2.54.0"})
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
	}

	err := Run(context.Background(), cfg, writer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to discover session name")

	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary)
	assert.Equal(t, "failed", summary.Data["outcome"])
	assert.Equal(t, "my-cluster", summary.Data["cluster"])
	assert.Equal(t, false, summary.Data["marker_written"])
}

func TestRunSnapshotWithOwner(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/version":
			writeJSON(t, w, versionResponse("session_2026-08-17_10-30-00_123456"))
		case r.URL.Path == "/api/jobs/":
			writeJSON(t, w, []any{})
		default:
			writeJSON(t, w, map[string]any{})
		}
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
		OwnerKind:           "rayjob",
		OwnerName:           "my-job",
	}

	err := Run(context.Background(), cfg, writer)
	require.NoError(t, err)

	sessionName := "session_2026-08-17_10-30-00_123456"

	markerPath := "root/cluster-metadata/rayjob/default_my-job_my-cluster/" + sessionName
	_, markerExists := writer.files[markerPath]
	assert.True(t, markerExists, "cluster metadata marker with owner should exist at %s; files: %v", markerPath, keys(writer.files))

	expectedPrefix := "root/cluster-history/rayjob/default/my-job/my-cluster/" + sessionName + "/fetched_endpoints/"
	foundFetched := false
	for p := range writer.files {
		if strings.HasPrefix(p, expectedPrefix) {
			foundFetched = true
			break
		}
	}
	assert.True(t, foundFetched, "fetched endpoints should be under owner cluster dir %s; files: %v", expectedPrefix, keys(writer.files))
}

func TestRunSnapshotCancelation(t *testing.T) {
	hook := installHook(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, versionResponse("session_2026-08-17_10-30-00_123456"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
	}

	err := Run(ctx, cfg, writer)
	assert.Error(t, err, "should fail when context is canceled")

	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary)
	assert.Equal(t, "failed", summary.Data["outcome"])
	assert.Equal(t, false, summary.Data["marker_written"])
}

func TestDiscoverSessionName(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, versionResponse("session_2026-01-01_12-00-00_000000"))
	}))
	defer srv.Close()

	client := &http.Client{}
	name, err := discoverSessionName(context.Background(), client, srv.URL)
	require.NoError(t, err)
	assert.Equal(t, "session_2026-01-01_12-00-00_000000", name)
}

func TestDiscoverSessionNameDashboardUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	defer srv.Close()

	client := &http.Client{}
	_, err := discoverSessionName(context.Background(), client, srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dashboard unreachable")
}

func TestDiscoverSessionNameNoSessionInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"version": "2.54.0"})
	}))
	defer srv.Close()

	client := &http.Client{}
	_, err := discoverSessionName(context.Background(), client, srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "did not include session_name")
}

func TestRunSnapshotTotalScrapeFailure(t *testing.T) {
	hook := installHook(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/version" {
			writeJSON(t, w, versionResponse("session_2026-08-17_10-30-00_123456"))
			return
		}
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
	}

	err := Run(context.Background(), cfg, writer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no endpoints were stored successfully")

	for p := range writer.files {
		assert.NotContains(t, p, "cluster-metadata", "no marker should be written when all endpoints fail")
	}

	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary)
	assert.Equal(t, "failed", summary.Data["outcome"])
	assert.Equal(t, false, summary.Data["marker_written"])
	assert.EqualValues(t, 0, summary.Data["stored"])
	failed, _ := summary.Data["failed"].(int)
	assert.Greater(t, failed, 0)
}

func TestRunSnapshotLateCancelation(t *testing.T) {
	hook := installHook(t)
	ctx, cancel := context.WithCancel(context.Background())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/version":
			writeJSON(t, w, versionResponse("session_2026-08-17_10-30-00_123456"))
		case r.URL.Path == "/api/jobs/":
			cancel()
			writeJSON(t, w, []any{})
		default:
			writeJSON(t, w, map[string]any{})
		}
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
	}

	err := Run(ctx, cfg, writer)
	require.Error(t, err)

	for p := range writer.files {
		assert.NotContains(t, p, "cluster-metadata", "no marker should be written after late cancellation")
	}

	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary)
	assert.Equal(t, "failed", summary.Data["outcome"])
	assert.Equal(t, false, summary.Data["marker_written"])
}

func TestRunSnapshotDatasetErrors(t *testing.T) {
	hook := installHook(t)

	jobs := []map[string]any{
		{"job_id": "01000000", "status": "SUCCEEDED"},
		{"job_id": "02000000", "status": "RUNNING"},
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/version":
			writeJSON(t, w, versionResponse("session_2026-08-17_10-30-00_123456"))
		case r.URL.Path == "/api/jobs/":
			writeJSON(t, w, jobs)
		case strings.HasPrefix(r.URL.Path, "/api/data/datasets/"):
			http.Error(w, "not found", http.StatusNotFound)
		default:
			writeJSON(t, w, map[string]any{})
		}
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
	}

	err := Run(context.Background(), cfg, writer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "/api/data/datasets/01000000")
	assert.Contains(t, err.Error(), "/api/data/datasets/02000000")
	assert.Contains(t, err.Error(), "endpoint errors")

	// Marker should still be written — other endpoints succeeded.
	sessionName := "session_2026-08-17_10-30-00_123456"
	markerPath := "root/cluster-metadata/raycluster/default_my-cluster/" + sessionName
	_, markerExists := writer.files[markerPath]
	assert.True(t, markerExists, "marker should be written despite dataset failures; files: %v", keys(writer.files))

	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary)
	assert.Equal(t, "partial", summary.Data["outcome"])
	assert.Equal(t, true, summary.Data["marker_written"])
}

func TestRunSnapshotDatasetDiscoveryFailure(t *testing.T) {
	hook := installHook(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case p == "/api/version":
			writeJSON(t, w, versionResponse("session_2026-08-17_10-30-00_123456"))
		case p == "/api/jobs/":
			writeJSON(t, w, map[string]any{})
			writeJSON(t, w, []byte("invalid"))
		default:
			writeJSON(t, w, map[string]any{})
		}
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "my-cluster",
		RayClusterNamespace: "default",
	}

	err := Run(context.Background(), cfg, writer)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dataset discovery")

	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary)
	assert.Equal(t, "partial", summary.Data["outcome"])

	// Verify the dataset discovery failure is counted.
	failed, _ := summary.Data["failed"].(int)
	assert.Greater(t, failed, 0)
}

func TestRunSnapshotLogClusterIdentity(t *testing.T) {
	hook := installHook(t)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/api/version":
			writeJSON(t, w, versionResponse("session_test"))
		case r.URL.Path == "/api/jobs/":
			writeJSON(t, w, []any{})
		default:
			writeJSON(t, w, map[string]any{})
		}
	}))
	defer srv.Close()

	writer := newMemWriter()
	cfg := Config{
		DashboardAddress:    srv.URL,
		StorageRootDir:      "root",
		RayClusterName:      "prod-cluster",
		RayClusterNamespace: "ml-team",
	}

	err := Run(context.Background(), cfg, writer)
	require.NoError(t, err)

	// The "Snapshot starting" log should have cluster identity.
	starting := hook.find("Snapshot starting")
	require.NotNil(t, starting, "expected 'Snapshot starting' log entry")
	assert.Equal(t, "prod-cluster", starting.Data["cluster"])
	assert.Equal(t, "ml-team", starting.Data["namespace"])

	// The "Session discovered" log should include the session.
	discovered := hook.find("Session discovered")
	require.NotNil(t, discovered, "expected 'Session discovered' log entry")
	assert.Equal(t, "session_test", discovered.Data["session"])

	// The summary should have all three.
	summary := hook.find("Snapshot finished")
	require.NotNil(t, summary)
	assert.Equal(t, "prod-cluster", summary.Data["cluster"])
	assert.Equal(t, "ml-team", summary.Data["namespace"])
	assert.Equal(t, "session_test", summary.Data["session"])
	assert.Equal(t, "complete", summary.Data["outcome"])
	assert.Contains(t, summary.Data, "duration")
}

func TestDedup(t *testing.T) {
	result := dedup(
		[]string{"/a", "/b", "/c"},
		[]string{"/b", "/d"},
		[]string{"/a", "/e"},
	)
	assert.Equal(t, []string{"/a", "/b", "/c", "/d", "/e"}, result)
}

func TestFetchAndStore(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"key":"value"}`))
	}))
	defer srv.Close()

	writer := newMemWriter()
	stored, err := fetchAndStore(context.Background(), &http.Client{}, srv.URL, "/api/v0/cluster_metadata", "root/cluster-history/raycluster/default/my-cluster", "session_2026-01-01_12-00-00_000000", writer)
	require.NoError(t, err)
	assert.True(t, stored, "expected stored=true for non-empty response")

	expectedKey := "root/cluster-history/raycluster/default/my-cluster/session_2026-01-01_12-00-00_000000/fetched_endpoints/restful__api__v0__cluster_metadata"
	data, ok := writer.files[expectedKey]
	require.True(t, ok, "expected file at %s; files: %v", expectedKey, keys(writer.files))
	assert.JSONEq(t, `{"key":"value"}`, string(data))
}

func TestFetchAndStoreSkipsEmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
	}))
	defer srv.Close()

	writer := newMemWriter()
	stored, err := fetchAndStore(context.Background(), &http.Client{}, srv.URL, "/api/v0/cluster_metadata", "root/cluster-history/raycluster/default/my-cluster", "session_2026-01-01_12-00-00_000000", writer)
	require.NoError(t, err)
	assert.False(t, stored, "expected stored=false for empty response")
	assert.Empty(t, writer.files, "should not store empty responses")
}

func TestFetchEndpointAuthFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := fetchEndpoint(context.Background(), &http.Client{}, srv.URL, "/api/v0/cluster_metadata")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "authentication failed")
}

func assertStoredEndpoint(t *testing.T, writer *memWriter, sessionName, endpoint string) { //nolint:unparam // sessionName is conceptually variable; helper exists for readability
	t.Helper()
	storageKey := "restful__" + strings.ReplaceAll(strings.Trim(endpoint, "/"), "/", "__")
	filePath := "root/cluster-history/raycluster/default/my-cluster/" + sessionName + "/fetched_endpoints/" + storageKey
	_, ok := writer.files[filePath]
	assert.True(t, ok, "endpoint %s should be stored at %s; files: %v", endpoint, filePath, keys(writer.files))
}

func keys(m map[string][]byte) []string {
	result := make([]string, 0, len(m))
	for k := range m {
		result = append(result, k)
	}
	return result
}
