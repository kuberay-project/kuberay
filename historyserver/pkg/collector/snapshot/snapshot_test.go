package snapshot

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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

func nodesViewSummaryResponse(sessionName string) map[string]any {
	return map[string]any{
		"result": true,
		"msg":    "Node summary fetched.",
		"data": map[string]any{
			"summary": []any{
				map[string]any{
					"raylet": map[string]any{
						"rayletSocketName": "/tmp/ray/" + sessionName + "/sockets/raylet",
					},
				},
			},
		},
	}
}

func TestRunSnapshot(t *testing.T) {
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
	nodes := []any{}
	nodesSummary := nodesViewSummaryResponse("session_2026-08-17_10-30-00_123456")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		query := r.URL.RawQuery
		full := path
		if query != "" {
			full = path + "?" + query
		}

		var resp any
		switch {
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
		case full == "/nodes":
			resp = nodes
		case strings.HasPrefix(path, "/api/data/datasets/"):
			jobID := strings.TrimPrefix(path, "/api/data/datasets/")
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
}

func TestRunSnapshotMissingSessionInfo(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(t, w, map[string]any{
			"result": true,
			"data":   map[string]any{"summary": []any{}},
		})
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
}

func TestRunSnapshotWithOwner(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		full := r.URL.Path
		if r.URL.RawQuery != "" {
			full += "?" + r.URL.RawQuery
		}
		switch {
		case full == "/nodes?view=summary":
			writeJSON(t, w, nodesViewSummaryResponse("session_2026-08-17_10-30-00_123456"))
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
	for path := range writer.files {
		if strings.HasPrefix(path, expectedPrefix) {
			foundFetched = true
			break
		}
	}
	assert.True(t, foundFetched, "fetched endpoints should be under owner cluster dir %s; files: %v", expectedPrefix, keys(writer.files))
}

func TestRunSnapshotCancelation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, nodesViewSummaryResponse("session_2026-08-17_10-30-00_123456"))
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
}

func TestDiscoverSessionName(t *testing.T) {
	tests := []struct {
		name     string
		response map[string]any
		wantName string
	}{
		{
			name:     "from rayletSocketName",
			response: nodesViewSummaryResponse("session_2026-01-01_12-00-00_000000"),
			wantName: "session_2026-01-01_12-00-00_000000",
		},
		{
			name: "from custom ray tmp root",
			response: map[string]any{
				"result": true,
				"data": map[string]any{
					"summary": []any{
						map[string]any{
							"raylet": map[string]any{
								"rayletSocketName": "/custom/ray/session_2026-02-01_13-00-00_111111/sockets/raylet",
							},
						},
					},
				},
			},
			wantName: "session_2026-02-01_13-00-00_111111",
		},
		{
			name: "picks first node with valid session",
			response: map[string]any{
				"result": true,
				"data": map[string]any{
					"summary": []any{
						map[string]any{
							"raylet": map[string]any{
								"rayletSocketName": "/tmp/ray/session_2026-03-01_14-00-00_222222/sockets/raylet",
							},
						},
						map[string]any{
							"raylet": map[string]any{
								"rayletSocketName": "/tmp/ray/session_2026-03-01_14-00-00_222222/sockets/raylet",
							},
						},
					},
				},
			},
			wantName: "session_2026-03-01_14-00-00_222222",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, tt.response)
			}))
			defer srv.Close()

			client := &http.Client{}
			name, err := discoverSessionName(context.Background(), client, srv.URL)
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, name)
		})
	}
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
		writeJSON(t, w, map[string]any{
			"result": true,
			"data":   map[string]any{"summary": []any{}},
		})
	}))
	defer srv.Close()

	client := &http.Client{}
	_, err := discoverSessionName(context.Background(), client, srv.URL)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no node in")
}

func TestRunSnapshotTotalScrapeFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		full := r.URL.Path
		if r.URL.RawQuery != "" {
			full += "?" + r.URL.RawQuery
		}
		if full == "/nodes?view=summary" {
			writeJSON(t, w, nodesViewSummaryResponse("session_2026-08-17_10-30-00_123456"))
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

	for path := range writer.files {
		assert.NotContains(t, path, "cluster-metadata", "no marker should be written when all endpoints fail")
	}
}

func TestRunSnapshotLateCancelation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		full := r.URL.Path
		if r.URL.RawQuery != "" {
			full += "?" + r.URL.RawQuery
		}
		switch {
		case full == "/nodes?view=summary":
			writeJSON(t, w, nodesViewSummaryResponse("session_2026-08-17_10-30-00_123456"))
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

	for path := range writer.files {
		assert.NotContains(t, path, "cluster-metadata", "no marker should be written after late cancellation")
	}
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
