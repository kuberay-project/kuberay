package historyserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emicklei/go-restful/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type mockStorageReader struct {
	listCount int
	clusters  []utils.ClusterInfo
}

func (m *mockStorageReader) List() []utils.ClusterInfo {
	m.listCount++
	return m.clusters
}

func (m *mockStorageReader) GetContent(_, _ string) io.Reader {
	return strings.NewReader("")
}

func (m *mockStorageReader) ListFiles(_, _ string) []string {
	return nil
}

func TestEnterCluster(t *testing.T) {
	// Reset default container to avoid polluting or using duplicate services across runs
	restful.DefaultContainer = restful.NewContainer()

	mockReader := &mockStorageReader{
		clusters: []utils.ClusterInfo{
			// cluster-a (Single session)
			{
				Namespace:   "default",
				Name:        "cluster-a",
				SessionName: "session_2026-04-22_10-00-00_000000_1",
				OwnerKind:   "rayjob",
				OwnerName:   "job-a",
			},
			// cluster-b (Multi-session cluster: past session AND live session)
			{
				Namespace:       "default",
				Name:            "cluster-b",
				SessionName:     "session_2026-04-22_10-00-00_000000_1",
				OwnerKind:       "rayservice",
				OwnerName:       "svc-b",
				CreateTimeStamp: 1000, // Older
			},
			{
				Namespace:       "default",
				Name:            "cluster-b",
				SessionName:     "live",
				OwnerKind:       "rayservice",
				OwnerName:       "svc-b",
				CreateTimeStamp: 2000, // Newer (latest)
			},
			// cluster-c (session resolved but triggers live resolution)
			{
				Namespace:   "default",
				Name:        "cluster-c",
				SessionName: "session_2026-04-22_10-00-00_000000_2_live",
				OwnerKind:   "rayjob",
				OwnerName:   "job-c",
			},
			// cluster-d (invalid session format)
			{
				Namespace:   "default",
				Name:        "cluster-d",
				SessionName: "invalid-session-name",
			},
		},
	}

	// Initialize fake client manager for tests
	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	clientManager := &ClientManager{
		clients: []client.Client{k8sClient},
	}

	// Create ServerHandler with fake sessionLoader and mockStorageReader
	handler := &ServerHandler{
		enableLiveClusters: true,
		maxClusters:        100,
		reader:             mockReader,
		clientManager:      clientManager,
	}

	// Setup fake processor for sessionLoader
	fp := &fakeProcessor{
		fn: func(_ context.Context, info utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
			if info.SessionName == "session_2026-04-22_10-00-00_000000_1" {
				return SessionStatusProcessed, &eventserver.SessionSnapshot{}, nil
			}
			if info.SessionName == "session_2026-04-22_10-00-00_000000_2_live" {
				return SessionStatusLive, nil, nil
			}
			return SessionStatusEventsErr, nil, fmt.Errorf("unknown session")
		},
	}
	handler.sessionLoader = NewSessionLoader(context.Background(), fp, DefaultSessionProcessTimeout, DefaultSessionCacheSize, defaultSessionCacheMaxBytes, DefaultSessionCacheTTL)

	// Register actual router
	routerRayClusterSet(handler)

	container := restful.DefaultContainer

	t.Run("Verify sorted order of multi-session slice puts latest first", func(t *testing.T) {
		clusters := handler.listClusters(100)
		var sessions []utils.ClusterInfo
		for _, c := range clusters {
			if c.Name == "cluster-b" {
				sessions = append(sessions, c)
			}
		}
		if len(sessions) != 2 {
			t.Fatalf("Expected 2 sessions, got %d", len(sessions))
		}
		// Index 0 must be the newer session (live, timestamp 2000)
		if sessions[0].SessionName != "live" {
			t.Errorf("Expected latest session 'live' at index 0, got %s", sessions[0].SessionName)
		}
	})

	t.Run("Enter existing single-session cluster with explicit session (Successful Dead Session Loading)", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-a/session_2026-04-22_10-00-00_000000_1", nil)
		resp := httptest.NewRecorder()
		container.ServeHTTP(resp, req)

		if resp.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", resp.Code, resp.Body.String())
		}

		cookies := resp.Result().Cookies()
		cookieMap := make(map[string]*http.Cookie)
		for _, cookie := range cookies {
			cookieMap[cookie.Name] = cookie
		}

		if c, ok := cookieMap[COOKIE_SESSION_NAME_KEY]; !ok || c.Value != "session_2026-04-22_10-00-00_000000_1" {
			t.Errorf("Expected cookie %s to be 'session_2026-04-22_10-00-00_000000_1', got %v", COOKIE_SESSION_NAME_KEY, c)
		}
	})

	t.Run("Enter cluster with session that is actually live maps to live sentinel", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-c/session_2026-04-22_10-00-00_000000_2_live", nil)
		resp := httptest.NewRecorder()
		container.ServeHTTP(resp, req)

		if resp.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", resp.Code, resp.Body.String())
		}

		cookies := resp.Result().Cookies()
		cookieMap := make(map[string]*http.Cookie)
		for _, cookie := range cookies {
			cookieMap[cookie.Name] = cookie
		}

		// Since the fake processor returns SessionStatusLive for this session, resolvedSession is set to "live"
		if c, ok := cookieMap[COOKIE_SESSION_NAME_KEY]; !ok || c.Value != "live" {
			t.Errorf("Expected cookie %s to be 'live' (resolved from timestamp because cluster is live), got %v", COOKIE_SESSION_NAME_KEY, c)
		}
	})

	t.Run("Enter cluster with invalid session name format rejects request", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-d/invalid-session-name", nil)
		resp := httptest.NewRecorder()
		container.ServeHTTP(resp, req)

		if resp.Code != http.StatusBadRequest {
			t.Fatalf("Expected status 400 (BadRequest), got %d", resp.Code)
		}
	})
	t.Run("Enter cluster with 'latest' keyword resolves to newest session in the list", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-b/latest", nil)
		resp := httptest.NewRecorder()
		container.ServeHTTP(resp, req)

		if resp.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.Code)
		}

		// Verify cookies are set to the ACTUAL newest session name ("live") rather than literal "latest"
		cookies := resp.Result().Cookies()
		cookieMap := make(map[string]*http.Cookie)
		for _, cookie := range cookies {
			cookieMap[cookie.Name] = cookie
		}

		if c, ok := cookieMap[COOKIE_CLUSTER_NAME_KEY]; !ok || c.Value != "cluster-b" {
			t.Errorf("Expected cookie %s to be 'cluster-b', got %v", COOKIE_CLUSTER_NAME_KEY, c)
		}
		if c, ok := cookieMap[COOKIE_CLUSTER_NAMESPACE_KEY]; !ok || c.Value != "default" {
			t.Errorf("Expected cookie %s to be 'default', got %v", COOKIE_CLUSTER_NAMESPACE_KEY, c)
		}
		if c, ok := cookieMap[COOKIE_SESSION_NAME_KEY]; !ok || c.Value != "live" {
			t.Errorf("Expected cookie %s to be 'live' (actual latest session name), got %v", COOKIE_SESSION_NAME_KEY, c)
		}
	})

	t.Run("Enter cluster with no session parameter defaults to latest", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-b", nil)
		resp := httptest.NewRecorder()
		container.ServeHTTP(resp, req)

		if resp.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d", resp.Code)
		}

		// Verify cookies are set to the ACTUAL newest session name ("live") rather than literal "latest"
		cookies := resp.Result().Cookies()
		cookieMap := make(map[string]*http.Cookie)
		for _, cookie := range cookies {
			cookieMap[cookie.Name] = cookie
		}

		if c, ok := cookieMap[COOKIE_SESSION_NAME_KEY]; !ok || c.Value != "live" {
			t.Errorf("Expected cookie %s to default to 'live' (actual latest session name), got %v", COOKIE_SESSION_NAME_KEY, c)
		}
	})
}

func TestEnterClusterLatestFromStorage(t *testing.T) {
	restful.DefaultContainer = restful.NewContainer()

	// Initialize fake client manager
	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	clientManager := &ClientManager{
		clients: []client.Client{k8sClient},
	}

	// Initialize mock reader with dummy data
	mockReader := &mockStorageReader{
		clusters: []utils.ClusterInfo{
			{
				Namespace:       "default",
				Name:            "cluster-refresh",
				SessionName:     "session_2026-04-22_10-00-00_000000_1",
				CreateTimeStamp: 1000,
			},
			{
				Namespace:       "default",
				Name:            "cluster-refresh",
				SessionName:     "session_2026-04-22_10-00-00_000000_2",
				CreateTimeStamp: 2000, // Newer latest!
			},
		},
	}

	handler := &ServerHandler{
		enableLiveClusters: true,
		maxClusters:        100,
		reader:             mockReader,
		clientManager:      clientManager,
	}

	fp := &fakeProcessor{
		fn: func(_ context.Context, _ utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
			return SessionStatusProcessed, &eventserver.SessionSnapshot{}, nil
		},
	}
	handler.sessionLoader = NewSessionLoader(context.Background(), fp, DefaultSessionProcessTimeout, DefaultSessionCacheSize, defaultSessionCacheMaxBytes, DefaultSessionCacheTTL)

	routerRayClusterSet(handler)
	container := restful.DefaultContainer

	// Call enter_cluster with "latest"
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-refresh/latest", nil)
	resp := httptest.NewRecorder()
	container.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", resp.Code, resp.Body.String())
	}

	// The mock reader's List() must be called to retrieve sessions from storage!
	if mockReader.listCount == 0 {
		t.Errorf("Expected StorageReader.List() to be called to retrieve sessions, but call count was 0")
	}

	// Verify the cookie is set to the newest session
	cookies := resp.Result().Cookies()
	cookieMap := make(map[string]*http.Cookie)
	for _, cookie := range cookies {
		cookieMap[cookie.Name] = cookie
	}

	if c, ok := cookieMap[COOKIE_SESSION_NAME_KEY]; !ok || c.Value != "session_2026-04-22_10-00-00_000000_2" {
		t.Errorf("Expected cookie %s to resolve to the new latest session 'session_2026-04-22_10-00-00_000000_2', got %v", COOKIE_SESSION_NAME_KEY, c)
	}
}

func TestEnterClusterLatestPrioritizesLive(t *testing.T) {
	restful.DefaultContainer = restful.NewContainer()

	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "cluster-prioritize-live"},
	}).Build()
	clientManager := &ClientManager{
		clients: []client.Client{k8sClient},
	}

	mockReader := &mockStorageReader{
		clusters: []utils.ClusterInfo{
			{
				Namespace:       "default",
				Name:            "cluster-prioritize-live",
				SessionName:     "session_2026-04-22_10-00-00_000000_1",
				CreateTimeStamp: 2000, // Newer timestamp on storage!
			},
		},
	}

	handler := &ServerHandler{
		enableLiveClusters: true,
		maxClusters:        100,
		clientManager:      clientManager,
		reader:             mockReader,
	}

	fp := &fakeProcessor{
		fn: func(_ context.Context, _ utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
			return SessionStatusLive, nil, nil
		},
	}
	handler.sessionLoader = NewSessionLoader(context.Background(), fp, DefaultSessionProcessTimeout, DefaultSessionCacheSize, defaultSessionCacheMaxBytes, DefaultSessionCacheTTL)

	routerRayClusterSet(handler)
	container := restful.DefaultContainer

	// Call enter_cluster with "latest"
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-prioritize-live/latest", nil)
	resp := httptest.NewRecorder()
	container.ServeHTTP(resp, req)

	if resp.Code != http.StatusOK {
		t.Fatalf("Expected status 200, got %d: %s", resp.Code, resp.Body.String())
	}

	// Verify the cookie resolves to "live" (due to live prioritization) instead of the newer archived session
	cookies := resp.Result().Cookies()
	cookieMap := make(map[string]*http.Cookie)
	for _, cookie := range cookies {
		cookieMap[cookie.Name] = cookie
	}

	if c, ok := cookieMap[COOKIE_SESSION_NAME_KEY]; !ok || c.Value != "live" {
		t.Errorf("Expected cookie %s to prioritize 'live', got %v", COOKIE_SESSION_NAME_KEY, c)
	}
}

func TestEnterClusterReturnsNotFoundWhenRemovedFromStorage(t *testing.T) {
	restful.DefaultContainer = restful.NewContainer()

	// 1. Storage reader returns an empty list (sessions were deleted/cleaned up)
	mockReader := &mockStorageReader{
		clusters: []utils.ClusterInfo{},
	}

	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	clientManager := &ClientManager{
		clients: []client.Client{k8sClient},
	}

	handler := &ServerHandler{
		enableLiveClusters: true,
		maxClusters:        100,
		reader:             mockReader,
		clientManager:      clientManager,
	}

	fp := &fakeProcessor{
		fn: func(_ context.Context, _ utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
			return SessionStatusProcessed, &eventserver.SessionSnapshot{}, nil
		},
	}
	handler.sessionLoader = NewSessionLoader(context.Background(), fp, DefaultSessionProcessTimeout, DefaultSessionCacheSize, defaultSessionCacheMaxBytes, DefaultSessionCacheTTL)

	routerRayClusterSet(handler)
	container := restful.DefaultContainer

	// Call enter_cluster with "latest"
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-removed-from-storage/latest", nil)
	resp := httptest.NewRecorder()
	container.ServeHTTP(resp, req)

	// Since storage is configured but returned empty matching list, fallback should NOT occur, returning 404
	if resp.Code != http.StatusNotFound {
		t.Fatalf("Expected status 404 (NotFound), got %d: %s", resp.Code, resp.Body.String())
	}
}

type errorClient struct {
	client.Client
	err error
}

func (c *errorClient) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return c.err
}

func TestEnterClusterReturnsErrorOnTransientK8sError(t *testing.T) {
	restful.DefaultContainer = restful.NewContainer()

	// clientManager returns non-NotFound transient error (e.g. timeout)
	transientErr := fmt.Errorf("connection timeout")
	errClient := &errorClient{err: transientErr}
	clientManager := &ClientManager{
		clients: []client.Client{errClient},
	}

	// Storage contains a historical session entry
	mockReader := &mockStorageReader{
		clusters: []utils.ClusterInfo{
			{
				Namespace:       "default",
				Name:            "cluster-transient-err",
				SessionName:     "session_2026-04-22_10-00-00_000000_1",
				CreateTimeStamp: 1000,
			},
		},
	}

	handler := &ServerHandler{
		enableLiveClusters: true,
		maxClusters:        100,
		reader:             mockReader,
		clientManager:      clientManager,
	}

	fp := &fakeProcessor{
		fn: func(_ context.Context, _ utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
			return SessionStatusProcessed, &eventserver.SessionSnapshot{}, nil
		},
	}
	handler.sessionLoader = NewSessionLoader(context.Background(), fp, DefaultSessionProcessTimeout, DefaultSessionCacheSize, defaultSessionCacheMaxBytes, DefaultSessionCacheTTL)

	routerRayClusterSet(handler)
	container := restful.DefaultContainer

	// Request "latest" when K8s returns transient error
	req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-transient-err/latest", nil)
	resp := httptest.NewRecorder()
	container.ServeHTTP(resp, req)

	// Since non-NotFound errors are propagated upwards, it should return 500 InternalServerError rather than falling back to storage
	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("Expected status 500 (StatusInternalServerError), got %d: %s", resp.Code, resp.Body.String())
	}
}

func TestEnterClusterRayJobAndRayService(t *testing.T) {
	restful.DefaultContainer = restful.NewContainer()

	mockReader := &mockStorageReader{
		clusters: []utils.ClusterInfo{
			{
				Namespace:       "default",
				Name:            "rayjob-cluster",
				SessionName:     "session_2026-04-22_10-00-00_000000_1",
				CreateTimeStamp: 1000,
				OwnerKind:       "rayjob",
				OwnerName:       "my-job",
			},
			{
				Namespace:       "default",
				Name:            "rayservice-cluster",
				SessionName:     "session_2026-04-22_10-00-00_000000_2",
				CreateTimeStamp: 2000,
				OwnerKind:       "rayservice",
				OwnerName:       "my-service",
			},
		},
	}

	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	clientManager := &ClientManager{
		clients: []client.Client{k8sClient},
	}

	handler := &ServerHandler{
		enableLiveClusters: true,
		maxClusters:        100,
		reader:             mockReader,
		clientManager:      clientManager,
	}

	fp := &fakeProcessor{
		fn: func(_ context.Context, _ utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
			return SessionStatusProcessed, &eventserver.SessionSnapshot{}, nil
		},
	}
	handler.sessionLoader = NewSessionLoader(context.Background(), fp, DefaultSessionProcessTimeout, DefaultSessionCacheSize, defaultSessionCacheMaxBytes, DefaultSessionCacheTTL)

	routerRayClusterSet(handler)
	container := restful.DefaultContainer

	t.Run("Enter RayJob by owner name without session defaults to latest", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/rayjob/my-job", nil)
		resp := httptest.NewRecorder()
		container.ServeHTTP(resp, req)

		if resp.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", resp.Code, resp.Body.String())
		}
		cookies := resp.Result().Cookies()
		cookieMap := make(map[string]*http.Cookie)
		for _, cookie := range cookies {
			cookieMap[cookie.Name] = cookie
		}
		if c, ok := cookieMap[COOKIE_SESSION_NAME_KEY]; !ok || c.Value != "session_2026-04-22_10-00-00_000000_1" {
			t.Errorf("Expected cookie %s to be 'session_2026-04-22_10-00-00_000000_1', got %v", COOKIE_SESSION_NAME_KEY, c)
		}
		if c, ok := cookieMap[COOKIE_OWNER_KIND_KEY]; !ok || c.Value != "rayjob" {
			t.Errorf("Expected cookie %s to be 'rayjob', got %v", COOKIE_OWNER_KIND_KEY, c)
		}
	})

	t.Run("Enter RayService by owner name with specific session", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/rayservice/my-service/session_2026-04-22_10-00-00_000000_2", nil)
		resp := httptest.NewRecorder()
		container.ServeHTTP(resp, req)

		if resp.Code != http.StatusOK {
			t.Fatalf("Expected status 200, got %d: %s", resp.Code, resp.Body.String())
		}
		cookies := resp.Result().Cookies()
		cookieMap := make(map[string]*http.Cookie)
		for _, cookie := range cookies {
			cookieMap[cookie.Name] = cookie
		}
		if c, ok := cookieMap[COOKIE_SESSION_NAME_KEY]; !ok || c.Value != "session_2026-04-22_10-00-00_000000_2" {
			t.Errorf("Expected cookie %s to be 'session_2026-04-22_10-00-00_000000_2', got %v", COOKIE_SESSION_NAME_KEY, c)
		}
		if c, ok := cookieMap[COOKIE_OWNER_KIND_KEY]; !ok || c.Value != "rayservice" {
			t.Errorf("Expected cookie %s to be 'rayservice', got %v", COOKIE_OWNER_KIND_KEY, c)
		}
	})
}

// newDisabledLiveHandler returns a handler with live clusters disabled and a single RayCluster that
// exists in both Kubernetes and storage. status controls what the fake session loader reports for
// its stored session.
func newDisabledLiveHandler(t *testing.T, status SessionStatus) *ServerHandler {
	t.Helper()

	handler := &ServerHandler{
		maxClusters: 100,
		reader: &mockStorageReader{
			clusters: []utils.ClusterInfo{{
				Namespace:   "default",
				Name:        "cluster-running",
				SessionName: "session_2026-04-22_10-00-00_000000_1",
			}},
		},
		clientManager: newTestClientManager(liveRayCluster("default", "cluster-running")),
	}
	fp := &fakeProcessor{
		fn: func(_ context.Context, _ utils.ClusterInfo) (SessionStatus, *eventserver.SessionSnapshot, error) {
			if status == SessionStatusProcessed {
				return status, &eventserver.SessionSnapshot{}, nil
			}
			return status, nil, nil
		},
	}
	handler.sessionLoader = NewSessionLoader(context.Background(), fp, DefaultSessionProcessTimeout, DefaultSessionCacheSize, defaultSessionCacheMaxBytes, DefaultSessionCacheTTL)
	return handler
}

// TestListClusters verifies live entries appear only when --enable-live-clusters is set.
func TestListClusters(t *testing.T) {
	t.Run("Omits live clusters when live access is disabled", func(t *testing.T) {
		handler := newDisabledLiveHandler(t, SessionStatusProcessed)

		for _, c := range handler.listClusters(100) {
			if c.SessionName == "live" {
				t.Fatalf("listClusters returned a live entry for %s/%s while live clusters are disabled", c.Namespace, c.Name)
			}
		}
	})

	t.Run("Includes live clusters when live access is enabled", func(t *testing.T) {
		handler := newDisabledLiveHandler(t, SessionStatusProcessed)
		handler.enableLiveClusters = true

		for _, c := range handler.listClusters(100) {
			if c.SessionName == "live" {
				return
			}
		}
		t.Fatal("listClusters returned no live entry while live clusters are enabled")
	})
}

// TestEnterClusterWithLiveClustersDisabled checks how /enter_cluster answers while
// --enable-live-clusters is off.
func TestEnterClusterWithLiveClustersDisabled(t *testing.T) {
	t.Run("Explicit live session is rejected", func(t *testing.T) {
		restful.DefaultContainer = restful.NewContainer()
		handler := newDisabledLiveHandler(t, SessionStatusProcessed)
		routerRayClusterSet(handler)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-running/live", nil)
		resp := httptest.NewRecorder()
		restful.DefaultContainer.ServeHTTP(resp, req)

		if resp.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", resp.Code, resp.Body.String())
		}
		for _, c := range resp.Result().Cookies() {
			if c.Name == COOKIE_SESSION_NAME_KEY && c.Value == "live" {
				t.Error("a live session cookie was handed out while live clusters are disabled")
			}
		}
	})

	t.Run("Running cluster is rejected with an explanation", func(t *testing.T) {
		// A running cluster has no replayable state in memory, so once live access is refused there
		// is nothing for the handler to fall back to.
		restful.DefaultContainer = restful.NewContainer()
		handler := newDisabledLiveHandler(t, SessionStatusLive)
		routerRayClusterSet(handler)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-running/latest", nil)
		resp := httptest.NewRecorder()
		restful.DefaultContainer.ServeHTTP(resp, req)

		if resp.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", resp.Code, resp.Body.String())
		}
		// The frontend surfaces this body verbatim, so it has to say why rather than just "not found".
		if !strings.Contains(resp.Body.String(), "still running") {
			t.Errorf("expected the body to explain the cluster is still running, got %q", resp.Body.String())
		}
	})

	t.Run("Stored session of a dead cluster still works", func(t *testing.T) {
		restful.DefaultContainer = restful.NewContainer()
		handler := newDisabledLiveHandler(t, SessionStatusProcessed)
		routerRayClusterSet(handler)

		req := httptest.NewRequestWithContext(t.Context(), "GET", "/enter_cluster/default/raycluster/cluster-running/latest", nil)
		resp := httptest.NewRecorder()
		restful.DefaultContainer.ServeHTTP(resp, req)

		if resp.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
		}
		for _, c := range resp.Result().Cookies() {
			if c.Name == COOKIE_SESSION_NAME_KEY && c.Value != "session_2026-04-22_10-00-00_000000_1" {
				t.Errorf("expected the stored session cookie, got %q", c.Value)
			}
		}
	})
}
