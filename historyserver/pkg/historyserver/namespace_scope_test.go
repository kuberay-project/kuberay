package historyserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/emicklei/go-restful/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

func TestNamespaceScopeValidation(t *testing.T) {
	for _, tc := range []struct{ namespace, prefix string }{
		{"team-a", "team-"},
		{"Team-a", ""},
		{"team-a/other", ""},
		{"", "team-*"},
		{"", "*"},
		{"", " team-"},
		{"", "team-/"},
		{"", "-team"},
		{"", "-"},
		{"", strings.Repeat("a", 64)},
	} {
		t.Run(tc.namespace+"_"+tc.prefix, func(t *testing.T) {
			_, err := newNamespaceScope(tc.namespace, tc.prefix)
			require.Error(t, err)
			// Reject invalid configuration before attempting to load Kubernetes credentials.
			_, err = NewClientManager(ClientManagerConfig{Namespace: tc.namespace, NamespacePrefix: tc.prefix})
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "kubeconfig")
		})
	}
	for _, tc := range []struct{ namespace, prefix string }{
		{"", ""}, {"team-a", ""}, {"", "team-"}, {"", "team--"}, {"", "team"},
	} {
		_, err := newNamespaceScope(tc.namespace, tc.prefix)
		require.NoError(t, err)
	}
}

type scopeRecordingClient struct {
	client.Client
	listNamespaces []string
	getCount       int
}

func (c *scopeRecordingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	options := &client.ListOptions{}
	options.ApplyOptions(opts)
	c.listNamespaces = append(c.listNamespaces, options.Namespace)
	return c.Client.List(ctx, list, opts...)
}

func (c *scopeRecordingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.getCount++
	return c.Client.Get(ctx, key, obj, opts...)
}

func TestNamespaceScopedListPreservesSelectors(t *testing.T) {
	selected := liveRayCluster("team-a", "selected")
	selected.Labels = map[string]string{"workload": "training"}
	excluded := liveRayCluster("team-a", "other-label")
	excluded.Labels = map[string]string{"workload": "serving"}
	otherNamespace := selected.DeepCopy()
	otherNamespace.Namespace = "team-b"
	manager := newTestClientManager(selected, excluded, otherNamespace)
	manager.namespaceScope = namespaceScope{namespace: "team-a"}
	recording := &scopeRecordingClient{Client: manager.clients[0]}
	manager.clients[0] = recording

	clusters, err := manager.ListRayClusters(t.Context(), client.MatchingLabels{"workload": "training"})
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "selected", clusters[0].Name)
	assert.Equal(t, []string{"team-a"}, recording.listNamespaces,
		"exact scope must use a namespaced API request, compatible with a RoleBinding")

	_, err = manager.ListRayClusters(t.Context(), client.InNamespace("team-b"))
	require.ErrorIs(t, err, errNamespaceNotAllowed)
	assert.Len(t, recording.listNamespaces, 1, "denied requests must not reach Kubernetes")
}

func TestNamespaceScopedClusterInventory(t *testing.T) {
	for _, tc := range []struct {
		name        string
		scope       namespaceScope
		live        bool
		wantLive    int
		wantArchive string
	}{
		{"unrestricted", namespaceScope{}, true, 3, "outside"},
		{"exact", namespaceScope{namespace: "team-a"}, true, 1, "archive-a"},
		{"prefix", namespaceScope{prefix: "team-"}, true, 2, "archive-a"},
		{"archive-only", namespaceScope{prefix: "team-"}, false, 0, "archive-a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := newTestClientManager(liveRayCluster("team-a", "live-a"),
				liveRayCluster("team-b", "live-b"), liveRayCluster("other", "outside"))
			manager.namespaceScope = tc.scope
			reader := &mockStorageReader{clusters: []utils.ClusterInfo{
				{Name: "outside", Namespace: "other", SessionName: "session", CreateTimeStamp: 100},
				{Name: "archive-b", Namespace: "team-b", SessionName: "session", CreateTimeStamp: 10},
				{Name: "archive-a", Namespace: "team-a", SessionName: "session", CreateTimeStamp: 20},
			}}
			handler := &ServerHandler{clientManager: manager, reader: reader, enableLiveClusters: tc.live, maxClusters: 1}
			previous := restful.DefaultContainer
			restful.DefaultContainer = restful.NewContainer()
			t.Cleanup(func() { restful.DefaultContainer = previous })
			routerClusters(handler)
			resp := httptest.NewRecorder()
			restful.DefaultContainer.ServeHTTP(resp, httptest.NewRequestWithContext(t.Context(), "GET", "/clusters", nil))
			require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
			var clusters []utils.ClusterInfo
			require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &clusters))
			require.Len(t, clusters, tc.wantLive+1)
			var liveCount int
			for _, info := range clusters {
				assert.True(t, tc.scope.allows(info.Namespace))
				if info.SessionName == "live" {
					liveCount++
				} else {
					assert.Equal(t, tc.wantArchive, info.Name, "filter archives before applying the result limit")
				}
			}
			assert.Equal(t, tc.wantLive, liveCount)
			assert.Equal(t, "outside", reader.clusters[0].Name, "do not mutate the storage reader's slice")

			if tc.scope.prefix != "" && tc.live {
				// A newly created matching namespace needs no server restart or Namespace-list permission.
				require.NoError(t, manager.Client().Create(t.Context(), liveRayCluster("team-new", "new-cluster")))
				assert.Len(t, handler.listClusters(1), tc.wantLive+2)
			}
		})
	}
}

func TestNamespaceScopeRejectsSessionBypasses(t *testing.T) {
	for _, scope := range []namespaceScope{{namespace: "team-a"}, {prefix: "team-"}} {
		manager := newTestClientManager()
		manager.namespaceScope = scope
		recording := &scopeRecordingClient{Client: manager.clients[0]}
		manager.clients[0] = recording
		reader := &mockStorageReader{}
		handler := &ServerHandler{clientManager: manager, reader: reader, enableLiveClusters: true, useAuthTokenMode: true}
		previous := restful.DefaultContainer
		restful.DefaultContainer = restful.NewContainer()
		t.Cleanup(func() { restful.DefaultContainer = previous })
		routerRayClusterSet(handler)

		deniedNamespaces := []string{"other", "not-team-a", "team-", "Team-a", "team-a/other"}
		if scope.namespace != "" {
			deniedNamespaces = append(deniedNamespaces, "team-b")
		}
		for _, namespace := range deniedNamespaces {
			_, err := manager.ListRayClusters(t.Context(), client.InNamespace(namespace))
			require.ErrorIs(t, err, errNamespaceNotAllowed)
			_, err = manager.GetRayCluster(t.Context(), namespace, "cluster")
			require.ErrorIs(t, err, errNamespaceNotAllowed)
			_, err = manager.GetAuthTokenForRayCluster(t.Context(), namespace, "cluster")
			require.ErrorIs(t, err, errNamespaceNotAllowed)
			manager.svcInfoCache.Add(namespace+"/cluster", ServiceInfo{}, svcInfoCacheTTL)
			_, err = manager.GetSvcInfo("cluster", namespace)
			require.ErrorIs(t, err, errNamespaceNotAllowed, "cached routes must not bypass scope")

			for _, session := range []string{"live", "latest", "session_2026-04-22_10-00-00_000000_1"} {
				_, _, err := handler.resolveSession(t.Context(), namespace, "raycluster", "cluster", session)
				require.ErrorIs(t, err, errNamespaceNotAllowed)
				cookies := liveCookies()
				cookies[COOKIE_CLUSTER_NAMESPACE_KEY], cookies[COOKIE_SESSION_NAME_KEY] = namespace, session
				resp, reached := serveThroughCookieHandle(handler, cookies)
				assert.Equal(t, http.StatusForbidden, resp.Code, resp.Body.String())
				assert.False(t, reached)
				assert.Empty(t, resp.Result().Cookies(), "denial must not refresh session cookies")
			}
		}
		for _, kind := range []string{"raycluster", "rayjob", "rayservice"} {
			for _, session := range []string{"live", "latest", "session_2026-04-22_10-00-00_000000_1"} {
				resp := httptest.NewRecorder()
				url := "/enter_cluster/other/" + kind + "/cluster/" + session
				restful.DefaultContainer.ServeHTTP(resp, httptest.NewRequestWithContext(t.Context(), "GET", url, nil))
				assert.Equal(t, http.StatusForbidden, resp.Code, resp.Body.String())
				assert.Empty(t, resp.Result().Cookies())
			}
		}
		assert.Zero(t, reader.listCount, "deny before listing archives")
		assert.Zero(t, recording.getCount, "deny before reading cluster or authentication Secrets")
		assert.Empty(t, recording.listNamespaces)
		restful.DefaultContainer = previous
	}
}

func TestNamespaceScopeAllowsSelectedSessions(t *testing.T) {
	for _, scope := range []namespaceScope{{namespace: "team-a"}, {prefix: "team-"}} {
		cluster := liveRayCluster("team-a", "cluster-a")
		controller := true
		cluster.OwnerReferences = []metav1.OwnerReference{{Kind: "RayJob", Name: "job-a", Controller: &controller}}
		manager := newTestClientManager(cluster)
		manager.namespaceScope = scope
		archive := utils.ClusterInfo{
			Name: cluster.Name, Namespace: cluster.Namespace,
			SessionName: "session_2026-04-22_10-00-00_000000_1", OwnerKind: "rayjob", OwnerName: "job-a",
		}
		handler := &ServerHandler{clientManager: manager, reader: &mockStorageReader{clusters: []utils.ClusterInfo{archive}}, enableLiveClusters: true}
		info, found, err := handler.resolveSession(t.Context(), "team-a", "raycluster", "cluster-a", "live")
		require.NoError(t, err)
		assert.True(t, found)
		assert.Equal(t, "live", info.SessionName)
		for _, kind := range []string{"raycluster", "rayjob"} {
			name := "cluster-a"
			if kind == "rayjob" {
				name = "job-a"
			}
			info, found, err = handler.resolveSession(t.Context(), "team-a", kind, name, archive.SessionName)
			require.NoError(t, err)
			assert.True(t, found)
			assert.Equal(t, archive, info)
		}
		for _, session := range []string{"live", archive.SessionName} {
			cookies := liveCookies()
			cookies[COOKIE_CLUSTER_NAMESPACE_KEY], cookies[COOKIE_SESSION_NAME_KEY] = "team-a", session
			resp, reached := serveThroughCookieHandle(handler, cookies)
			assert.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
			assert.True(t, reached)
		}
	}
}
