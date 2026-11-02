package e2e

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/historyserver"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/localtest"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	testsupport "github.com/ray-project/kuberay/ray-operator/test/support"
)

// This test uses the real API server and a namespaced reader identity. Suspended
// RayClusters exercise discovery without starting Ray pods or requiring storage.
func TestNamespaceScopedHistoryServer(t *testing.T) {
	test := testsupport.With(t)
	// Initialize the log directory before namespace cleanup callbacks are registered.
	test.OutputDir()
	allowed := test.NewTestNamespace()
	outside := test.NewTestNamespace()
	for _, ns := range []string{allowed.Name, outside.Name} {
		_, err := test.Client().Ray().RayV1().RayClusters(ns).Create(test.Ctx(), &rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "scope-cluster", Namespace: ns},
			Spec: rayv1.RayClusterSpec{
				Suspend: new(true), RayVersion: "2.58.0",
				HeadGroupSpec: rayv1.HeadGroupSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "ray-head", Image: "rayproject/ray:2.58.0"}},
				}}},
			},
		}, metav1.CreateOptions{})
		require.NoError(t, err)
	}
	core := test.Client().Core()
	_, err := core.CoreV1().ServiceAccounts(allowed.Name).Create(test.Ctx(), &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{Name: "history-reader"},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = core.RbacV1().Roles(allowed.Name).Create(test.Ctx(), &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "history-reader"},
		Rules:      []rbacv1.PolicyRule{{APIGroups: []string{"ray.io"}, Resources: []string{"rayclusters"}, Verbs: []string{"get", "list"}}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = core.RbacV1().RoleBindings(allowed.Name).Create(test.Ctx(), &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "history-reader"},
		RoleRef:    rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: "history-reader"},
		Subjects:   []rbacv1.Subject{{Kind: "ServiceAccount", Name: "history-reader", Namespace: allowed.Name}},
	}, metav1.CreateOptions{})
	require.NoError(t, err)
	token, err := core.CoreV1().ServiceAccounts(allowed.Name).CreateToken(test.Ctx(), "history-reader", &authv1.TokenRequest{}, metav1.CreateOptions{})
	require.NoError(t, err)
	config := test.Client().Config()
	kubeconfig := filepath.Join(t.TempDir(), "reader.yaml")
	require.NoError(t, clientcmd.WriteToFile(clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{"test": {
			Server: config.Host, CertificateAuthorityData: config.CAData, CertificateAuthority: config.CAFile,
		}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"reader": {Token: token.Status.Token}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "reader"}},
		CurrentContext: "test",
	}, kubeconfig))
	cfg := historyserver.ClientManagerConfig{Kubeconfigs: kubeconfig, Namespace: allowed.Name, QPS: 10, Burst: 20}
	manager, err := historyserver.NewClientManager(cfg)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		clusters, listErr := manager.ListRayClusters(test.Ctx())
		return listErr == nil && len(clusters) == 1
	}, 30*time.Second, 200*time.Millisecond)

	// The same credentials cannot list cluster-wide: the test would fail on the
	// old discovery path, even though its namespaced Role allows get/list.
	cfg.Namespace = ""
	unscoped, err := historyserver.NewClientManager(cfg)
	require.NoError(t, err)
	_, err = unscoped.ListRayClusters(test.Ctx())
	require.True(t, apierrors.IsForbidden(err), "cluster-wide list must be denied: %v", err)

	handler, err := historyserver.NewServerHandler(&types.RayHistoryServerConfig{}, "", localtest.NewMockReader(), manager, nil, false, false, true)
	require.NoError(t, err)
	previousMux, previousContainer := http.DefaultServeMux, restful.DefaultContainer
	http.DefaultServeMux, restful.DefaultContainer = http.NewServeMux(), restful.NewContainer()
	t.Cleanup(func() { http.DefaultServeMux, restful.DefaultContainer = previousMux, previousContainer })
	handler.RegisterRouter()
	server := httptest.NewServer(restful.DefaultContainer)
	t.Cleanup(server.Close)
	httpClient := &http.Client{Timeout: 10 * time.Second}
	get := func(path string) *http.Response {
		request, requestErr := http.NewRequestWithContext(test.Ctx(), http.MethodGet, server.URL+path, nil)
		require.NoError(t, requestErr)
		response, requestErr := httpClient.Do(request)
		require.NoError(t, requestErr)
		return response
	}
	response := get("/clusters")
	var clusters []utils.ClusterInfo
	require.NoError(t, json.NewDecoder(response.Body).Decode(&clusters))
	require.NoError(t, response.Body.Close())
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Len(t, clusters, 1)
	assert.Equal(t, allowed.Name, clusters[0].Namespace)
	assert.Equal(t, "scope-cluster", clusters[0].Name)

	for _, session := range []string{"live", "latest", "session_2026-04-22_10-00-00_000000_1"} {
		response = get("/enter_cluster/" + outside.Name + "/raycluster/scope-cluster/" + session)
		require.NoError(t, response.Body.Close())
		assert.Equal(t, http.StatusForbidden, response.StatusCode)
		assert.Empty(t, response.Cookies())
	}
	request, err := http.NewRequestWithContext(test.Ctx(), http.MethodGet, server.URL+"/nodes", nil)
	require.NoError(t, err)
	for name, value := range map[string]string{"cluster_name": "scope-cluster", "cluster_namespace": outside.Name, "session_name": "live"} {
		request.AddCookie(&http.Cookie{Name: name, Value: value})
	}
	response, err = httpClient.Do(request)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	assert.Equal(t, http.StatusForbidden, response.StatusCode)
	assert.Empty(t, response.Cookies())
}
