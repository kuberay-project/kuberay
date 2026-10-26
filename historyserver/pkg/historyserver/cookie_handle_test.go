package historyserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/emicklei/go-restful/v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// newTestClientManager builds a ClientManager backed by a fake client holding objs.
func newTestClientManager(objs ...client.Object) *ClientManager {
	scheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(scheme)
	return &ClientManager{
		clients:      []client.Client{fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()},
		svcInfoCache: cache.NewLRUExpireCache(16),
	}
}

// liveRayCluster returns a RayCluster whose head service is resolvable by fetchSvcInfo.
func liveRayCluster(namespace, name string) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Status: rayv1.RayClusterStatus{
			Head: rayv1.HeadInfo{ServiceName: name + "-head-svc"},
		},
	}
}

// serveThroughCookieHandle drives one request through CookieHandle and reports whether the
// downstream handler ran.
func serveThroughCookieHandle(handler *ServerHandler, cookies map[string]string) (*httptest.ResponseRecorder, bool) {
	reached := false

	container := restful.NewContainer()
	ws := new(restful.WebService)
	// Mirror the real routes: CookieHandle answers with WriteHeaderAndEntity, which needs a
	// negotiable media type or go-restful replies 406 before the body is written.
	ws.Path("/probe").Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)
	ws.Route(ws.GET("").To(func(_ *restful.Request, resp *restful.Response) {
		reached = true
		resp.WriteHeader(http.StatusOK)
	}).Filter(handler.CookieHandle))
	container.Add(ws)

	req := httptest.NewRequestWithContext(context.Background(), "GET", "/probe", nil)
	for name, value := range cookies {
		req.AddCookie(&http.Cookie{Name: name, Value: value})
	}
	resp := httptest.NewRecorder()
	container.ServeHTTP(resp, req)

	return resp, reached
}

func liveCookies() map[string]string {
	return map[string]string{
		COOKIE_CLUSTER_NAME_KEY:      "cluster-a",
		COOKIE_CLUSTER_NAMESPACE_KEY: "default",
		COOKIE_SESSION_NAME_KEY:      "live",
	}
}

// TestCookieHandleRejectsLiveSessionBeforeReadingSecret guards the confused-deputy path: in
// auth-token mode CookieHandle reads the cluster's auth token Secret on the caller's behalf, so the
// gate has to fire before that read, not after.
func TestCookieHandleRejectsLiveSessionBeforeReadingSecret(t *testing.T) {
	// The RayCluster exists, so GetSvcInfo would succeed and GetAuthTokenForRayCluster would run.
	// The fake client has no corev1 scheme registered, so any Secret read would error into a 500.
	handler := &ServerHandler{
		clientManager:    newTestClientManager(liveRayCluster("default", "cluster-a")),
		useAuthTokenMode: true,
	}

	resp, reached := serveThroughCookieHandle(handler, liveCookies())

	if resp.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", resp.Code, resp.Body.String())
	}
	if reached {
		t.Error("downstream handler ran; the live session must be rejected by the filter")
	}
}

// TestCookieHandleAllowsLiveSessionWhenEnabled keeps the gate from becoming an unconditional block.
func TestCookieHandleAllowsLiveSessionWhenEnabled(t *testing.T) {
	handler := &ServerHandler{
		clientManager:      newTestClientManager(liveRayCluster("default", "cluster-a")),
		enableLiveClusters: true,
	}

	resp, reached := serveThroughCookieHandle(handler, liveCookies())

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if !reached {
		t.Error("downstream handler did not run for an enabled live session")
	}
}

// TestCookieHandlePassesStoredSessionWhenLiveDisabled confirms the gate is scoped to the live
// sentinel and does not disturb replay of stored sessions.
func TestCookieHandlePassesStoredSessionWhenLiveDisabled(t *testing.T) {
	handler := &ServerHandler{clientManager: newTestClientManager()}

	cookies := liveCookies()
	cookies[COOKIE_SESSION_NAME_KEY] = "session_2026-04-22_10-00-00_000000_1"

	resp, reached := serveThroughCookieHandle(handler, cookies)

	if resp.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", resp.Code, resp.Body.String())
	}
	if !reached {
		t.Error("downstream handler did not run for a stored session")
	}
}
