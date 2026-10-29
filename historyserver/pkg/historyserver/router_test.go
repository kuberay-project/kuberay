package historyserver

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/emicklei/go-restful/v3"
)

func TestRouterAPI(t *testing.T) {
	previousContainer := restful.DefaultContainer
	restful.DefaultContainer = restful.NewContainer()
	t.Cleanup(func() {
		restful.DefaultContainer = previousContainer
	})
	routerAPI(&ServerHandler{})

	clusterCookies := map[string]string{
		COOKIE_CLUSTER_NAME_KEY:      "cluster-a",
		COOKIE_CLUSTER_NAMESPACE_KEY: "default",
		COOKIE_SESSION_NAME_KEY:      "live",
	}
	cases := []struct {
		name       string
		method     string
		path       string
		cookies    map[string]string
		wantStatus int
	}{
		{"authentication_mode", http.MethodGet, "/api/authentication_mode", nil, http.StatusNotFound},
		{"authenticate", http.MethodPost, "/api/authenticate", nil, http.StatusNotFound},
		{"authentication_mode_with_cluster_cookies", http.MethodGet, "/api/authentication_mode", clusterCookies, http.StatusNotFound},
		{"authenticate_with_cluster_cookies", http.MethodPost, "/api/authenticate", clusterCookies, http.StatusNotFound},
		{"cluster_status_requires_cookies", http.MethodGet, "/api/cluster_status", nil, http.StatusBadRequest},
		{"fallback_requires_cookies", http.MethodGet, "/api/custom_endpoint", nil, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequestWithContext(t.Context(), tc.method, tc.path, nil)
			req.Header.Set("Content-Type", restful.MIME_JSON)
			for name, value := range tc.cookies {
				req.AddCookie(&http.Cookie{Name: name, Value: value})
			}
			resp := httptest.NewRecorder()
			restful.DefaultContainer.ServeHTTP(resp, req)

			if resp.Code != tc.wantStatus {
				t.Fatalf("expected %d, got %d: %s", tc.wantStatus, resp.Code, resp.Body.String())
			}
		})
	}
}
