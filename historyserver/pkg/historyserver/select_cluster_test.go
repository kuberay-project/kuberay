package historyserver

import (
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/html"
)

func TestSelectClusterFiles(t *testing.T) {
	mux := http.NewServeMux()
	for path, file := range selectClusterFiles {
		mux.Handle(path, file)
	}
	get := func(t *testing.T, path, ifNoneMatch string) *httptest.ResponseRecorder {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil)
		if ifNoneMatch != "" {
			req.Header.Set("If-None-Match", ifNoneMatch)
		}
		resp := httptest.NewRecorder()
		mux.ServeHTTP(resp, req)
		return resp
	}

	tests := map[string]struct {
		path        string
		contentType string
		body        []byte
	}{
		"page":                     {path: "/select_cluster", contentType: "text/html; charset=utf-8", body: html.ClusterSelectorHTML},
		"page with trailing slash": {path: "/select_cluster/", contentType: "text/html; charset=utf-8", body: html.ClusterSelectorHTML},
		"stylesheet":               {path: "/select_cluster/select_cluster.css", contentType: "text/css; charset=utf-8", body: html.ClusterSelectorCSS},
		"script":                   {path: "/select_cluster/select_cluster.js", contentType: "text/javascript; charset=utf-8", body: html.ClusterSelectorJS},
		"font 400":                 {path: "/select_cluster/fonts/roboto-latin-400-normal.woff2", contentType: "font/woff2", body: html.ClusterSelectorRoboto400},
		"font 500":                 {path: "/select_cluster/fonts/roboto-latin-500-normal.woff2", contentType: "font/woff2", body: html.ClusterSelectorRoboto500},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			resp := get(t, tc.path, "")

			require.Equal(t, http.StatusOK, resp.Code)
			assert.Equal(t, tc.body, resp.Body.Bytes())
			assert.Equal(t, tc.contentType, resp.Header().Get("Content-Type"))
			assert.Equal(t, "nosniff", resp.Header().Get("X-Content-Type-Options"))
			assert.NotContains(t, resp.Header().Get("Content-Security-Policy"), "'unsafe-inline'")

			etag := resp.Header().Get("ETag")
			require.NotEmpty(t, etag)
			notModified := get(t, tc.path, etag)
			assert.Equal(t, http.StatusNotModified, notModified.Code)
			assert.Empty(t, notModified.Body.Bytes())

			// A copy cached before an upgrade has another ETag, so it gets the new file.
			modified := get(t, tc.path, `"stale"`)
			assert.Equal(t, http.StatusOK, modified.Code)
			assert.Equal(t, tc.body, modified.Body.Bytes())
		})
	}

	assert.Equal(t, http.StatusNotFound, get(t, "/select_cluster/missing.js", "").Code)
}

// The page and its stylesheet must load exactly the files that routerSelectCluster serves.
func TestSelectClusterPageReferences(t *testing.T) {
	refPattern := regexp.MustCompile(`(?:href="|src="|url\(")(/select_cluster/[^"]*)"`)
	loaded := map[string]bool{}
	for _, file := range [][]byte{html.ClusterSelectorHTML, html.ClusterSelectorCSS} {
		for _, ref := range refPattern.FindAllSubmatch(file, -1) {
			loaded[string(ref[1])] = true
		}
	}

	served := map[string]bool{}
	for path, file := range selectClusterFiles {
		if file.contentType != selectClusterPage.contentType {
			served[path] = true
		}
	}
	assert.Equal(t, served, loaded)
}
