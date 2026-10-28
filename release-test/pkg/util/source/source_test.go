package source

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/release-test/pkg/util"
)

func TestBlobSHA(t *testing.T) {
	// The value `git hash-object` prints for the same content.
	assert.Equal(t, "ce013625030ba8dba906f756967f9e9ca394464a", BlobSHA([]byte("hello\n")))
}

func TestDocsPagePath(t *testing.T) {
	_, ok := docsPagePath(util.DocsMaster+"cluster/kubernetes/examples/ml-example.html", util.DocsMaster+"kuberay/index.html")
	assert.False(t, ok, "a removed page redirects to the section index")

	page, ok := docsPagePath(util.DocsMaster+"kuberay/examples/index.html", util.DocsMaster+"kuberay/examples/index.html")
	assert.Equal(t, "kuberay/examples/index", page)
	assert.True(t, ok, "an index page asked for by name")
}

func TestPageCommands(t *testing.T) {
	tests := []struct {
		name string
		page string
		want string
	}{
		{
			name: "prose around a code block",
			page: "# Title\n\nInstall it.\n\n```sh\nkubectl get pods\n```\n\nMore prose.\n",
			want: "kubectl get pods",
		},
		{
			name: "a code block inside a directive",
			page: "`````{tab-set}\n\n````{tab-item} Helm\nSome prose in the tab.\n\n```sh\nhelm install x\n```\n````\n\n`````\n",
			want: "helm install x",
		},
		{
			name: "prose in an admonition",
			page: "```{note}\nSome prose in a note.\n```\n\n```sh\nkubectl get pods\n```\n",
			want: "kubectl get pods",
		},
		{
			name: "a code directive",
			page: "```{code-block} bash\nhelm install x\n```\n",
			want: "helm install x",
		},
		{
			name: "a page without code blocks is hashed whole",
			page: "# Concepts\n\nNo commands here.\n",
			want: "# Concepts\n\nNo commands here.\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, string(pageCommands([]byte(tc.page))))
		})
	}
}

// TestPageCommandsEdits checks which edits to a code block leave the unit fresh.
func TestPageCommandsEdits(t *testing.T) {
	tests := []struct {
		name          string
		before, after string // the page's one code block
		fresh         bool
	}{
		{
			name:   "a command",
			before: "kubectl get pods",
			after:  "kubectl get pods -A",
		},
		{
			name:   "a comment",
			before: "# List the Ray Pods\nkubectl get pods",
			after:  "# List the RayCluster's Pods\nkubectl get pods",
			fresh:  true,
		},
		{
			name:   "the chart version on a continued line",
			before: "helm install kuberay-operator kuberay/kuberay-operator \\\n  --version 1.7.0 \\\n  --set a=b",
			after:  "helm install kuberay-operator kuberay/kuberay-operator \\\n  --version 1.8.0 \\\n  --set a=b",
			fresh:  true,
		},
		{
			name:   "a third-party chart version",
			before: "helm install prometheus prometheus-community/kube-prometheus-stack --version 48.2.1",
			after:  "helm install prometheus prometheus-community/kube-prometheus-stack --version 49.0.0",
		},
		{
			name:   "a raw URL",
			before: "kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.7.0/ray-operator/config/samples/ray-cluster.sample.yaml",
			after:  "kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/v1.8.0-rc.0/ray-operator/config/samples/ray-cluster.sample.yaml",
			fresh:  true,
		},
		{
			name:   "the plugin tarball",
			before: "curl -LO https://github.com/ray-project/kuberay/releases/download/v1.7.0/kubectl-ray_v1.7.0_linux_amd64.tar.gz",
			after:  "curl -LO https://github.com/ray-project/kuberay/releases/download/v1.8.0/kubectl-ray_v1.8.0_linux_amd64.tar.gz",
			fresh:  true,
		},
		{
			name:   "the operator image",
			before: "image: quay.io/kuberay/operator:v1.7.0",
			after:  "image: quay.io/kuberay/operator:v1.8.0",
			fresh:  true,
		},
		{
			name:   "ArgoCD targetRevision",
			before: `    targetRevision: "1.7.0"`,
			after:  `    targetRevision: "1.8.0"`,
			fresh:  true,
		},
		{
			name:   "a Ray image",
			before: "image: rayproject/ray:2.46.0",
			after:  "image: rayproject/ray:2.52.0",
		},
	}
	commands := func(block string) string {
		return string(pageCommands([]byte("```sh\n" + block + "\n```\n")))
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.fresh {
				assert.Equal(t, commands(tc.before), commands(tc.after))
			} else {
				assert.NotEqual(t, commands(tc.before), commands(tc.after))
			}
		})
	}
}

func TestFetchSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v9.9.9/ray-operator/config/samples/x.yaml":
			fmt.Fprint(w, "kind: RayCluster\n")
		case "/v9.9.9/ray-operator/config/samples/limited.yaml":
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	old := kuberayRaw
	kuberayRaw = srv.URL + "/"
	defer func() { kuberayRaw = old }()

	root := t.TempDir()
	paths := util.Paths{Root: root, Cache: filepath.Join(root, "release-test", ".cache")}
	fetchSample := func(name string) (Source, error) {
		item := util.Item{ID: "sample", Source: "ray-operator/config/samples/" + name}
		return Fetch(context.Background(), paths, item, "v9.9.9")
	}

	t.Run("hashed as it is at the tag", func(t *testing.T) {
		src, err := fetchSample("x.yaml")
		require.NoError(t, err)
		assert.Equal(t, BlobSHA([]byte("kind: RayCluster\n")), src.SHA, "not as it is in this checkout")
	})
	t.Run("missing at the tag", func(t *testing.T) {
		src, err := fetchSample("new.yaml")
		require.NoError(t, err)
		assert.Equal(t, srv.URL+"/v9.9.9/ray-operator/config/samples/new.yaml", src.Dead)
	})
	t.Run("rate limited", func(t *testing.T) {
		_, err := fetchSample("limited.yaml")
		require.ErrorContains(t, err, "HTTP 429", "a rate limit must not be taken for a removed source")
	})
}
