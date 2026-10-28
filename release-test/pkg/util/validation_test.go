package util

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLintManifest(t *testing.T) {
	items := []Item{
		{
			ID:     "doc",
			Source: "https://docs.ray.io/en/master/kuberay/x.html",
		},
		{
			ID:     "gpu",
			Source: "ray-operator/config/samples/gpu.yaml", Requires: []string{"gpu"},
		},
	}
	step := func(run string) []Step { return []Step{{Run: run, Check: "true"}} }
	tests := []struct {
		name  string
		units []Unit
		want  []string // substrings, one per expected problem
	}{
		{
			name: "structure",
			units: []Unit{
				{ID: "doc", Steps: []Step{{Check: "true"}}},
				{ID: "doc", SourceSHA: "a", Steps: step("true")},
				{ID: "gpu", SourceSHA: "c", Steps: step("true")},
				{ID: "unknown", SourceSHA: "d"},
			},
			want: []string{
				"doc: duplicate unit", "doc: no source_sha", "doc: step 1 has no run",
				"gpu: the Item has requires",
				"unknown: not in items.yaml", "unknown: no steps",
			},
		},
		{
			name: "versions",
			units: []Unit{{ID: "doc", SourceSHA: "a", Steps: []Step{
				{Run: "kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/x.yaml"},
				{Run: "kubectl create -k \"github.com/ray-project/kuberay/ray-operator/config/default?ref=v1.5.1\""},
				{Run: "helm install kuberay-operator kuberay/kuberay-operator \\\n  --version 1.5.1"},
				{Run: "kubectl set image deploy/kuberay-operator kuberay-operator=quay.io/kuberay/operator:nightly"},
				{Run: "kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/$KUBERAY_VERSION/x.yaml"},
				{Run: "helm install prometheus prometheus-community/kube-prometheus-stack --version 45.0.0"},
			}}},
			want: []string{"kuberay/master/", "ref=v1.5.1", "--version 1.5.1", "operator:nightly"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			problems := LintManifest(items, tc.units)
			require.Len(t, problems, len(tc.want), strings.Join(problems, "\n"))
			for _, want := range tc.want {
				assert.True(t, slices.ContainsFunc(problems, func(problem string) bool { return strings.Contains(problem, want) }),
					"no problem mentions %q in:\n%s", want, strings.Join(problems, "\n"))
			}
		})
	}
}
