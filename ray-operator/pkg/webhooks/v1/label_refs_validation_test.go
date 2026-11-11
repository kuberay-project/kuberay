package v1

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestValidateLabelRefs(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.NodeLabelDelivery, true)
	const zone, clique = "topology.kubernetes.io/zone", "nvidia.com/gpu.clique"
	allowed := []string{zone, clique}
	newSpec := func() *rayv1.RayClusterSpec {
		return &rayv1.RayClusterSpec{WorkerGroupSpecs: []rayv1.WorkerGroupSpec{{
			GroupName: "train",
			Template:  corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-worker"}}}},
			LabelRefs: []rayv1.LabelRef{
				nodeLabelRef(zone, "ray.io/zone"),
				nodeLabelRef(clique, ""),
			},
		}}}
	}

	tests := []struct {
		mutate        func(group *rayv1.WorkerGroupSpec)
		annotations   map[string]string
		name          string
		errorContains string
	}{
		{name: "valid mappings"},
		{name: "no labelRefs", mutate: func(g *rayv1.WorkerGroupSpec) { g.LabelRefs = nil }},
		{name: "empty labelRefs", mutate: func(g *rayv1.WorkerGroupSpec) { g.LabelRefs = []rayv1.LabelRef{} }},
		{
			name: "overwrite-container-cmd annotation",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Template.Annotations = map[string]string{utils.RayOverwriteContainerCmdAnnotationKey: "true"}
			},
			errorContains: utils.RayOverwriteContainerCmdAnnotationKey,
		},
		{
			name:          "overwrite-container-cmd annotation on the CR",
			annotations:   map[string]string{utils.RayOverwriteContainerCmdAnnotationKey: "true"},
			errorContains: utils.RayOverwriteContainerCmdAnnotationKey,
		},
		{
			name: "user command runs ray start",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Template.Spec.Containers[0].Command = []string{"ray start --address=head:6379 --block"}
			},
			errorContains: "runs ray start",
		},
		{
			name: "RAY_NODE_LABELS_JSON set in the pod template",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: utils.RAY_NODE_LABELS_JSON, Value: "{}"}}
			},
			errorContains: "managed by KubeRay",
		},
		{
			name:          "rayStartParams labels",
			mutate:        func(g *rayv1.WorkerGroupSpec) { g.RayStartParams = map[string]string{"labels": "a=b"} },
			errorContains: "cannot be combined with rayStartParams labels",
		},
		{
			name:          "rayStartParams labels-file",
			mutate:        func(g *rayv1.WorkerGroupSpec) { g.RayStartParams = map[string]string{"labels-file": "/x.yaml"} },
			errorContains: "cannot be combined with rayStartParams labels-file",
		},
		{
			name:          "Ray key also set in the group's static labels",
			mutate:        func(g *rayv1.WorkerGroupSpec) { g.Labels = map[string]string{"ray.io/zone": "static"} },
			errorContains: `Ray label "ray.io/zone" is also set in labels`,
		},
		{
			name:          "name is not a valid label key",
			mutate:        func(g *rayv1.WorkerGroupSpec) { g.LabelRefs[0].Name = "ray zone!" },
			errorContains: "labelRefs[0].name",
		},
		{
			name: "fieldPath names a malformed label key",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.LabelRefs[1].ValueFrom.NodeRef.FieldPath = "metadata.labels['bad key!']"
			},
			errorContains: "labelRefs[1].valueFrom.nodeRef.fieldPath",
		},
		{
			name: "fieldPath is not a node label",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.LabelRefs[1].ValueFrom.NodeRef.FieldPath = "metadata.annotations['nvidia.com/gpu.clique']"
			},
			errorContains: "only metadata.labels['<key>'] is supported",
		},
		{
			name: "node label outside the allowlist",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.LabelRefs[1].ValueFrom.NodeRef.FieldPath = "metadata.labels['cloud.google.com/gke-nodepool']"
			},
			errorContains: `node label "cloud.google.com/gke-nodepool" is not in the operator's allowedNodeLabels`,
		},
		{
			name: "two mappings deliver the same Ray key",
			mutate: func(g *rayv1.WorkerGroupSpec) {
				g.LabelRefs[1].Name = "ray.io/zone"
			},
			errorContains: `Duplicate value: "ray.io/zone"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := newSpec()
			if tt.mutate != nil {
				tt.mutate(&spec.WorkerGroupSpecs[0])
			}
			err := validateLabelRefs(spec, nil, tt.annotations, allowed, field.NewPath("spec"))
			if tt.errorContains == "" {
				require.Nil(t, err)
				return
			}
			require.NotNil(t, err)
			require.Contains(t, err.Error(), tt.errorContains)
		})
	}

	t.Run("gate off", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.NodeLabelDelivery, false)
		spec := newSpec()
		err := validateLabelRefs(spec, nil, nil, allowed, field.NewPath("spec"))
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "requires the NodeLabelDelivery feature gate to be enabled")
		spec.WorkerGroupSpecs[0].LabelRefs = nil
		require.Nil(t, validateLabelRefs(spec, nil, nil, allowed, field.NewPath("spec")))
	})

	t.Run("unchanged labelRefs skip the gate and allowlist on update", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.NodeLabelDelivery, false)
		old := newSpec()
		spec := newSpec()
		spec.WorkerGroupSpecs[0].RayStartParams = map[string]string{"num-cpus": "4"}
		require.Nil(t, validateLabelRefs(spec, old, nil, nil, field.NewPath("spec")))
	})
}
