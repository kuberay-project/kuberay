package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

var instanceWithRouteEnabled = &rayv1.RayCluster{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "raycluster-sample",
		Namespace: "default",
		Annotations: map[string]string{
			IngressClassAnnotationKey: "nginx",
		},
	},
	Spec: rayv1.RayClusterSpec{
		HeadGroupSpec: rayv1.HeadGroupSpec{
			EnableIngress: new(true),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:    "ray-head",
							Image:   "rayproject/autoscaler",
							Command: []string{"python"},
							Args:    []string{"/opt/code.py"},
						},
					},
				},
			},
		},
	},
}

func TestBuildRouteForHeadService(t *testing.T) {
	route, err := BuildRouteForHeadService(*instanceWithRouteEnabled)
	require.NoError(t, err)

	// Test name
	assert.Equal(t, instanceWithIngressEnabled.ObjectMeta.Name+"-head-route", route.Name)

	// Test To subject
	assert.Equal(t, "Service", route.Spec.To.Kind)

	// Test Service name
	assert.Equal(t, instanceWithIngressEnabled.ObjectMeta.Name+"-head-svc", route.Spec.To.Name)

	// Test Service port
	assert.Equal(t, intstr.FromInt(8265), route.Spec.Port.TargetPort)
}
