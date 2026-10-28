package apiserversdk

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestGetRayClusterProxy(t *testing.T) {
	tCtx, err := NewEnd2EndTestingContext(t)
	require.NoError(t, err, "No error expected when creating testing context")
	rayClient := tCtx.GetRayHttpClient()
	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      tCtx.GetRayClusterName(),
			Namespace: tCtx.GetNamespaceName(),
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-head",
								Image: tCtx.GetRayImage(),
							},
						},
					},
				},
			},
		},
	}
	_, err = rayClient.RayClusters(tCtx.GetNamespaceName()).Create(tCtx.GetCtx(), rayCluster, metav1.CreateOptions{})
	require.NoError(t, err)

	// Wait for the Ray cluster's head pod to be ready so that we can access the dashboard
	waitForClusterConditions(t, tCtx, tCtx.GetRayClusterName(), []rayv1.RayClusterConditionType{rayv1.HeadPodReady})

	k8sClient := tCtx.GetK8sClient()
	serviceName := tCtx.GetRayClusterName() + "-head-svc"
	r := k8sClient.CoreV1().Services(tCtx.GetNamespaceName()).ProxyGet("http", serviceName, "8265", "", nil)
	resp, err := r.DoRaw(tCtx.GetCtx())
	require.NoError(t, err)
	require.Contains(t, string(resp), "Ray Dashboard")

	err = rayClient.RayClusters(tCtx.GetNamespaceName()).Delete(tCtx.GetCtx(), tCtx.GetRayClusterName(), metav1.DeleteOptions{})
	require.NoError(t, err)
}
