package apiserversdk

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestCreateCluster(t *testing.T) {
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
							{Name: "test", Image: "test"},
						},
					},
				},
			},
		},
	}
	_, err = rayClient.RayClusters(tCtx.GetNamespaceName()).Create(tCtx.GetCtx(), rayCluster, metav1.CreateOptions{})
	require.NoError(t, err)

	actualCluster, err := rayClient.RayClusters(tCtx.GetNamespaceName()).Get(tCtx.GetCtx(), tCtx.GetRayClusterName(), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, tCtx.GetRayClusterName(), actualCluster.Name)
	require.Equal(t, rayCluster.Spec, actualCluster.Spec)

	err = rayClient.RayClusters(tCtx.GetNamespaceName()).Delete(tCtx.GetCtx(), tCtx.GetRayClusterName(), metav1.DeleteOptions{})
	require.NoError(t, err)
}
