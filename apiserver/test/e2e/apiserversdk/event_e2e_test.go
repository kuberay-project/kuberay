package apiserversdk

import (
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestGetRayClusterEvent(t *testing.T) {
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

	k8sClient := tCtx.GetK8sClient()
	g := gomega.NewWithT(t)
	g.Eventually(func() bool {
		events, err := k8sClient.CoreV1().Events(tCtx.GetNamespaceName()).List(tCtx.GetCtx(), metav1.ListOptions{})
		if err != nil {
			return false
		}

		for _, e := range events.Items {
			if e.InvolvedObject.Name == tCtx.GetRayClusterName() && e.InvolvedObject.Kind == "RayCluster" {
				return true
			}
		}
		return false
	}, 10*time.Second, testPollingInterval).Should(gomega.BeTrue(), "Expected to see RayCluster event in the list of events")

	err = rayClient.RayClusters(tCtx.GetNamespaceName()).Delete(tCtx.GetCtx(), tCtx.GetRayClusterName(), metav1.DeleteOptions{})
	require.NoError(t, err)
}
