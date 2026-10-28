package support

import (
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// Self-contained; the generated cluster name is only known from the RayService status.
const rayServiceManifestPath = "../testdata/rayservice.yaml"

// ApplyRayServiceAndWaitForRunning applies the RayService and waits until it is Running.
func ApplyRayServiceAndWaitForRunning(test Test, g *WithT, namespace *corev1.Namespace) *rayv1.RayService {
	rayServiceFromYaml := DeserializeRayServiceYAML(test, rayServiceManifestPath)
	rayServiceFromYaml.Namespace = namespace.Name

	rayService, err := test.Client().Ray().RayV1().
		RayServices(namespace.Name).
		Create(test.Ctx(), rayServiceFromYaml, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayService %s/%s successfully", rayService.Namespace, rayService.Name)

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be running", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(RayServiceStatus, Equal(rayv1.Running)))

	rayService, err = GetRayService(test, rayService.Namespace, rayService.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService.Status.ActiveServiceStatus.RayClusterName).NotTo(BeEmpty())
	LogWithTimestamp(test.T(), "RayService %s/%s is running on cluster %s",
		rayService.Namespace, rayService.Name, rayService.Status.ActiveServiceStatus.RayClusterName)

	return rayService
}

// DeleteRayServiceAndWait deletes a RayService and waits until its RayCluster is gone.
func DeleteRayServiceAndWait(test Test, g *WithT, namespace, name, clusterName string) {
	err := test.Client().Ray().RayV1().RayServices(namespace).Delete(test.Ctx(), name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Deleted RayService %s/%s", namespace, name)

	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace, clusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "RayCluster %s/%s fully deleted", namespace, clusterName)
}
