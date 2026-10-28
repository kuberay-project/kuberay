package e2erayservice

import (
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRedeployRayServe(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-sample"

	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).WithSpec(RayServiceSampleYamlApplyConfiguration())

	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService).NotTo(BeNil())

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be ready", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	// Get the latest RayService
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService).NotTo(BeNil())

	curlPodName := "curl-pod"
	curlContainerName := "curl-container"

	LogWithTimestamp(test.T(), "Creating curl pod %s/%s", namespace.Name, curlPodName)

	curlPod, err := CreateCurlPod(g, test, curlPodName, curlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Sending requests to the RayService to make sure it is ready to serve requests")
	stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
	g.Expect(stdout.String()).To(Equal("15 pizzas please!"))

	LogWithTimestamp(test.T(), "Deleting the current Head for recreating a new one")
	rayServiceUnderlyingRayCluster, err := GetRayCluster(test, namespace.Name, rayService.Status.ActiveServiceStatus.RayClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	oldHeadPod, err := GetHeadPod(test, rayServiceUnderlyingRayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	err = test.Client().Core().CoreV1().Pods(namespace.Name).Delete(test.Ctx(), oldHeadPod.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Checking that the K8s serve service eventually has 1 endpoint and the endpoint is not the old head Pod")
	g.Eventually(func(g Gomega) {
		svcName := utils.GenerateServeServiceName(rayService.Name)
		readyEndpoints, err := GetReadyEndpointsFromSlices(test.Ctx(), test.Client(), namespace.Name, svcName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(readyEndpoints).To(HaveLen(1))
		g.Expect(readyEndpoints[0].TargetRefUID).NotTo(Equal(oldHeadPod.UID))
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be ready", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	LogWithTimestamp(test.T(), "Sending requests to the RayService to make sure it is ready to serve requests")
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
	g.Expect(stdout.String()).To(Equal("15 pizzas please!"))
}
