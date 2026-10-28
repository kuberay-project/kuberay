package e2erayservice

import (
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayServiceInPlaceUpdate(t *testing.T) {
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

	// Create curl pod
	curlPodName := "curl-pod"
	curlContainerName := "curl-container"

	curlPod, err := CreateCurlPod(g, test, curlPodName, curlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Sending requests to the RayService to make sure it is ready to serve requests")
	stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
	g.Expect(stdout.String()).To(Equal("15 pizzas please!"))

	// In-place update
	// Parse ServeConfigV2 and replace the string in the simplest way to update it.
	rayService, err = GetRayService(test, namespace.Name, rayService.Name)
	g.Expect(err).NotTo(HaveOccurred())

	serveConfig := rayService.Spec.ServeConfigV2
	serveConfig = strings.ReplaceAll(serveConfig, "price: 3", "price: 4")
	serveConfig = strings.ReplaceAll(serveConfig, "factor: 5", "factor: 3")

	rayService.Spec.ServeConfigV2 = serveConfig
	rayService, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(
		test.Ctx(),
		rayService,
		metav1.UpdateOptions{},
	)
	g.Expect(err).NotTo(HaveOccurred())

	// Test the new price and factor
	g.Eventually(func(g Gomega) {
		// curl /fruit
		stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
		g.Expect(stdout.String()).To(Equal("8"))
		// curl /calc
		stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
		g.Expect(stdout.String()).To(Equal("9 pizzas please!"))
	}, TestTimeoutShort).Should(Succeed())
}

func TestUpdateServeConfigAndRayClusterSpec(t *testing.T) {
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

	// Create curl pod
	curlPodName := "curl-pod"
	curlContainerName := "curl-container"

	curlPod, err := CreateCurlPod(g, test, curlPodName, curlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Sending requests to the RayService to make sure it is ready to serve requests")
	stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
	g.Expect(stdout.String()).To(Equal("15 pizzas please!"))

	// Update both `serveConfigV2` and the Ray head's environment variables.
	// Then, verify the following two conditions:
	// (1) The new `serveConfigV2` is not applied to the old RayCluster.
	// (2) A zero-downtime upgrade is triggered.
	rayService, err = GetRayService(test, namespace.Name, rayService.Name)
	g.Expect(err).NotTo(HaveOccurred())

	serveConfig := rayService.Spec.ServeConfigV2
	serveConfig = strings.ReplaceAll(serveConfig, "price: 3", "price: 456")
	rayService.Spec.ServeConfigV2 = serveConfig

	rayService.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "FOO", Value: "BAR"}}

	var oldRayCluster *rayv1.RayCluster
	oldRayCluster, err = GetRayCluster(test, namespace.Name, rayService.Status.ActiveServiceStatus.RayClusterName)
	g.Expect(err).NotTo(HaveOccurred())

	// Apply a finalizer to the active RayCluster to prevent it from being deleted after the zero-downtime upgrade process is complete.
	oldRayCluster.Finalizers = append(oldRayCluster.Finalizers, "no-existed-finalizer")
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Update(
		test.Ctx(),
		oldRayCluster,
		metav1.UpdateOptions{},
	)
	g.Expect(err).NotTo(HaveOccurred())

	rayService, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(
		test.Ctx(),
		rayService,
		metav1.UpdateOptions{},
	)
	g.Expect(err).NotTo(HaveOccurred())

	waitingForRayClusterSwitch(g, test, rayService, oldRayCluster.Name)

	// Make sure the serveConfig is updated to new ray cluster.
	g.Eventually(func(g Gomega) {
		// curl /fruit
		stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
		g.Expect(stdout.String()).To(Equal("912"))
	}, TestTimeoutShort).Should(Succeed())

	// Make sure the old ray cluster is not updated.
	stdout, _ = curlHeadPodWithRayServicePath(test, oldRayCluster, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))

	// fresh old RayCluster
	oldRayCluster, err = GetRayCluster(test, oldRayCluster.Namespace, oldRayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())

	// remove the non-existed-finalizer
	oldRayCluster.Finalizers = nil
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Update(
		test.Ctx(),
		oldRayCluster,
		metav1.UpdateOptions{},
	)
	g.Expect(err).NotTo(HaveOccurred())
}

func TestUpdateServeConfigAndRayClusterSpecWithUpgradeDisabled(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-sample"

	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).WithSpec(RayServiceSampleYamlApplyConfiguration().
		WithUpgradeStrategy(rayv1ac.RayServiceUpgradeStrategy().WithType(rayv1.RayServiceUpgradeNone)))

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

	// Create curl pod
	curlPodName := "curl-pod"
	curlContainerName := "curl-container"

	curlPod, err := CreateCurlPod(g, test, curlPodName, curlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Sending requests to the RayService to make sure it is ready to serve requests")
	stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
	g.Expect(stdout.String()).To(Equal("15 pizzas please!"))

	// Update both `serveConfigV2` and the Ray head's environment variables.
	// Since `upgradeStrategy` is set to `None`, a zero-downtime upgrade will not be triggered,
	// and the new `serveConfigV2` will be applied to the existing RayCluster.
	rayService, err = GetRayService(test, namespace.Name, rayService.Name)
	g.Expect(err).NotTo(HaveOccurred())

	serveConfig := rayService.Spec.ServeConfigV2
	serveConfig = strings.ReplaceAll(serveConfig, "price: 3", "price: 456")
	rayService.Spec.ServeConfigV2 = serveConfig

	rayService.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "FOO", Value: "BAR"}}

	var oldRayCluster *rayv1.RayCluster
	oldRayCluster, err = GetRayCluster(test, namespace.Name, rayService.Status.ActiveServiceStatus.RayClusterName)
	g.Expect(err).NotTo(HaveOccurred())

	rayService, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(
		test.Ctx(),
		rayService,
		metav1.UpdateOptions{},
	)
	g.Expect(err).NotTo(HaveOccurred())

	// Test the new price and factor
	g.Eventually(func(g Gomega) {
		// curl /fruit
		stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
		g.Expect(stdout.String()).To(Equal("912"))
		// curl /calc
		stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
		g.Expect(stdout.String()).To(Equal("15 pizzas please!"))
	}, TestTimeoutShort).Should(Succeed())

	// RayCluster is not changed
	activeRayClusterAfterUpdate, err2 := GetRayCluster(test, namespace.Name, oldRayCluster.Name)
	g.Expect(err2).NotTo(HaveOccurred())
	g.Expect(activeRayClusterAfterUpdate).To(Equal(oldRayCluster))
}
