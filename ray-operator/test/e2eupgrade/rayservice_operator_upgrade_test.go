package e2eupgrade

import (
	"fmt"
	"os"
	"os/exec"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestZeroDowntimeUpgradeAfterOperatorUpgrade(t *testing.T) {
	// buildkite e2eupgrade installs the previous supported release of KubeRay.
	// This test installs a RayService, checks the connectivity of the RayService,
	// upgrades the Ray operator to the latest release, initiates a zero-downtime upgrade of
	// the RayService, while ensuring throughout the RayService can handle requests.

	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-sample"

	// Get the upgrade version from environment
	upgradeVersion := GetKubeRayUpgradeVersion()

	test.T().Logf("Detected upgrade version: %s", upgradeVersion)

	// Create RayService custom resource
	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).WithSpec(rayServiceSampleYamlApplyConfigurationWithWorker())
	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService).NotTo(BeNil())

	// Check RayService using deprecated ServiceStatus from previous release
	test.T().Logf("Waiting for RayService %s/%s to be running", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(RayServiceStatus, Equal(rayv1.Running)))

	// Get the latest RayService
	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService).NotTo(BeNil())

	// Create a curl Pod to query RayService
	curlPodName := "curl-pod"
	curlContainerName := "curl-container"

	test.T().Logf("Creating curl pod %s/%s", namespace.Name, curlPodName)
	curlPod, err := CreateCurlPod(g, test, curlPodName, curlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())

	// Validate RayService is able to serve requests
	test.T().Logf("Sending requests to the RayService to make sure it is ready to serve requests")
	g.Expect(requestRayService(test, rayService, curlPod, curlContainerName)).To(Equal("6, 15 pizzas please!"))

	// Validate RayService serve service correctly configured
	svcName := utils.GenerateServeServiceName(rayService.Name)
	test.T().Logf("Checking that the K8s serve service %s has two ready endpoints", svcName)
	g.Eventually(func() int {
		readyEndpoints, err := GetReadyEndpointsFromSlices(test.Ctx(), test.Client(), namespace.Name, svcName)
		g.Expect(err).NotTo(HaveOccurred())
		return len(readyEndpoints)
	}, TestTimeoutShort).Should(Equal(2))

	// Upgrade KubeRay operator to latest version and replace CRDs
	test.T().Logf("Upgrading the KubeRay operator to %s", upgradeVersion)

	useLocalHelmChart := false

	if val, ok := os.LookupEnv("USE_LOCAL_HELM_CHART"); ok && val == "true" {
		useLocalHelmChart = true
	}

	if useLocalHelmChart {
		cmd := exec.CommandContext(test.Ctx(), "kubectl", "replace", "-k", "../../config/crd")
		output, err := cmd.CombinedOutput()
		if err != nil {
			test.T().Logf("Error upgrading KubeRay operator: %v\nCommand output:\n%s", err, string(output))
			t.Fatal("Failed to upgrade KubeRay operator")
		}
		g.Eventually(cmd, TestTimeoutShort).Should(WithTransform(ProcessStateSuccess, BeTrue()))

		t.Logf("Upgrading operator deployment using local helm image version: %s", upgradeVersion)
		cmd = exec.CommandContext(test.Ctx(), "helm", "upgrade", "kuberay-operator", "../../../helm-chart/kuberay-operator", "--set", fmt.Sprintf("image.repository=kuberay/kuberay-operator,image.tag=%s", upgradeVersion)) //nolint:gosec // required for upgrade
		err = cmd.Run()
		g.Expect(err).NotTo(HaveOccurred())
		g.Eventually(cmd, TestTimeoutShort).Should(WithTransform(ProcessStateSuccess, BeTrue()))
	} else {
		cmd := exec.CommandContext(test.Ctx(), "kubectl", "replace", "-k", fmt.Sprintf("github.com/ray-project/kuberay/ray-operator/config/crd?ref=%s", upgradeVersion)) //nolint:gosec // required for upgrade
		err = cmd.Run()
		g.Expect(err).NotTo(HaveOccurred())
		g.Eventually(cmd, TestTimeoutShort).Should(WithTransform(ProcessStateSuccess, BeTrue()))
		cmd = exec.CommandContext(test.Ctx(), "helm", "upgrade", "kuberay-operator", "kuberay/kuberay-operator", "--version", upgradeVersion)
		err = cmd.Run()
		g.Expect(err).NotTo(HaveOccurred())
		g.Eventually(cmd, TestTimeoutShort).Should(WithTransform(ProcessStateSuccess, BeTrue()))
	}

	// Validate RayService is able to serve requests during the upgrade
	test.T().Logf("Sending requests to the RayService to make sure it is ready to serve requests")
	g.Consistently(requestRayService, "30s", "1s").WithArguments(test, rayService, curlPod, curlContainerName).Should(Equal("6, 15 pizzas please!"))

	// Trigger a zero-downtime upgrade of the RayService
	test.T().Logf("Upgrading the RayService to trigger a zero downtime upgrade")
	rayService, err = GetRayService(test, namespace.Name, rayService.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Env = append(
		rayService.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Env,
		corev1.EnvVar{
			Name:  "RAY_SERVE_PROXY_MIN_DRAINING_PERIOD_S",
			Value: "30",
		},
	)
	rayService, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	// Validate RayService performs upgrade and is reachable
	test.T().Logf("Waiting for RayService %s/%s UpgradeInProgress condition to be true", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeTrue()))

	test.T().Logf("Waiting for RayService %s/%s UpgradeInProgress condition to be false", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeFalse()))
	test.T().Logf("Waiting for RayService %s/%s Ready condition to be true because there is an endpoint", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceReady, BeTrue()))

	test.T().Logf("Sending requests to the RayService to make sure it is ready to serve requests")
	g.Expect(requestRayService(test, rayService, curlPod, curlContainerName)).To(Equal("6, 15 pizzas please!"))
}
