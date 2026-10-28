package e2eincrementalupgrade

import (
	"fmt"
	"net/http"
	"testing"

	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayServiceSuspendDuringIncrementalUpgrade(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	rayServiceName := "suspend-incremental-rayservice"

	stepSize := new(int32(25))
	interval := new(int32(20))
	maxSurge := new(int32(30))

	rayService, httpRoute, gatewayHost := bootstrapIncrementalRayService(
		test, g, namespace.Name, rayServiceName, stepSize, interval, maxSurge, defaultIncrementalUpgradeServeConfigV2)
	gatewayName := fmt.Sprintf("%s-gateway", rayServiceName)

	// Sanity-check that the service is wired up through the Gateway before
	// touching Spec.Suspend.
	curlPod, err := CreateCurlPod(g, test, CurlPodName, CurlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())
	stdout, _ := CurlRayServiceGateway(test, gatewayHost, curlPod, CurlContainerName, http.MethodPost, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))

	// Trigger an incremental upgrade so both active and pending RayClusters
	// exist when Spec.Suspend kicks in.
	LogWithTimestamp(test.T(), "Triggering incremental upgrade so both active and pending clusters exist before suspend")
	g.Expect(triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, defaultIncrementalUpgradeServeConfigV2,
		withWorkerCPURequest("500m"), withUpgradedServeConfig())).To(Succeed())
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceUpgrading, BeTrue()))

	LogWithTimestamp(test.T(), "Waiting until active and pending RayClusters coexist before flipping Spec.Suspend=true")
	g.Eventually(func(gg Gomega) {
		rs, err := GetRayService(test, namespace.Name, rayServiceName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(rs.Status.ActiveServiceStatus.RayClusterName).NotTo(BeEmpty(), "active cluster name should be set")
		gg.Expect(rs.Status.PendingServiceStatus.RayClusterName).NotTo(BeEmpty(), "pending cluster name should be set")

		rcList, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(rcList.Items).To(HaveLen(2), "both active and pending RayClusters should exist concurrently before suspend")
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Setting Spec.Suspend=true while incremental upgrade is in progress")
	g.Expect(triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, defaultIncrementalUpgradeServeConfigV2,
		withWorkerCPURequest("500m"), withUpgradedServeConfig(), withSuspend(true))).To(Succeed())

	LogWithTimestamp(test.T(), "Waiting for the Suspended condition to be True")
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceSuspended, BeTrue()))

	LogWithTimestamp(test.T(), "Gateway and HTTPRoute must be deleted alongside RayClusters and Services")
	g.Eventually(func(gg Gomega) {
		_, err := GetGateway(test, namespace.Name, gatewayName)
		gg.Expect(errors.IsNotFound(err)).To(BeTrue(), "Gateway %s should be deleted; got err=%v", gatewayName, err)

		_, err = GetHTTPRoute(test, namespace.Name, httpRoute.Name)
		gg.Expect(errors.IsNotFound(err)).To(BeTrue(), "HTTPRoute %s should be deleted; got err=%v", httpRoute.Name, err)

		rcList, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(rcList.Items).To(BeEmpty(), "All RayClusters owned by the RayService should be deleted")

		svcList, err := test.Client().Core().CoreV1().Services(namespace.Name).List(test.Ctx(), metav1.ListOptions{
			LabelSelector: utils.RayOriginatedFromCRNameLabelKey + "=" + rayServiceName,
		})
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(svcList.Items).To(BeEmpty(), "All Services owned by the RayService should be deleted")
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Setting Spec.Suspend=false; the controller must recreate Gateway, HTTPRoute, RayCluster, and Services")
	g.Expect(triggerIncrementalUpgrade(test, namespace.Name, rayServiceName, stepSize, interval, maxSurge, defaultIncrementalUpgradeServeConfigV2,
		withWorkerCPURequest("500m"), withUpgradedServeConfig(), withSuspend(false))).To(Succeed())

	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
		Should(WithTransform(IsRayServiceReady, BeTrue()))
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).
		Should(WithTransform(IsRayServiceSuspended, BeFalse()))

	// Gateway and HTTPRoute must come back, and the resumed Gateway must
	// serve traffic again so we know the network path is fully restored
	// rather than just the K8s objects re-existing.
	setGatewayServiceType(test, g, namespace.Name, gatewayName)
	waitForGatewayReady(test, g, namespace.Name, gatewayName)

	LogWithTimestamp(test.T(), "Waiting for HTTPRoute %s/%s to be ready again", namespace.Name, httpRoute.Name)
	g.Eventually(func() (bool, error) {
		gw, err := GetGateway(test, namespace.Name, gatewayName)
		if err != nil {
			return false, err
		}
		route, err := GetHTTPRoute(test, namespace.Name, httpRoute.Name)
		if err != nil {
			return false, err
		}
		return utils.IsHTTPRouteReady(gw, route), nil
	}, TestTimeoutMedium).Should(BeTrue())

	// Resume runs against the upgraded spec — incrementalUpgrade() bumped
	// MangoStand price 3→4 via the API before Spec.Suspend was flipped, so
	// (MANGO, 2) -> 8, not 6. This also doubles as a check that resume
	// applied the upgraded spec rather than reviving the pre-upgrade state.
	LogWithTimestamp(test.T(), "Verifying the resumed RayService serves traffic through the recreated Gateway with the upgraded spec")
	curlCmd := []string{
		"curl", "-sS", "--connect-timeout", "3", "--max-time", "5",
		"-X", "POST",
		"-H", "Connection: close",
		"-H", "Content-Type: application/json",
		fmt.Sprintf("http://%s/fruit", gatewayHost),
		"-d", `["MANGO", 2]`,
	}
	g.Eventually(func(gg Gomega) {
		stdout, _, err := ExecPodCmdWithError(test, curlPod, CurlContainerName, curlCmd)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(stdout.String()).To(Equal("8"))
	}, TestTimeoutMedium).Should(Succeed())
}
