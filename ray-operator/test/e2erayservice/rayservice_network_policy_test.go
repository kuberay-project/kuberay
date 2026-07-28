package e2erayservice

import (
	"testing"

	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// RayService under NetworkPolicy DenyAll. Checks the service comes up and, more
// importantly, that we can actually hit the Serve app from an external pod. Under
// DenyAll the operator only allows intra-cluster traffic, so we add DNS egress
// (FQDN resolution), HTTPS egress (the sample pulls its working_dir from GitHub),
// Serve-port ingress (so the curl pod can reach 8000), and operator ingress on
// the dashboard (8265, push Serve config + poll status) and serving (8000, proxy
// health probe) ports, since the reconciler dials the head directly on both and
// without them the service never goes Ready. Kindnet enforces NetworkPolicy on
// k8s >= 1.32 (CI runs on 1.35), so a wrong rule means the service never starts
// or the curl gets blocked.
func TestRayServiceWithNetworkPolicy(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	rayServiceName := "rayservice-network-policy"

	networkPolicy := rayv1ac.NetworkPolicyConfig().
		WithMode(rayv1.NetworkPolicyDenyAll).
		WithHead(rayv1ac.NetworkPolicyRules().
			WithIngressRules(
				ServeIngressRule(utils.DefaultServingPort),
				// The reconciler hits the head directly on both the dashboard (push
				// Serve config / poll status) and the serving port (proxy health probe).
				OperatorIngressRule(utils.DefaultDashboardPort, utils.DefaultServingPort),
			).
			WithEgressRules(DNSEgressRule(), AllHostsHTTPSEgressRule())).
		WithWorker(rayv1ac.NetworkPolicyRules().
			WithIngressRules(ServeIngressRule(utils.DefaultServingPort)).
			WithEgressRules(DNSEgressRule(), AllHostsHTTPSEgressRule()))

	rayServiceSpec := RayServiceSampleYamlApplyConfiguration()
	rayServiceSpec.RayClusterSpec.WithNetworkPolicy(networkPolicy)

	rayServiceAC := rayv1ac.RayService(rayServiceName, namespace.Name).WithSpec(rayServiceSpec)

	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayService).NotTo(BeNil())
	LogWithTimestamp(test.T(), "Created RayService %s/%s successfully with NetworkPolicy DenyAll", rayService.Namespace, rayService.Name)

	defer func() {
		err := test.Client().Ray().RayV1().RayServices(namespace.Name).Delete(test.Ctx(), rayService.Name, metav1.DeleteOptions{})
		if err != nil {
			LogWithTimestamp(test.T(), "WARNING: Failed to delete RayService %s: %v", rayService.Name, err)
		}
	}()

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be ready", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutLong).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	rayService, err = GetRayService(test, namespace.Name, rayServiceName)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "RayService %s/%s is ready", rayService.Namespace, rayService.Name)

	rayClusterName := rayService.Status.ActiveServiceStatus.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty(), "RayCluster name should be populated in status")

	// Sanity check that both policies landed.
	g.Eventually(func(gg Gomega) {
		_, err := test.Client().Core().NetworkingV1().NetworkPolicies(namespace.Name).
			Get(test.Ctx(), rayClusterName+"-head", metav1.GetOptions{})
		gg.Expect(err).NotTo(HaveOccurred())
		_, err = test.Client().Core().NetworkingV1().NetworkPolicies(namespace.Name).
			Get(test.Ctx(), rayClusterName+"-workers", metav1.GetOptions{})
		gg.Expect(err).NotTo(HaveOccurred())
	}, TestTimeoutShort).Should(Succeed())
	LogWithTimestamp(test.T(), "Head and worker NetworkPolicies created for RayCluster %s/%s", namespace.Name, rayClusterName)

	// Spin up a curl pod outside the cluster and confirm it can actually reach
	// the Serve app through the ingress rule.
	curlPodName := "curl-pod"
	curlContainerName := "curl-container"
	LogWithTimestamp(test.T(), "Creating curl pod %s/%s", namespace.Name, curlPodName)
	curlPod, err := CreateCurlPod(g, test, curlPodName, curlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Sending requests to the RayService Serve application")
	g.Eventually(func(gg Gomega) {
		stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
		gg.Expect(stdout.String()).To(Equal("6"))
	}, TestTimeoutMedium).Should(Succeed())

	stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
	g.Expect(stdout.String()).To(Equal("15 pizzas please!"))

	LogWithTimestamp(test.T(), "RayService %s/%s served requests successfully under NetworkPolicy DenyAll", rayService.Namespace, rayService.Name)
}
