package e2e

import (
	"net"
	"os"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayClusterIPv6(t *testing.T) {
	if os.Getenv("KUBERAY_TEST_IPV6") != "true" {
		t.Skip("set KUBERAY_TEST_IPV6=true to run the IPv6-only cluster test")
	}
	testRayClusterIPv6(t, "raycluster-ipv6", NewRayClusterSpec(), false)
}

func TestRayClusterTLSIPv6(t *testing.T) {
	if os.Getenv("KUBERAY_TEST_IPV6") != "true" {
		t.Skip("set KUBERAY_TEST_IPV6=true to run the IPv6-only mTLS cluster test")
	}

	test := With(t)
	if !certManagerAvailable(test) {
		t.Fatal("cert-manager CRDs are required for the dedicated IPv6 mTLS test")
	}

	testRayClusterIPv6(t, "raycluster-tls-ipv6", NewRayClusterSpecWithMTLS(), true)
}

func testRayClusterIPv6(
	t *testing.T,
	clusterName string,
	spec *rayv1ac.RayClusterSpecApplyConfiguration,
	wantTLSInit bool,
) {
	t.Helper()
	test := With(t)
	g := NewWithT(t)
	namespace := test.NewTestNamespace()

	// Exercise the operator's family-aware default instead of the shared test
	// fixture's explicit IPv4 dashboard address.
	spec.HeadGroupSpec.RayStartParams = map[string]string{}
	rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).WithSpec(spec)

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	workerPods, err := GetWorkerPods(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workerPods).NotTo(BeEmpty())

	pods := append(workerPods, *headPod)
	primaryPodIPs := make([]string, 0, len(pods))
	for _, pod := range pods {
		primaryPodIPs = append(primaryPodIPs, pod.Status.PodIP)
		primaryIP := net.ParseIP(pod.Status.PodIP)
		g.Expect(primaryIP).NotTo(BeNil(), "Pod %s has invalid primary IP %q", pod.Name, pod.Status.PodIP)
		g.Expect(primaryIP.To4()).To(BeNil(), "Pod %s primary IP should be IPv6, got %q", pod.Name, pod.Status.PodIP)
		g.Expect(pod.Status.PodIPs).NotTo(BeEmpty(), "Pod %s should report an IPv6 address", pod.Name)
		for _, podIP := range pod.Status.PodIPs {
			ip := net.ParseIP(podIP.IP)
			g.Expect(ip).NotTo(BeNil(), "Pod %s has invalid IP %q", pod.Name, podIP.IP)
			g.Expect(ip.To4()).To(BeNil(), "Pod %s should be IPv6-only, got %q", pod.Name, podIP.IP)
		}

		if wantTLSInit {
			assertTLSIPSanInitContainerCompleted(g, &pod)
		}
	}

	// Ready Pods prove the local raylet is healthy. These checks additionally
	// exercise the head's localhost GCS and dashboard paths used by sidecars.
	ExecPodCmd(test, headPod, headPod.Spec.Containers[0].Name, []string{"ray", "status"})
	ExecPodCmd(test, headPod, headPod.Spec.Containers[0].Name, []string{
		"python", "-c",
		`import ipaddress, ray, sys
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

ray.init(address="auto")
nodes = [node for node in ray.nodes() if node["Alive"]]
actual = {node["NodeManagerAddress"] for node in nodes}
expected = set(sys.argv[1].split(","))
assert actual == expected, f"Ray node addresses {actual} do not match Pod primary IPs {expected}"
assert all(not ipaddress.ip_address(address).is_loopback for address in actual), actual

# Registration alone is insufficient: a node can appear in ray.nodes() while
# advertising an address that the head cannot use for task or object traffic.
worker = next(node for node in nodes if node["NodeManagerAddress"] != sys.argv[2])

@ray.remote
def worker_object_round_trip():
    return ray.get_runtime_context().get_node_id(), b"x" * (16 * 1024 * 1024)

worker_node_id, payload = ray.get(worker_object_round_trip.options(
    scheduling_strategy=NodeAffinitySchedulingStrategy(
        node_id=worker["NodeID"], soft=False
    )
).remote())
assert worker_node_id == worker["NodeID"], (worker_node_id, worker["NodeID"])
assert len(payload) == 16 * 1024 * 1024 and payload[:1] == b"x"`,
		strings.Join(primaryPodIPs, ","),
		headPod.Status.PodIP,
	})
	ExecPodCmd(test, headPod, headPod.Spec.Containers[0].Name, []string{
		"python", "-c",
		"import urllib.request; urllib.request.urlopen('http://localhost:8265/api/gcs_healthz', timeout=10).read()",
	})
}

func assertTLSIPSanInitContainerCompleted(g Gomega, pod *corev1.Pod) {
	for _, status := range pod.Status.InitContainerStatuses {
		if status.Name != "wait-for-tls-ip-san" {
			continue
		}
		g.Expect(status.State.Terminated).NotTo(BeNil(), "Pod %s TLS IP SAN init container should have terminated", pod.Name)
		g.Expect(status.State.Terminated.ExitCode).To(BeZero(), "Pod %s TLS IP SAN init container should succeed", pod.Name)
		return
	}
	g.Expect(false).To(BeTrue(), "Pod %s should contain wait-for-tls-ip-san status", pod.Name)
}
