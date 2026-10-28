package e2erayservice

import (
	"net"
	"os"
	"testing"

	. "github.com/onsi/gomega"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const ipv6ServeApp = `from ray import serve

@serve.deployment
class IPv6App:
    async def __call__(self, request):
        return "ipv6-ok"

app = IPv6App.bind()
`

func TestRayServiceIPv6(t *testing.T) {
	if os.Getenv("KUBERAY_TEST_IPV6") != "true" {
		t.Skip("set KUBERAY_TEST_IPV6=true to run the IPv6-only RayService test")
	}

	test := With(t)
	g := NewWithT(t)
	namespace := test.NewTestNamespace()

	appConfigMapAC := corev1ac.ConfigMap("ipv6-serve-app", namespace.Name).
		WithData(map[string]string{"ipv6_app.py": ipv6ServeApp})
	appConfigMap, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).
		Apply(test.Ctx(), appConfigMapAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	rayClusterSpec := NewRayClusterSpec()
	// Exercise KubeRay's family-aware dashboard default instead of the shared
	// fixture's explicit IPv4 wildcard.
	rayClusterSpec.HeadGroupSpec.RayStartParams = map[string]string{}
	// Keep the minimal Serve application on the head Pod, where its ConfigMap is
	// mounted, and avoid any runtime_env download from the public internet.
	rayClusterSpec.WorkerGroupSpecs = nil
	headPodSpec := rayClusterSpec.HeadGroupSpec.Template.Spec
	headPodSpec.Containers[0].WithEnv(corev1ac.EnvVar().WithName("PYTHONPATH").WithValue("/home/ray/ipv6-app"))
	headPodSpec.Containers[0].WithVolumeMounts(corev1ac.VolumeMount().
		WithName("ipv6-serve-app").
		WithMountPath("/home/ray/ipv6-app").
		WithReadOnly(true))
	headPodSpec.WithVolumes(corev1ac.Volume().
		WithName("ipv6-serve-app").
		WithConfigMap(corev1ac.ConfigMapVolumeSource().WithName(appConfigMap.Name)))

	spec := rayv1ac.RayServiceSpec().
		WithServeConfigV2(`http_options:
  host: "::"
applications:
  - name: ipv6-app
    import_path: ipv6_app.app
    route_prefix: /
`).
		WithRayClusterSpec(rayClusterSpec)
	rayServiceAC := rayv1ac.RayService("rayservice-ipv6", namespace.Name).WithSpec(spec)

	rayService, err := test.Client().Ray().RayV1().RayServices(namespace.Name).Apply(test.Ctx(), rayServiceAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutLong).
		Should(WithTransform(IsRayServiceReady, BeTrue()))

	rayService, err = GetRayService(test, namespace.Name, rayService.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayCluster, err := GetRayCluster(test, namespace.Name, rayService.Status.ActiveServiceStatus.RayClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	ip := net.ParseIP(headPod.Status.PodIP)
	g.Expect(ip).NotTo(BeNil(), "head Pod should have a valid IP")
	g.Expect(ip.To4()).To(BeNil(), "head Pod should use IPv6, got %q", headPod.Status.PodIP)

	curlPod, err := CreateCurlPod(g, test, "curl-ipv6", "curl", namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())
	stdout, _ := CurlRayServicePod(test, rayService, curlPod, "curl", "/", `{}`)
	g.Expect(stdout.String()).To(Equal("ipv6-ok"))
}
