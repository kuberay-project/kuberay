package support

import (
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"os/exec"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"

	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// CreateHTTPClientWithCookieJar creates an HTTP client with a cookie jar to maintain
// session. The client authenticates to the Kubernetes API server, since requests to
// in-cluster services are routed through the API server's service proxy.
func CreateHTTPClientWithCookieJar(test Test, g *WithT) *http.Client {
	cfg := test.Client().Config()
	client, err := rest.HTTPClientFor(&cfg)
	g.Expect(err).NotTo(HaveOccurred())
	client.Timeout = 30 * time.Second

	jar, err := cookiejar.New(nil)
	g.Expect(err).NotTo(HaveOccurred())
	client.Jar = jar
	return client
}

// GetContainerStatusByName retrieves the container status by container name.
// NOTE: ContainerStatuses order doesn't guarantee to match Spec.Containers order.
// For more details, please refer to the following link:
// https://github.com/ray-project/kuberay/blob/7791a8786861818f0cebcce381ef221436a0fa4d/ray-operator/controllers/ray/raycluster_controller.go#L1160C1-L1171C2
func GetContainerStatusByName(pod *corev1.Pod, containerName string) (*corev1.ContainerStatus, error) {
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == containerName {
			return &containerStatus, nil
		}
	}
	return nil, fmt.Errorf("container %s not found in pod %s/%s", containerName, pod.Namespace, pod.Name)
}

// PortForwardService forwards a service port to localhost via kubectl.
//
// TODO: port-forwarding is flaky in CI (see #5302) — the forward can die
// silently and nothing restarts it. Only the Azurite tests still use this;
// they should migrate to in-cluster access like the RustFS and history server
// tests, after which this helper should be deleted.
func PortForwardService(test Test, g *WithT, namespace, serviceName string, port int) {
	kubectlCmd := exec.Command(
		"kubectl",
		"-n", namespace,
		"port-forward",
		fmt.Sprintf("svc/%s", serviceName),
		fmt.Sprintf("%d:%d", port, port),
	)
	err := kubectlCmd.Start()
	g.Expect(err).NotTo(HaveOccurred())

	// Kill and reap on cleanup: a leaked forward keeps the port, so the next test's
	// forward cannot bind and silently talks to this test's deleted namespace.
	test.T().Cleanup(func() {
		_ = kubectlCmd.Process.Kill()
		_ = kubectlCmd.Wait()
	})
}

// InstallGrafanaAndPrometheus installs Grafana and Prometheus in the cluster for testing.
func InstallGrafanaAndPrometheus(test Test, g *WithT) {
	test.T().Cleanup(func() {
		cleanCMD := exec.Command("kubectl", "delete", "ns", "prometheus-system")
		output, err := cleanCMD.CombinedOutput()
		if err != nil {
			LogWithTimestamp(test.T(), "Failed to clean up Prometheus and Grafana installation with %v because %s",
				err, string(output))
			return
		}
		LogWithTimestamp(test.T(), "Clean up Prometheus and Grafana installation.")
	})

	installGrafanaCMD := exec.CommandContext(
		test.Ctx(),
		"../../../install/prometheus/install.sh",
		"--auto-load-dashboard",
		"true",
	)
	output, err := installGrafanaCMD.CombinedOutput()
	g.Expect(err).NotTo(HaveOccurred(), "fail to install because %s", string(output))
	LogWithTimestamp(test.T(), "Install Grafana and Prometheus successfully.")
}
