package e2erayservice

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestOldHeadPodFailDuringUpgrade(t *testing.T) {
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
	curlContainerName := "curl"

	LogWithTimestamp(test.T(), "Creating curl pod %s/%s", namespace.Name, curlPodName)

	curlPod, err := CreateCurlPod(g, test, curlPodName, curlContainerName, namespace.Name)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Sending requests to the RayService to make sure it is ready to serve requests")
	stdout, _ := CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
	g.Expect(stdout.String()).To(Equal("15 pizzas please!"))

	svcName := utils.GenerateServeServiceName(rayService.Name)
	LogWithTimestamp(test.T(), "Checking that the K8s serve service %s has exactly one endpoint because the cluster only has a head Pod", svcName)
	readyEndpoints, err := GetReadyEndpointsFromSlices(test.Ctx(), test.Client(), namespace.Name, svcName)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(readyEndpoints).To(HaveLen(1))
	headPodName := readyEndpoints[0].TargetRefName
	headPodUID := readyEndpoints[0].TargetRefUID

	LogWithTimestamp(test.T(), "Upgrading the RayService to trigger a zero downtime upgrade")
	rayService, err = GetRayService(test, namespace.Name, rayService.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayService.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.InitContainers = append(
		rayService.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.InitContainers,
		corev1.Container{
			Name:    "sleep",
			Image:   rayService.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].Image,
			Command: []string{"/bin/bash"},
			// Intentionally make the new pending cluster sleep for a while to give us
			// time to capture the iptables manipulation below.
			Args: []string{"-c", "sleep 30"},
		},
	)
	rayService, err = test.Client().Ray().RayV1().RayServices(namespace.Name).Update(test.Ctx(), rayService, metav1.UpdateOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	headPod, err := test.Client().Core().CoreV1().Pods(namespace.Name).Get(test.Ctx(), headPodName, metav1.GetOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod.Status.PodIP).NotTo(BeEmpty())

	LogWithTimestamp(test.T(), "Checking that the old Head Pod's label `ray.io/serve` is `true`")
	g.Expect(headPod.Labels[utils.RayClusterServingServiceLabelKey]).To(Equal("true"))

	for _, iptables := range []string{"iptables-legacy", "iptables-nft"} {
		LogWithTimestamp(test.T(), "Using %s to make the ProxyActor(8000 port) fail to receive requests on the old head Pod", iptables)
		headPodPatch := map[string]any{
			"spec": map[string]any{
				"ephemeralContainers": []corev1.EphemeralContainer{
					{
						EphemeralContainerCommon: corev1.EphemeralContainerCommon{
							Name:    "proxy-actor-drop-" + iptables,
							Image:   "istio/iptables:1.27-2026-02-26T19-02-11",
							Command: []string{iptables},
							Args:    []string{"-A", "INPUT", "-p", "tcp", "--dport", "8000", "-j", "DROP"},
							SecurityContext: &corev1.SecurityContext{
								Privileged: new(true),
								RunAsUser:  new(int64(0)),
							},
						},
					},
				},
			},
		}
		patchBytes, err := json.Marshal(headPodPatch)
		g.Expect(err).NotTo(HaveOccurred())
		_, err = test.Client().Core().CoreV1().Pods(namespace.Name).Patch(test.Ctx(), headPodName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "ephemeralcontainers")
		g.Expect(err).NotTo(HaveOccurred())
	}

	healthURL := fmt.Sprintf("http://%s:%d/", headPod.Status.PodIP, utils.DefaultServingPort) + utils.RayServeProxyHealthPath
	LogWithTimestamp(test.T(), "Curling head Pod %s at %s until ephemeral iptables applies DROP on :%d (expect curl to fail)", headPodName, healthURL, utils.DefaultServingPort)
	curlCmd := []string{
		"curl", "-sS", "-f", "--connect-timeout", "3", "--max-time", "5", "-X", "GET", healthURL,
	}
	g.Eventually(func() error {
		_, _, curlErr := ExecPodCmdWithError(test, curlPod, curlContainerName, curlCmd)
		return curlErr
	}, TestTimeoutShort, 2*time.Second).Should(HaveOccurred(), "iptables should block TCP :8000 like CheckProxyActorHealth would fail")

	LogWithTimestamp(test.T(), "Checking that the old Head Pod's label `ray.io/serve` is `false` because it is not healthy")
	g.Eventually(func(g Gomega) string {
		headPod, err := test.Client().Core().CoreV1().Pods(namespace.Name).Get(test.Ctx(), headPodName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		return headPod.Labels[utils.RayClusterServingServiceLabelKey]
	}, TestTimeoutShort).Should(Equal("false"))

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s UpgradeInProgress condition to be true", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeTrue()))
	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s Ready condition to be false because there is no endpoint", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceReady, BeFalse()))

	LogWithTimestamp(test.T(), "Checking that the K8s serve service eventually has 1 endpoint and the endpoint is not the old head Pod")
	g.Eventually(func(g Gomega) {
		readyEndpoints, err := GetReadyEndpointsFromSlices(test.Ctx(), test.Client(), namespace.Name, svcName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(readyEndpoints).To(HaveLen(1))
		g.Expect(readyEndpoints[0].TargetRefName).NotTo(Equal(headPodName))
		g.Expect(readyEndpoints[0].TargetRefUID).NotTo(Equal(headPodUID))
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s UpgradeInProgress condition to be false", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceUpgrading, BeFalse()))
	LogWithTimestamp(test.T(), "Waiting for RayService %s/%s Ready condition to be true because there is an endpoint", rayService.Namespace, rayService.Name)
	g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutShort).Should(WithTransform(IsRayServiceReady, BeTrue()))

	LogWithTimestamp(test.T(), "Sending requests to the RayService to make sure it is ready to serve requests")
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/fruit", `["MANGO", 2]`)
	g.Expect(stdout.String()).To(Equal("6"))
	stdout, _ = CurlRayServicePod(test, rayService, curlPod, curlContainerName, "/calc", `["MUL", 3]`)
	g.Expect(stdout.String()).To(Equal("15 pizzas please!"))
}
