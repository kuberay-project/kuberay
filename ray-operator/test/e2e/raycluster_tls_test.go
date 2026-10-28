package e2e

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// certManagerAvailable returns true if cert-manager CRDs are installed in the cluster.
// Tests that require cert-manager call t.Skip() when this returns false.
func certManagerAvailable(test Test) bool {
	_, resources, err := test.Client().Core().Discovery().ServerGroupsAndResources()
	if err != nil {
		return false
	}
	for _, r := range resources {
		if r.GroupVersion == "cert-manager.io/v1" {
			return true
		}
	}
	return false
}

// NewRayClusterSpecWithMTLS creates a RayClusterSpec with mTLS enabled (auto-generated certs).
func NewRayClusterSpecWithMTLS() *rayv1ac.RayClusterSpecApplyConfiguration {
	return NewRayClusterSpec().
		WithTLSOptions(rayv1ac.TLSOptions().WithEnabled(true))
}

// verifyContainerTLSEnvVars asserts a container has the expected TLS environment variables.
func verifyContainerTLSEnvVars(g Gomega, container *corev1.Container) {
	envMap := make(map[string]string)
	for _, env := range container.Env {
		envMap[env.Name] = env.Value
	}

	g.Expect(envMap).To(HaveKeyWithValue(utils.RAY_USE_TLS, "1"),
		"container %s should have RAY_USE_TLS=1", container.Name)
	g.Expect(envMap).To(HaveKeyWithValue(utils.RAY_TLS_SERVER_CERT, utils.RayTLSCertMountPath+"/tls.crt"),
		"container %s should have RAY_TLS_SERVER_CERT", container.Name)
	g.Expect(envMap).To(HaveKeyWithValue(utils.RAY_TLS_SERVER_KEY, utils.RayTLSCertMountPath+"/tls.key"),
		"container %s should have RAY_TLS_SERVER_KEY", container.Name)
	g.Expect(envMap).To(HaveKeyWithValue(utils.RAY_TLS_CA_CERT, utils.RayTLSCertMountPath+"/ca.crt"),
		"container %s should have RAY_TLS_CA_CERT", container.Name)
}

// verifyNoTLSEnvVars asserts a container does NOT have TLS environment variables.
func verifyNoTLSEnvVars(g Gomega, container *corev1.Container) {
	for _, env := range container.Env {
		g.Expect(env.Name).NotTo(Equal(utils.RAY_USE_TLS),
			"container %s should not have RAY_USE_TLS", container.Name)
	}
}

// verifyTLSVolumeMount asserts the pod has a ray-tls volume backed by a Secret
// and the container has a volume mount at the TLS mount path.
func verifyTLSVolumeMount(g Gomega, pod *corev1.Pod, expectedSecretName string) {
	// Find the ray-tls volume
	var tlsVolume *corev1.Volume
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Name == utils.RayTLSVolumeName {
			tlsVolume = &pod.Spec.Volumes[i]
			break
		}
	}
	g.Expect(tlsVolume).NotTo(BeNil(), "pod %s should have a %s volume", pod.Name, utils.RayTLSVolumeName)
	g.Expect(tlsVolume.Secret).NotTo(BeNil(), "volume %s should be a Secret volume", utils.RayTLSVolumeName)

	if expectedSecretName != "" {
		g.Expect(tlsVolume.Secret.SecretName).To(Equal(expectedSecretName),
			"volume %s should reference secret %s", utils.RayTLSVolumeName, expectedSecretName)
	}

	// Verify the Ray container has the volume mount
	rayContainer := pod.Spec.Containers[utils.RayContainerIndex]
	var tlsMount *corev1.VolumeMount
	for i := range rayContainer.VolumeMounts {
		if rayContainer.VolumeMounts[i].Name == utils.RayTLSVolumeName {
			tlsMount = &rayContainer.VolumeMounts[i]
			break
		}
	}
	g.Expect(tlsMount).NotTo(BeNil(), "container %s should have a %s volume mount", rayContainer.Name, utils.RayTLSVolumeName)
	g.Expect(tlsMount.MountPath).To(Equal(utils.RayTLSCertMountPath),
		"volume mount path should be %s", utils.RayTLSCertMountPath)
}

// verifyNoTLSVolume asserts the pod does NOT have a ray-tls volume.
func verifyNoTLSVolume(g Gomega, pod *corev1.Pod) {
	for _, vol := range pod.Spec.Volumes {
		g.Expect(vol.Name).NotTo(Equal(utils.RayTLSVolumeName),
			"pod %s should not have a %s volume", pod.Name, utils.RayTLSVolumeName)
	}
}

// --- Test: Auto-Generate mTLS ---

func TestRayClusterTLSAutoGenerate(t *testing.T) {
	test := With(t)

	if !certManagerAvailable(test) {
		t.Skip("cert-manager CRDs not found; skipping TLS auto-generate e2e test")
	}

	namespace := test.NewTestNamespace()

	t.Run("mTLS auto-generate cluster becomes Ready", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-autogen-mtls"
		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpecWithMTLS().WithRayVersion(GetRayVersion()))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created RayCluster %s/%s with mTLS auto-generate", rayCluster.Namespace, rayCluster.Name)

		// Wait for cluster to become Ready
		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
		LogWithTimestamp(t, "RayCluster %s is Ready", clusterName)

		// Verify head pod is running
		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(headPod).NotTo(BeNil())
		g.Expect(IsPodRunningAndReady(headPod)).To(BeTrue(), "head pod should be running and ready")

		// Verify worker pods are running
		workerPods, err := GetWorkerPods(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(workerPods).NotTo(BeEmpty())
		g.Expect(AllPodsRunningAndReady(workerPods)).To(BeTrue(), "all worker pods should be running and ready")
	})

	t.Run("mTLS auto-generate cert-manager resources created", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-certmgr-res"
		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpecWithMTLS().WithRayVersion(GetRayVersion()))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// Re-fetch to get the UID assigned by the API server.
		rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), clusterName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		// Verify cert-manager CA secret was created.
		caSecretName := utils.GetCASecretName(clusterName, rayCluster.UID)
		caSecret, err := test.Client().Core().CoreV1().Secrets(namespace.Name).Get(test.Ctx(), caSecretName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred(), "CA secret %s should exist", caSecretName)
		g.Expect(caSecret.Data).To(HaveKey("tls.crt"), "CA secret should have tls.crt")
		g.Expect(caSecret.Data).To(HaveKey("tls.key"), "CA secret should have tls.key")
		g.Expect(caSecret.Data).To(HaveKey("ca.crt"), "CA secret should have ca.crt")

		// Verify head certificate secret was created by cert-manager.
		headSecretName := fmt.Sprintf("%s-%s", utils.RayHeadSecretPrefix, clusterName)
		headSecret, err := test.Client().Core().CoreV1().Secrets(namespace.Name).Get(test.Ctx(), headSecretName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred(), "head secret %s should exist", headSecretName)
		g.Expect(headSecret.Data).To(HaveKey("tls.crt"), "head secret should have tls.crt")
		g.Expect(headSecret.Data).To(HaveKey("tls.key"), "head secret should have tls.key")
		g.Expect(headSecret.Data).To(HaveKey("ca.crt"), "head secret should have ca.crt")

		// Verify worker certificate secret was created by cert-manager.
		workerSecretName := fmt.Sprintf("%s-%s", utils.RayWorkerSecretPrefix, clusterName)
		workerSecret, err := test.Client().Core().CoreV1().Secrets(namespace.Name).Get(test.Ctx(), workerSecretName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred(), "worker secret %s should exist", workerSecretName)
		g.Expect(workerSecret.Data).To(HaveKey("tls.crt"), "worker secret should have tls.crt")
		g.Expect(workerSecret.Data).To(HaveKey("tls.key"), "worker secret should have tls.key")
		g.Expect(workerSecret.Data).To(HaveKey("ca.crt"), "worker secret should have ca.crt")

		LogWithTimestamp(t, "Cert-manager CA, head, and worker secrets verified for cluster %s", clusterName)
	})

	t.Run("mTLS auto-generate pod TLS configuration", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-pod-config"
		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpecWithMTLS().WithRayVersion(GetRayVersion()))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// Auto-generate mode: head pod mounts the head secret, workers mount the worker secret.
		headSecretName := fmt.Sprintf("%s-%s", utils.RayHeadSecretPrefix, clusterName)
		workerSecretName := fmt.Sprintf("%s-%s", utils.RayWorkerSecretPrefix, clusterName)

		// Verify head pod configuration
		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		verifyContainerTLSEnvVars(g, &headPod.Spec.Containers[utils.RayContainerIndex])
		verifyTLSVolumeMount(g, headPod, headSecretName)

		// Verify worker pod configuration
		workerPods, err := GetWorkerPods(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(workerPods).NotTo(BeEmpty())
		for i := range workerPods {
			verifyContainerTLSEnvVars(g, &workerPods[i].Spec.Containers[utils.RayContainerIndex])
			verifyTLSVolumeMount(g, &workerPods[i], workerSecretName)
		}

		LogWithTimestamp(t, "Pod TLS configuration verified for cluster %s", clusterName)
	})

	t.Run("mTLS auto-generate Ray job submission succeeds", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-job-submit"
		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpecWithMTLS().WithRayVersion(GetRayVersion()))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())

		// Submit a simple Ray job and verify it completes.
		// This proves head-worker mTLS communication is functional.
		submissionID := fmt.Sprintf("mtls-test-job-%d", time.Now().Unix())
		submitCmd := []string{
			"bash", "-c",
			fmt.Sprintf(
				"ray job submit --address http://127.0.0.1:8265 --submission-id %s --no-wait -- python -c 'import ray; ray.init(); print(ray.cluster_resources())'",
				submissionID,
			),
		}
		stdout, _ := ExecPodCmd(test, headPod, headPod.Spec.Containers[utils.RayContainerIndex].Name, submitCmd)
		g.Expect(stdout.String()).To(ContainSubstring(submissionID), "job submission should succeed")

		// Wait for the job to complete
		g.Eventually(func(gg Gomega) {
			statusCmd := []string{
				"bash", "-c",
				fmt.Sprintf("ray job status --address http://127.0.0.1:8265 %s", submissionID),
			}
			stdout, _ := ExecPodCmd(test, headPod, headPod.Spec.Containers[utils.RayContainerIndex].Name, statusCmd)
			gg.Expect(stdout.String()).To(ContainSubstring("succeeded"))
		}, TestTimeoutMedium).Should(Succeed())

		LogWithTimestamp(t, "Ray job submission succeeded with mTLS for cluster %s", clusterName)
	})
}

// --- Test: Edge Cases ---

func TestRayClusterTLSEdgeCases(t *testing.T) {
	test := With(t)

	namespace := test.NewTestNamespace()

	t.Run("TLS disabled has no TLS resources", func(t *testing.T) {
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-disabled"
		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpec().WithRayVersion(GetRayVersion()))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// Verify NO TLS env vars or volumes on head pod
		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		verifyNoTLSEnvVars(g, &headPod.Spec.Containers[utils.RayContainerIndex])
		verifyNoTLSVolume(g, headPod)

		// Verify NO TLS env vars or volumes on worker pods
		workerPods, err := GetWorkerPods(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		for i := range workerPods {
			verifyNoTLSEnvVars(g, &workerPods[i].Spec.Containers[utils.RayContainerIndex])
			verifyNoTLSVolume(g, &workerPods[i])
		}

		// Verify no CA secret was created for this cluster (list by prefix since
		// the CA secret name includes a UID suffix we don't know for disabled clusters).
		secretList, err := test.Client().Core().CoreV1().Secrets(namespace.Name).List(test.Ctx(), metav1.ListOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		for _, s := range secretList.Items {
			g.Expect(s.Name).NotTo(HavePrefix(clusterName+"-"+utils.RayCASecretPrefix),
				"CA secret should NOT exist for TLS-disabled cluster")
		}

		LogWithTimestamp(t, "TLS disabled cluster %s verified: no TLS resources", clusterName)
	})

	t.Run("mTLS with multiple worker groups", func(t *testing.T) {
		if !certManagerAvailable(test) {
			t.Skip("cert-manager CRDs not found; skipping auto-generate mTLS test")
		}
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-multi-wg"
		spec := rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithTLSOptions(rayv1ac.TLSOptions().WithEnabled(true)).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(HeadPodTemplateApplyConfiguration())).
			WithWorkerGroupSpecs(
				rayv1ac.WorkerGroupSpec().
					WithReplicas(1).WithMinReplicas(1).WithMaxReplicas(1).
					WithGroupName("group-a").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(WorkerPodTemplateApplyConfiguration()),
				rayv1ac.WorkerGroupSpec().
					WithReplicas(1).WithMinReplicas(1).WithMaxReplicas(1).
					WithGroupName("group-b").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(WorkerPodTemplateApplyConfiguration()),
			)

		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).WithSpec(spec)
		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created mTLS RayCluster %s with 2 worker groups", clusterName)

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// All worker pods from both groups mount the same cert-manager worker secret.
		workerSecretName := fmt.Sprintf("%s-%s", utils.RayWorkerSecretPrefix, clusterName)

		groupAPods, err := GetGroupPods(test, rayCluster, "group-a")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(groupAPods).NotTo(BeEmpty(), "group-a should have worker pods")
		for i := range groupAPods {
			verifyContainerTLSEnvVars(g, &groupAPods[i].Spec.Containers[utils.RayContainerIndex])
			verifyTLSVolumeMount(g, &groupAPods[i], workerSecretName)
		}

		groupBPods, err := GetGroupPods(test, rayCluster, "group-b")
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(groupBPods).NotTo(BeEmpty(), "group-b should have worker pods")
		for i := range groupBPods {
			verifyContainerTLSEnvVars(g, &groupBPods[i].Spec.Containers[utils.RayContainerIndex])
			verifyTLSVolumeMount(g, &groupBPods[i], workerSecretName)
		}

		LogWithTimestamp(t, "Multiple worker groups TLS verified for cluster %s", clusterName)
	})

	t.Run("mTLS cluster deletion cleans up cert-manager resources", func(t *testing.T) {
		if !certManagerAvailable(test) {
			t.Skip("cert-manager CRDs not found; skipping auto-generate mTLS test")
		}
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-cleanup"
		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpecWithMTLS().WithRayVersion(GetRayVersion()))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// Re-fetch to get the UID assigned by the API server.
		rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), clusterName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		// Verify secrets exist before deletion.
		caSecretName := utils.GetCASecretName(clusterName, rayCluster.UID)
		headSecretName := fmt.Sprintf("%s-%s", utils.RayHeadSecretPrefix, clusterName)
		workerSecretName := fmt.Sprintf("%s-%s", utils.RayWorkerSecretPrefix, clusterName)

		_, err = test.Client().Core().CoreV1().Secrets(namespace.Name).Get(test.Ctx(), caSecretName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred(), "CA secret should exist before deletion")
		_, err = test.Client().Core().CoreV1().Secrets(namespace.Name).Get(test.Ctx(), headSecretName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred(), "head secret should exist before deletion")

		// Delete the RayCluster
		err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), clusterName, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Deleted RayCluster %s", clusterName)

		// Verify all TLS secrets are garbage-collected via ownerRef.
		g.Eventually(func(gg Gomega) {
			_, err := test.Client().Core().CoreV1().Secrets(namespace.Name).Get(test.Ctx(), caSecretName, metav1.GetOptions{})
			gg.Expect(k8serrors.IsNotFound(err)).To(BeTrue(), "CA secret should be cleaned up after RayCluster deletion")
			_, err = test.Client().Core().CoreV1().Secrets(namespace.Name).Get(test.Ctx(), headSecretName, metav1.GetOptions{})
			gg.Expect(k8serrors.IsNotFound(err)).To(BeTrue(), "head secret should be cleaned up after RayCluster deletion")
			_, err = test.Client().Core().CoreV1().Secrets(namespace.Name).Get(test.Ctx(), workerSecretName, metav1.GetOptions{})
			gg.Expect(k8serrors.IsNotFound(err)).To(BeTrue(), "worker secret should be cleaned up after RayCluster deletion")
		}, TestTimeoutMedium).Should(Succeed())

		LogWithTimestamp(t, "Cert-manager resources cleaned up after deletion of cluster %s", clusterName)
	})

	t.Run("mTLS with autoscaler enabled", func(t *testing.T) {
		if !certManagerAvailable(test) {
			t.Skip("cert-manager CRDs not found; skipping auto-generate mTLS test")
		}
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-autoscaler"
		spec := NewRayClusterSpecWithMTLS().
			WithRayVersion(GetRayVersion()).
			WithEnableInTreeAutoscaling(true)

		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).WithSpec(spec)
		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(t, "Created mTLS RayCluster %s with autoscaler enabled", clusterName)

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())

		// The autoscaler sidecar is injected as a second container.
		// Find it and verify it has TLS config too.
		g.Expect(len(headPod.Spec.Containers)).To(BeNumerically(">", 1),
			"head pod should have more than 1 container when autoscaler is enabled")

		// Verify the Ray container has TLS
		verifyContainerTLSEnvVars(g, &headPod.Spec.Containers[utils.RayContainerIndex])

		// Verify the autoscaler sidecar also has TLS env vars
		for i := 1; i < len(headPod.Spec.Containers); i++ {
			container := &headPod.Spec.Containers[i]
			verifyContainerTLSEnvVars(g, container)
			LogWithTimestamp(t, "Autoscaler container %s has TLS env vars", container.Name)
		}

		// Verify TLS volume mount exists on the pod with the head secret.
		headSecretName := fmt.Sprintf("%s-%s", utils.RayHeadSecretPrefix, clusterName)
		verifyTLSVolumeMount(g, headPod, headSecretName)

		LogWithTimestamp(t, "Autoscaler TLS verified for cluster %s", clusterName)
	})

	t.Run("Init container TLS configuration", func(t *testing.T) {
		if !certManagerAvailable(test) {
			t.Skip("cert-manager CRDs not found; skipping auto-generate mTLS test")
		}
		t.Parallel()
		g := NewWithT(t)

		clusterName := "tls-initcontainer"
		rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
			WithSpec(NewRayClusterSpecWithMTLS().WithRayVersion(GetRayVersion()))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// Worker pods should have init containers (wait-gcs-ready) with TLS env vars
		workerPods, err := GetWorkerPods(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(workerPods).NotTo(BeEmpty())

		for _, pod := range workerPods {
			g.Expect(pod.Spec.InitContainers).NotTo(BeEmpty(),
				"worker pod %s should have init containers", pod.Name)

			// Only the wait-gcs-ready init container should have TLS config.
			for _, initContainer := range pod.Spec.InitContainers {
				if initContainer.Name != "wait-gcs-ready" {
					continue
				}
				envMap := make(map[string]string)
				for _, env := range initContainer.Env {
					envMap[env.Name] = env.Value
				}
				g.Expect(envMap).To(HaveKeyWithValue(utils.RAY_USE_TLS, "1"),
					"init container %s should have RAY_USE_TLS=1", initContainer.Name)

				// Verify init container has TLS volume mount
				var hasMount bool
				for _, mount := range initContainer.VolumeMounts {
					if mount.Name == utils.RayTLSVolumeName {
						hasMount = true
						g.Expect(mount.MountPath).To(Equal(utils.RayTLSCertMountPath))
						break
					}
				}
				g.Expect(hasMount).To(BeTrue(),
					"init container %s should have %s volume mount", initContainer.Name, utils.RayTLSVolumeName)
			}
		}

		LogWithTimestamp(t, "Init container TLS configuration verified for cluster %s", clusterName)
	})
}

// --- Test: Autoscaler + mTLS scale-up ---

// workerCertificateIPs returns the IP SANs currently configured on the worker
// cert-manager Certificate of the given RayCluster. It uses the dynamic client so the
// test does not need to depend on the cert-manager clientset.
func workerCertificateIPs(test Test, namespace, clusterName string) ([]string, error) {
	certGVR := schema.GroupVersionResource{Group: "cert-manager.io", Version: "v1", Resource: "certificates"}
	certName := utils.GetTLSCertName(clusterName, rayv1.WorkerNode)

	cert, err := test.Client().Dynamic().Resource(certGVR).Namespace(namespace).Get(test.Ctx(), certName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	ips, _, err := unstructured.NestedStringSlice(cert.Object, "spec", "ipAddresses")
	return ips, err
}

// TestRayClusterTLSAutoscalerScaleUp verifies that the autoscaler and the mTLS controller
// work together: worker pods added by the autoscaler are covered by the worker certificate
// IP SANs, and their IPs are dropped again after a scale-down.
//
// The existing "mTLS with autoscaler enabled" case pins the worker group to
// replicas = minReplicas = maxReplicas = 1, so it never triggers a scale-up. Worker pods
// only reach Running after wait-for-tls-ip-san and wait-gcs-ready succeed, so asserting
// readiness also covers the mTLS handshake.
func TestRayClusterTLSAutoscalerScaleUp(t *testing.T) {
	test := With(t)

	if !certManagerAvailable(test) {
		t.Skip("cert-manager CRDs not found; skipping autoscaler mTLS scale-up e2e test")
	}

	g := NewWithT(t)
	namespace := test.NewTestNamespace()

	// Scripts for creating and terminating detached actors to trigger autoscaling
	scriptsAC := NewConfigMap(namespace.Name, Files(test, "create_detached_actor.py", "terminate_detached_actor.py"))
	scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(t, "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

	clusterName := "tls-autoscaler-scaleup"
	specAC := rayv1ac.RayClusterSpec().
		WithRayVersion(GetRayVersion()).
		WithEnableInTreeAutoscaling(true).
		WithTLSOptions(rayv1ac.TLSOptions().WithEnabled(true)).
		WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
			WithRayStartParams(map[string]string{"num-cpus": "0"}).
			WithTemplate(HeadPodTemplateApplyConfiguration())).
		WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
			WithReplicas(0).
			WithMinReplicas(0).
			WithMaxReplicas(2).
			WithGroupName("small-group").
			WithRayStartParams(map[string]string{"num-cpus": "1"}).
			WithTemplate(WorkerPodTemplateApplyConfiguration()))

	rayClusterAC := rayv1ac.RayCluster(clusterName, namespace.Name).
		WithSpec(Apply(specAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(t, "Created mTLS RayCluster %s/%s with autoscaler enabled", rayCluster.Namespace, rayCluster.Name)

	// Wait for RayCluster to become ready and verify there is no worker replica.
	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).
		To(WithTransform(RayClusterDesiredWorkerReplicas, Equal(int32(0))))

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	rayContainerName := headPod.Spec.Containers[utils.RayContainerIndex].Name
	workerSecretName := utils.GetTLSSecretName(clusterName, rayv1.WorkerNode)

	// Each detached actor requests 1 CPU, so creating one actor adds one worker pod.
	for i, actorName := range []string{"actor1", "actor2"} {
		expectedReplicas := int32(i + 1)

		ExecPodCmd(test, headPod, rayContainerName, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", actorName, "--num-cpus=1"})

		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(WithTransform(RayClusterDesiredWorkerReplicas, Equal(expectedReplicas)))
		LogWithTimestamp(t, "Autoscaler scaled the worker group to %d replica(s) after creating %s", expectedReplicas, actorName)

		// The scaled-up worker pods must become ready and be configured for mTLS.
		g.Eventually(func(gg Gomega) {
			workerPods, err := GetWorkerPods(test, rayCluster)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(workerPods).To(HaveLen(int(expectedReplicas)))
			gg.Expect(AllPodsRunningAndReady(workerPods)).To(BeTrue(),
				"all worker pods should be running and ready after scale-up")

			for j := range workerPods {
				verifyContainerTLSEnvVars(gg, &workerPods[j].Spec.Containers[utils.RayContainerIndex])
				verifyTLSVolumeMount(gg, &workerPods[j], workerSecretName)
			}
		}, TestTimeoutLong).Should(Succeed())

		// The worker certificate must cover the new pod IPs, otherwise the new worker
		// cannot complete the mTLS handshake with GCS.
		g.Eventually(func(gg Gomega) {
			workerPods, err := GetWorkerPods(test, rayCluster)
			gg.Expect(err).NotTo(HaveOccurred())

			certIPs, err := workerCertificateIPs(test, namespace.Name, clusterName)
			gg.Expect(err).NotTo(HaveOccurred())

			for j := range workerPods {
				gg.Expect(certIPs).To(ContainElement(workerPods[j].Status.PodIP),
					"worker certificate IP SANs should contain the IP of pod %s", workerPods[j].Name)
			}
		}, TestTimeoutLong).Should(Succeed())
		LogWithTimestamp(t, "Worker certificate IP SANs cover all %d worker pod(s)", expectedReplicas)
	}

	// Record the worker pod IPs before scaling down so the test can verify they are
	// removed from the worker certificate afterwards.
	scaledUpWorkerPods, err := GetWorkerPods(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	scaledUpWorkerIPs := make([]string, 0, len(scaledUpWorkerPods))
	for i := range scaledUpWorkerPods {
		scaledUpWorkerIPs = append(scaledUpWorkerIPs, scaledUpWorkerPods[i].Status.PodIP)
	}
	g.Expect(scaledUpWorkerIPs).To(HaveLen(2), "both worker pod IPs should be recorded before scale-down")

	// Terminating the actors releases the CPUs and the autoscaler scales back down.
	for _, actorName := range []string{"actor1", "actor2"} {
		ExecPodCmd(test, headPod, rayContainerName,
			[]string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", actorName})
	}
	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
		Should(WithTransform(RayClusterDesiredWorkerReplicas, Equal(int32(0))))
	LogWithTimestamp(t, "Autoscaler scaled the worker group back down to 0 replicas")

	// The worker certificate must drop the IP SANs of the removed worker pods.
	g.Eventually(func(gg Gomega) {
		certIPs, err := workerCertificateIPs(test, namespace.Name, clusterName)
		gg.Expect(err).NotTo(HaveOccurred())

		for _, podIP := range scaledUpWorkerIPs {
			gg.Expect(certIPs).NotTo(ContainElement(podIP),
				"worker certificate IP SANs should no longer contain the IP %s of the removed worker pod", podIP)
		}
	}, TestTimeoutLong).Should(Succeed())
	LogWithTimestamp(t, "Worker certificate IP SANs no longer contain the removed worker pod IPs")
}
