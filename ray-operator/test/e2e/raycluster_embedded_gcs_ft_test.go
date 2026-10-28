package e2e

import (
	"os"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// TestRayClusterEmbeddedGCSFaultToleranceWiring validates the operator-owned
// surface of the embedded RocksDB GCS FT backend: PVC provisioning, head-Pod
// env/volume wiring, and PVC garbage collection on RayCluster deletion.
//
// It deliberately does not wait for the head Pod to become ready and does not
// require the embedded RocksDB store to initialize, so it runs against ANY Ray
// image (the operator sets the Pod spec and provisions the PVC regardless of
// whether the Ray version supports the backend). This makes it a fast, always-on
// e2e check that a reviewer can run on a local k3d/kind cluster.
func TestRayClusterEmbeddedGCSFaultToleranceWiring(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := rayv1ac.RayCluster("raycluster-embedded-gcsft-wiring", namespace.Name).
		WithSpec(NewRayClusterSpec().
			WithGcsFaultToleranceOptions(
				rayv1ac.GcsFaultToleranceOptions().
					WithBackend(rayv1.GcsFTBackendRocksDB).
					WithStorage(rayv1ac.GcsEmbeddedStorage().
						WithSize(resource.MustParse("1Gi")).
						WithAccessModes(corev1.ReadWriteOnce)),
			),
		)

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	// The operator provisions an operator-managed PVC owned by the RayCluster.
	pvcName := rayCluster.Name + utils.GCSStoragePVCSuffix
	LogWithTimestamp(test.T(), "Verifying operator-managed PVC %s/%s", namespace.Name, pvcName)
	g.Eventually(func(g Gomega) {
		pvc, err := test.Client().Core().CoreV1().PersistentVolumeClaims(namespace.Name).Get(test.Ctx(), pvcName, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(pvc.Spec.AccessModes).To(ContainElement(corev1.ReadWriteOnce))
		g.Expect(pvc.Spec.Resources.Requests[corev1.ResourceStorage]).To(Equal(resource.MustParse("1Gi")))
		g.Expect(pvc.OwnerReferences).To(HaveLen(1))
		g.Expect(pvc.OwnerReferences[0].Kind).To(Equal("RayCluster"))
		g.Expect(pvc.OwnerReferences[0].Name).To(Equal(rayCluster.Name))
	}, TestTimeoutShort).Should(Succeed())

	// The head Pod is wired with the embedded backend env vars, volume, and mount.
	LogWithTimestamp(test.T(), "Verifying head Pod embedded GCS FT wiring")
	g.Eventually(func(g Gomega) {
		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		container := headPod.Spec.Containers[utils.RayContainerIndex]

		g.Expect(getEnvVarValue(container.Env, utils.RAY_GCS_STORAGE)).To(Equal(utils.GCSStorageRocksDBValue))
		g.Expect(getEnvVarValue(container.Env, utils.RAY_GCS_STORAGE_PATH)).To(Equal(utils.GCSStorageMountPath))
		// No Redis env vars leak onto the embedded path.
		g.Expect(utils.EnvVarExists(utils.RAY_REDIS_ADDRESS, container.Env)).To(BeFalse())

		foundMount := false
		for _, m := range container.VolumeMounts {
			if m.Name == utils.GCSStorageVolumeName && m.MountPath == utils.GCSStorageMountPath {
				foundMount = true
			}
		}
		g.Expect(foundMount).To(BeTrue(), "expected volume mount %s at %s", utils.GCSStorageVolumeName, utils.GCSStorageMountPath)

		foundVolume := false
		for _, v := range headPod.Spec.Volumes {
			if v.Name == utils.GCSStorageVolumeName && v.PersistentVolumeClaim != nil && v.PersistentVolumeClaim.ClaimName == pvcName {
				foundVolume = true
			}
		}
		g.Expect(foundVolume).To(BeTrue(), "expected volume %s referencing PVC %s", utils.GCSStorageVolumeName, pvcName)
	}, TestTimeoutMedium).Should(Succeed())

	// Deleting the RayCluster garbage-collects the operator-managed PVC via ownerReferences.
	LogWithTimestamp(test.T(), "Deleting RayCluster and verifying operator-managed PVC is garbage collected")
	err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() bool {
		_, err := test.Client().Core().CoreV1().PersistentVolumeClaims(namespace.Name).Get(test.Ctx(), pvcName, metav1.GetOptions{})
		return err != nil
	}, TestTimeoutShort, time.Second).Should(BeTrue(), "expected operator-managed PVC %s to be garbage collected", pvcName)
}

// TestRayClusterEmbeddedGCSFaultTolerance exercises real GCS state recovery from
// the embedded RocksDB store across a head-Pod restart, plus PVC garbage
// collection on RayCluster deletion.
//
// It requires a Ray image that contains ray-project/ray#63657 (the
// RocksDbStoreClient). Because that image is not the default CI image, the test
// is skipped unless the KUBERAY_TEST_EMBEDDED_GCS_FT env var is set to "true".
// Point the Ray image at a build containing the PR via KUBERAY_TEST_RAY_IMAGE:
//
//	KUBERAY_TEST_EMBEDDED_GCS_FT=true \
//	KUBERAY_TEST_RAY_IMAGE=<registry>/ray:pr-63657 \
//	go test ./ray-operator/test/e2e -run TestRayClusterEmbeddedGCSFaultTolerance$ -v
func TestRayClusterEmbeddedGCSFaultTolerance(t *testing.T) {
	if os.Getenv("KUBERAY_TEST_EMBEDDED_GCS_FT") != "true" {
		t.Skip("Skipping embedded GCS FT recovery e2e test; set KUBERAY_TEST_EMBEDDED_GCS_FT=true and a Ray image containing ray#63657 to run it.")
	}

	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()
	testScriptAC := NewConfigMap(namespace.Name, Files(test, "test_detached_actor_1.py", "test_detached_actor_2.py"))
	testScript, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), testScriptAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	rayClusterSpecAC := rayv1ac.RayClusterSpec().
		WithGcsFaultToleranceOptions(
			rayv1ac.GcsFaultToleranceOptions().
				WithBackend(rayv1.GcsFTBackendRocksDB).
				WithStorage(rayv1ac.GcsEmbeddedStorage().WithSize(resource.MustParse("1Gi"))),
		).
		WithRayVersion(GetRayVersion()).
		WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
			WithRayStartParams(map[string]string{"num-cpus": "0"}).
			WithTemplate(HeadPodTemplateApplyConfiguration()),
		).
		WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
			WithRayStartParams(map[string]string{"num-cpus": "1"}).
			WithGroupName("small-group").
			WithReplicas(1).
			WithMinReplicas(1).
			WithMaxReplicas(2).
			WithTemplate(WorkerPodTemplateApplyConfiguration()),
		)
	rayClusterAC := rayv1ac.RayCluster("raycluster-embedded-gcsft", namespace.Name).
		WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](testScript, "/home/ray/samples")))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutLong).
		Should(WithTransform(StatusCondition(rayv1.RayClusterProvisioned), MatchCondition(metav1.ConditionTrue, rayv1.AllPodRunningAndReadyFirstTime)))

	rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), rayCluster.Name, metav1.GetOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	// Create a detached actor, then restart the head Pod and confirm the actor's
	// state is recovered from the embedded RocksDB store on the persistent volume.
	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	rayNamespace := "testing-ray-namespace"
	ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "samples/test_detached_actor_1.py", rayNamespace})

	LogWithTimestamp(test.T(), "Deleting the head Pod to trigger recovery from the embedded store")
	headPod, err = DeletePodAndWait(test, rayCluster, namespace, headPod)
	g.Expect(err).NotTo(HaveOccurred())

	// If GCS state survived on the PVC, the counter continues from the prior value.
	expectedOutput := "2"
	ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "samples/test_detached_actor_2.py", rayNamespace, expectedOutput})

	// Deleting the RayCluster garbage-collects the operator-managed PVC via ownerReferences.
	LogWithTimestamp(test.T(), "Deleting RayCluster and verifying operator-managed PVC is garbage collected")
	pvcName := rayCluster.Name + utils.GCSStoragePVCSuffix
	err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() bool {
		_, err := test.Client().Core().CoreV1().PersistentVolumeClaims(namespace.Name).Get(test.Ctx(), pvcName, metav1.GetOptions{})
		return err != nil
	}, TestTimeoutMedium, time.Second).Should(BeTrue(), "expected operator-managed PVC %s to be garbage collected", pvcName)
}

func getEnvVarValue(envs []corev1.EnvVar, name string) string {
	for _, e := range envs {
		if e.Name == name {
			return e.Value
		}
	}
	return ""
}
