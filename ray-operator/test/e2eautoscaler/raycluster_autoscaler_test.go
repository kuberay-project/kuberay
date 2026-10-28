package e2eautoscaler

import (
	"fmt"
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayClusterAutoscaler(t *testing.T) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)

			// Create a namespace
			namespace := test.NewTestNamespace()

			// Scripts for creating and terminating detached actors to trigger autoscaling
			scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_actor.py", "terminate_detached_actor.py"))
			scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(3).
					WithGroupName("small-group").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(tc.WorkerPodTemplateGetter()))
			rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Wait for RayCluster to become ready and verify the number of available worker replicas.
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			// Create a detached actor, and a worker should be created.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "actor1"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))

			// Create a detached actor, and a worker should be created.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "actor2"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(2))))

			// Terminate a detached actor, and a worker should be deleted.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "actor1"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))

			// Terminate a detached actor, and a worker should be deleted.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "actor2"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))
		})
	}
}

func TestRayClusterAutoscalerWithFakeGPU(t *testing.T) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)

			// Create a namespace
			namespace := test.NewTestNamespace()

			// Scripts for creating and terminating detached actors to trigger autoscaling
			scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_actor.py", "terminate_detached_actor.py"))
			scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(3).
					WithGroupName("gpu-group").
					WithRayStartParams(map[string]string{"num-cpus": "1", "num-gpus": "1"}).
					WithTemplate(tc.WorkerPodTemplateGetter()))
			rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Wait for RayCluster to become ready and verify the number of available worker replicas.
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			// Create a detached gpu actor, and a worker in the "gpu-group" should be created.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "gpu_actor", "--num-gpus=1"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))
			// We don't use real GPU resources of Kubernetes here, therefore we can't test the RayClusterDesiredGPU.
			// We test the Pods count of the "gpu-group" instead.
			g.Expect(GetGroupPods(test, rayCluster, "gpu-group")).To(gomega.HaveLen(1))

			// Terminate the gpu detached actor, and the worker should be deleted.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "gpu_actor"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))
		})
	}
}

func TestRayClusterAutoscalerWithFakeTPU(t *testing.T) {
	// Enable the below multi-host indexing feature, even on single-host, to ensure
	// stability on TPU which commonly run in SPMD configurations.
	features.SetFeatureGateDuringTest(t, features.RayMultiHostIndexing, true)

	tpuTestCases := []struct {
		nodeSelector map[string]string
		name         string
		groupName    string
		clusterName  string
		numOfHosts   int32
		maxReplicas  int32
	}{
		{
			name:        "v4 2x2x1 single-host",
			numOfHosts:  1,
			maxReplicas: 3,
			nodeSelector: map[string]string{
				"cloud.google.com/gke-tpu-topology":    "2x2x1",
				"cloud.google.com/gke-tpu-accelerator": "tpu-v4-podslice",
			},
			groupName:   "tpu-group",
			clusterName: "ray-cluster",
		},
		{
			name:        "v6e 4x4 multi-host",
			numOfHosts:  4,
			maxReplicas: 4,
			nodeSelector: map[string]string{
				"cloud.google.com/gke-tpu-topology":    "4x4",
				"cloud.google.com/gke-tpu-accelerator": "tpu-v6e-slice",
			},
			groupName:   "tpu-multi-host-group",
			clusterName: "ray-cluster-multi-host",
		},
	}

	for _, tc := range tests {
		for _, tpuTc := range tpuTestCases {
			t.Run(tc.name+"-"+tpuTc.name, func(t *testing.T) {
				test := With(t)
				g := gomega.NewWithT(t)

				// Create a namespace
				namespace := test.NewTestNamespace()

				// Scripts for creating and terminating detached actors to trigger autoscaling
				scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_actor.py", "terminate_detached_actor.py"))
				scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

				workerTemplate := tc.WorkerPodTemplateGetter()
				workerTemplate.Spec.WithNodeSelector(tpuTc.nodeSelector)

				rayClusterSpecAC := rayv1ac.RayClusterSpec().
					WithEnableInTreeAutoscaling(true).
					WithRayVersion(GetRayVersion()).
					WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
						WithRayStartParams(map[string]string{"num-cpus": "0"}).
						WithTemplate(tc.HeadPodTemplateGetter())).
					WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
						WithReplicas(0).
						WithMinReplicas(0).
						WithMaxReplicas(tpuTc.maxReplicas).
						WithNumOfHosts(tpuTc.numOfHosts).
						WithGroupName(tpuTc.groupName).
						WithRayStartParams(map[string]string{"num-cpus": "1", "resources": `'{"TPU":4}'`}).
						WithTemplate(workerTemplate))
				rayClusterAC := rayv1ac.RayCluster(tpuTc.clusterName, namespace.Name).
					WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

				rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

				// Wait for RayCluster to become ready and verify the number of available worker replicas.
				g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
					Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
				g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

				headPod, err := GetHeadPod(test, rayCluster)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

				// Create detached TPU actors concurrently using a single shell script execution.
				// This ensures the Autoscaler sees the resource request on the same iteration.
				ExecPodCmd(test, headPod, common.RayHeadContainer, []string{
					"bash", "-c",
					fmt.Sprintf(`for i in {0..%d}; do python /home/ray/test_scripts/create_detached_actor.py "tpu_actor_$i" --custom-resource-name=TPU --num-custom-resources=4 & done; wait`, tpuTc.numOfHosts-1),
				})

				// Autoscaler scales up desired replicas by 1
				g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
					Should(gomega.WithTransform(GetRayClusterWorkerGroupReplicaSum, gomega.Equal(int32(1))))
				g.Eventually(GroupPods(test, rayCluster, tpuTc.groupName), TestTimeoutLong).Should(gomega.HaveLen(int(tpuTc.numOfHosts)))
				LogWithTimestamp(test.T(), "Created TPU workers of group %s", tpuTc.groupName)

				// Terminate all TPU actors concurrently to remove the allocated resource requests.
				ExecPodCmd(test, headPod, common.RayHeadContainer, []string{
					"bash", "-c",
					fmt.Sprintf(`for i in {0..%d}; do python /home/ray/test_scripts/terminate_detached_actor.py "tpu_actor_$i" & done; wait`, tpuTc.numOfHosts-1),
				})

				// Set maxReplicas of the TPU worker group replica to 0 to force scale-down.
				// It's not possible to wait on idle timeout since the required TPU nodeSelectors prevent scheduling.
				rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), rayCluster.Name, metav1.GetOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
				rayCluster.Spec.WorkerGroupSpecs[0].MaxReplicas = Ptr(int32(0))
				rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Update(test.Ctx(), rayCluster, metav1.UpdateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Updated RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

				// Validate that the entire TPU slice is scaled down.
				g.Eventually(WorkerPods(test, rayCluster), TestTimeoutMedium).Should(gomega.BeEmpty())
			})
		}
	}
}

func TestRayClusterAutoscalerWithCustomResource(t *testing.T) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)

			// Create a namespace
			namespace := test.NewTestNamespace()

			// Scripts for creating and terminating detached actors to trigger autoscaling
			scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_actor.py", "terminate_detached_actor.py"))
			scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

			groupName := "custom-resource-group"

			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(3).
					WithGroupName(groupName).
					WithRayStartParams(map[string]string{"num-cpus": "1", "resources": `'{"CustomResource": 1}'`}).
					WithTemplate(tc.WorkerPodTemplateGetter()))
			rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)
			// Wait for RayCluster to become ready and verify the number of available worker replicas.
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			// Create a detached custom resource actor, and a worker in the "custom-resource-group" should be created.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "custom_resource_actor", "--num-custom-resources=1"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))
			g.Expect(GetGroupPods(test, rayCluster, groupName)).To(gomega.HaveLen(1))

			// Terminate the custom resource detached actor, and the worker should be deleted.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "custom_resource_actor"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))
		})
	}
}

func TestRayClusterAutoscalerWithDesiredState(t *testing.T) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)

			const maxReplica = 3
			// Set the scale down window to a large enough value, so scale down could be disabled to avoid test flakiness.
			const scaleDownWaitSec = 3600

			// Create a namespace
			namespace := test.NewTestNamespace()

			// Scripts for creating and terminating detached actors to trigger autoscaling
			scriptsAC := newConfigMap(namespace.Name, Files(test, "create_concurrent_tasks.py"))
			scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

			groupName := "custom-resource-group"
			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(maxReplica).
					WithGroupName(groupName).
					WithRayStartParams(map[string]string{"num-cpus": "1", "resources": `'{"CustomResource": 1}'`}).
					WithTemplate(tc.WorkerPodTemplateGetter())).
				WithAutoscalerOptions(rayv1ac.AutoscalerOptions().
					WithIdleTimeoutSeconds(scaleDownWaitSec))
			rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Wait for RayCluster to become ready and verify the number of available worker replicas.
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			// Create a number of tasks and wait for their completion, and a worker in the "custom-resource-group" should be created.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_concurrent_tasks.py"})

			// Scale down has been disabled, after ray script execution completion the cluster is expected to have max replica's number of pods.
			pods, err := GetWorkerPods(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(pods).To(gomega.HaveLen(maxReplica))
		})
	}
}

func TestRayClusterAutoscalerMinReplicasUpdate(t *testing.T) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)

			// Create a namespace
			namespace := test.NewTestNamespace()

			// Script for creating detached actors to trigger autoscaling
			scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_actor.py"))
			scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

			groupName := "test-group"

			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(1).
					WithMinReplicas(0).
					WithMaxReplicas(3).
					WithGroupName(groupName).
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(tc.WorkerPodTemplateGetter()))
			rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Wait for RayCluster to become ready
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))

			// Update minReplicas from 0 to 2
			rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), rayCluster.Name, metav1.GetOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			rayCluster.Spec.WorkerGroupSpecs[0].MinReplicas = new(int32(2))
			rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Update(test.Ctx(), rayCluster, metav1.UpdateOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Updated RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Verify that KubeRay creates an additional Pod
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(2))))

			// Create detached actors to trigger autoscaling to 3 Pods
			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			for i := range 3 {
				ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", fmt.Sprintf("actor%d", i)})
			}

			// Verify that the Autoscaler scales up to 3 Pods
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(3))))

			// Check that replicas is set to 3
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(GetRayClusterWorkerGroupReplicaSum, gomega.Equal(int32(3))))
		})
	}
}

func TestRayClusterAutoscalerMaxReplicasUpdate(t *testing.T) {
	replicaTests := []struct {
		name             string
		initialMax       int32
		updatedMax       int32
		expectedReplicas int32
		actorCount       int
	}{
		{
			name:       "Scale up maxReplicas from 2 to 3",
			initialMax: 2,
			updatedMax: 3,
		},
		{
			name:       "Scale down maxReplicas from 2 to 1",
			initialMax: 2,
			updatedMax: 1,
		},
	}

	for _, tc := range tests {
		for _, rtc := range replicaTests {
			t.Run(fmt.Sprintf("%s(%s)", tc.name, rtc.name), func(t *testing.T) {
				test := With(t)
				g := gomega.NewWithT(t)

				namespace := test.NewTestNamespace()

				scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_actor.py"))
				scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				groupName := "test-group"

				rayClusterSpecAC := rayv1ac.RayClusterSpec().
					WithEnableInTreeAutoscaling(true).
					WithRayVersion(GetRayVersion()).
					WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
						WithRayStartParams(map[string]string{"num-cpus": "0"}).
						WithTemplate(tc.HeadPodTemplateGetter())).
					WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
						WithReplicas(1).
						WithMinReplicas(1).
						WithMaxReplicas(rtc.initialMax).
						WithGroupName(groupName).
						WithRayStartParams(map[string]string{"num-cpus": "1"}).
						WithTemplate(tc.WorkerPodTemplateGetter()))
				rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
					WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

				rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Wait for RayCluster to become ready and verify the number of available initial worker replicas (1)
				g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
					Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
				g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))

				headPod, err := GetHeadPod(test, rayCluster)
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Create detached actors
				for i := 0; i < int(max(rtc.updatedMax, rtc.initialMax)); i++ {
					ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", fmt.Sprintf("actor%d", i)})
				}

				g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
					Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(rtc.initialMax)))
				// Verify that the Autoscaler scales up/down to initialMax Pod count
				g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).
					To(gomega.WithTransform(GetRayClusterWorkerGroupReplicaSum, gomega.Equal(rtc.initialMax)))

				// Update maxReplicas
				rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Get(test.Ctx(), rayCluster.Name, metav1.GetOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())
				rayCluster.Spec.WorkerGroupSpecs[0].MaxReplicas = new(rtc.updatedMax)
				rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Update(test.Ctx(), rayCluster, metav1.UpdateOptions{})
				g.Expect(err).NotTo(gomega.HaveOccurred())

				// Check that replicas is set to the updatedMax
				g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
					Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(rtc.updatedMax)))

				// Verify that the Autoscaler scales up/down to updatedMax Pod count
				g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutShort).
					Should(gomega.WithTransform(GetRayClusterWorkerGroupReplicaSum, gomega.Equal(rtc.updatedMax)))
			})
		}
	}
}
