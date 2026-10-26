package e2eautoscaler

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayClusterAutoscalerV2IdleTimeout(t *testing.T) {
	// Only test with the V2 Autoscaler
	tc := tests[1]
	t.Run(tc.name, func(t *testing.T) {
		test := With(t)
		g := gomega.NewWithT(t)

		// Create a namespace
		namespace := test.NewTestNamespace()

		idleTimeoutShort := int32(10)
		idleTimeoutLong := int32(30)
		timeoutBuffer := int32(30) // Additional wait time to allow for scale down operation

		// Script for creating detached actors to trigger autoscaling
		scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_actor.py", "terminate_detached_actor.py"))
		scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

		groupName1 := "short-idle-timeout-group"
		groupName2 := "long-idle-timeout-group"
		rayClusterSpecAC := rayv1ac.RayClusterSpec().
			WithEnableInTreeAutoscaling(true).
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"num-cpus": "0"}).
				WithTemplate(tc.HeadPodTemplateGetter())).
			WithWorkerGroupSpecs(
				rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(1).
					WithIdleTimeoutSeconds(idleTimeoutShort).
					WithGroupName(groupName1).
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(tc.WorkerPodTemplateGetter()),
				rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(1).
					WithIdleTimeoutSeconds(idleTimeoutLong).
					WithGroupName(groupName2).
					WithRayStartParams(map[string]string{"num-cpus": "2"}).
					WithTemplate(tc.WorkerPodTemplateGetter()),
			)
		rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
			WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		// Wait for RayCluster to become ready
		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
		g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(gomega.HaveOccurred())
		LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

		// Deploy one detached actor on each worker group. This is guaranteed by setting `maxReplicas` and specifying respective num-cpus.
		ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "actor-long-timeout", "--num-cpus=2"})
		ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "actor-short-timeout", "--num-cpus=1"})
		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
			Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(2))))
		g.Expect(GetGroupPods(test, rayCluster, groupName1)).To(gomega.HaveLen(1))
		g.Expect(GetGroupPods(test, rayCluster, groupName2)).To(gomega.HaveLen(1))

		// Terminate the first detached actor, and the worker should be marked idle after ~10 seconds.
		ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "actor-short-timeout"})
		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), time.Duration(idleTimeoutShort+timeoutBuffer)*time.Second).
			Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))

		// Terminate the second detached actor, and the worker should be marked idle after ~30 seconds.
		ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "actor-long-timeout"})
		g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), time.Duration(idleTimeoutLong+timeoutBuffer)*time.Second).
			Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))
	})
}

// This test verifies that the autoscaler can still trigger GPU nodes for CPU tasks when no CPU-only worker group is defined.
func TestRayClusterAutoscalerGPUNodesForCPUTasks(t *testing.T) {
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

			groupName := "gpu-group"

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
					// This group has GPU resources
					WithRayStartParams(map[string]string{"num-cpus": "1", "num-gpus": "1"}).
					WithTemplate(tc.WorkerPodTemplateGetter()))

			rayClusterAC := rayv1ac.RayCluster("ray-cluster-gpu-for-cpu", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Wait for RayCluster to become ready and verify the number of available worker replicas
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			// Create a detached actor that only needs CPU resources
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "cpu_actor", "--num-cpus=1"})

			// Verify that the autoscaler creates a GPU node for this CPU-only task
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))

			// Verify that the created node is from the GPU worker group
			g.Expect(GetGroupPods(test, rayCluster, groupName)).To(gomega.HaveLen(1))

			// Terminate the actor, and the worker should be deleted
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "cpu_actor"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))
		})
	}
}

// This test verifies that the autoscaler does not remove idle nodes required by an upcoming placement group.
// The following is how the test works with the do_not_remove_idles_for_pg.py script:
// 1. We create a placement group `pg1` with a bundle [{"CPU": 1}].
// 2. Autoscaler should scale up 1 worker Pod (`worker1`).
// 3. We remove `pg1`.
// 4. We create a placement group `pg2` with bundles [{"CPU": 1}, {"CPU": 1}].
// 5. Autoscaler should scale up the second worker Pod (`worker2`), but it needs at least 15 seconds to be up and running due to the injected init container.
// 6. We verify that `worker1` should not be terminated, although it is idle for more than the `IdleTimeoutSeconds`, which is 6 seconds.
func TestRayClusterAutoscalerDoNotRemoveIdlesForPlacementGroup(t *testing.T) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)

			// Create a namespace
			namespace := test.NewTestNamespace()

			scriptsAC := newConfigMap(namespace.Name, Files(test, "do_not_remove_idles_for_pg.py"))
			scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

			workerTemplate := tc.WorkerPodTemplateGetter()
			workerTemplate.Spec.WithInitContainers(corev1ac.Container().
				WithName("init-sleep").
				WithImage(GetRayImage()).
				// delay the worker startup to make sure it takes longer than the IdleTimeoutSeconds, which is 6 seconds,
				// and longer than the default autoscaler update interval of 5 seconds.
				WithCommand("bash", "-c", "sleep 15"))

			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithRayVersion(GetRayVersion()).
				WithAutoscalerOptions(rayv1ac.AutoscalerOptions().
					WithIdleTimeoutSeconds(6)).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(2).
					WithGroupName("short-idle-group").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(workerTemplate))

			rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Wait for RayCluster to become ready and verify there is no worker replica.
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			// Run the test script. It should exit without error.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/do_not_remove_idles_for_pg.py"})
		})
	}
}

// This test verifies that the autoscaler can launch nodes to fulfill ray.autoscaler.sdk.request_resources from the user program.
func TestRayClusterAutoscalerSDKRequestResources(t *testing.T) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)

			// Create a namespace
			namespace := test.NewTestNamespace()

			// Mount the call_request_resources.py script as a ConfigMap
			scriptsAC := newConfigMap(namespace.Name, Files(test, "call_request_resources.py"))
			scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

			groupName := "request-group"

			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(2).
					WithGroupName(groupName).
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(tc.WorkerPodTemplateGetter()))
			rayClusterAC := rayv1ac.RayCluster("ray-cluster-sdk", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Wait for RayCluster to become ready
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			// Trigger resource request via ray.autoscaler.sdk.request_resources
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{
				"python", "/home/ray/test_scripts/call_request_resources.py", "--num-cpus=1",
			})

			// Autoscaler should create 1 workers
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.BeNumerically("==", 1)))
		})
	}
}

// This test verifies that a new worker node can be launched in a newly added worker group.
func TestRayClusterAutoscalerAddNewWorkerGroup(t *testing.T) {
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)

			// Create a namespace
			namespace := test.NewTestNamespace()

			// Mount the create_detached_actor.py and terminate_detached_actor.py scripts as a ConfigMap
			scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_actor.py", "terminate_detached_actor.py"))
			scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

			cpuGroup := "cpu-group"
			gpuGroup := "gpu-group"

			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithAutoscalerOptions(rayv1ac.AutoscalerOptions().
					WithIdleTimeoutSeconds(10)).
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(3).
					WithGroupName(cpuGroup).
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
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			headPod, err := GetHeadPod(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

			// Create a CPU-only detached actor, and a worker should be created.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "cpu_actor", "--num-cpus=1"})
			g.Eventually(GroupPods(test, rayCluster, cpuGroup), TestTimeoutMedium).Should(gomega.HaveLen(1))

			// Update the CPU worker group to have 1 replica to avoid overwriting the existing worker group
			// when adding the new GPU worker group.
			//
			// TODO(kevin85421): Autoscaler V2 will get stuck forever if the CPU worker group's replicas are overwritten
			// from 1 to 0. Ideally, Autoscaler V2 should still work no matter whether `Replicas` is overwritten or not.
			//
			// (1) Create a CPU detached actor
			// (2) Autoscaler updates the CPU worker group's `Replicas` to 1
			// (3) Add a GPU worker group and overwrite the CPU worker group's `Replicas` to 0
			// (4) At this point, the CPU worker group's `Replicas` is 0 in CR, but the CPU worker Pod still exists
			// (5) Create a GPU detached actor, and the GPU worker group's `Replicas` will be updated to 1
			// (6) Create a CPU detached actor, and the CPU worker group's `Replicas` will be updated to 1 instead of 2
			rayClusterAC.Spec.WorkerGroupSpecs[0].WithReplicas(1)

			// Add a GPU worker group
			rayClusterAC.Spec.WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
				WithReplicas(0).
				WithMinReplicas(0).
				WithMaxReplicas(3).
				WithGroupName(gpuGroup).
				WithRayStartParams(map[string]string{"num-cpus": "0", "num-gpus": "1"}).
				WithTemplate(tc.WorkerPodTemplateGetter()))

			rayCluster, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Updated RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Create a GPU-only detached actor, and a worker should be created.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "gpu_actor", "--num-gpus=1", "--num-cpus=0"})
			g.Eventually(GroupPods(test, rayCluster, gpuGroup), TestTimeoutMedium).Should(gomega.HaveLen(1))

			// Create a CPU-only detached actor, and a worker should be created.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "cpu_actor_2", "--num-cpus=1"})
			g.Eventually(GroupPods(test, rayCluster, cpuGroup), TestTimeoutMedium).Should(gomega.HaveLen(2))

			// Terminate the GPU-only detached actor, and a worker should be deleted.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "gpu_actor"})
			g.Eventually(GroupPods(test, rayCluster, gpuGroup), TestTimeoutMedium).Should(gomega.BeEmpty())
		})
	}
}

func TestRayClusterAutoscalerPlacementGroup(t *testing.T) {
	for _, tc := range tests {
		for _, setting := range []struct {
			workerGroupRayStartParams   map[string]string
			createPlacementGroupCMD     []string
			expectedWorkerGroupReplicas int
			workerGroupMaxReplicas      int32
		}{
			{
				workerGroupRayStartParams:   map[string]string{"num-cpus": "2"},
				workerGroupMaxReplicas:      3,
				createPlacementGroupCMD:     []string{"python", "/home/ray/test_scripts/create_detached_placement_group.py", "--num-cpus-per-bundle=1", "--num-bundles=2", "--strategy=STRICT_PACK"},
				expectedWorkerGroupReplicas: 1,
			},
			{
				workerGroupRayStartParams:   map[string]string{"num-cpus": "2"},
				workerGroupMaxReplicas:      3,
				createPlacementGroupCMD:     []string{"python", "/home/ray/test_scripts/create_detached_placement_group.py", "--num-cpus-per-bundle=1", "--num-bundles=2", "--strategy=PACK"},
				expectedWorkerGroupReplicas: 1,
			},
			{
				workerGroupRayStartParams:   map[string]string{"num-cpus": "2"},
				workerGroupMaxReplicas:      3,
				createPlacementGroupCMD:     []string{"python", "/home/ray/test_scripts/create_detached_placement_group.py", "--num-cpus-per-bundle=1", "--num-bundles=2", "--strategy=STRICT_SPREAD"},
				expectedWorkerGroupReplicas: 2,
			},
			{
				workerGroupRayStartParams:   map[string]string{"num-cpus": "1"},
				workerGroupMaxReplicas:      3,
				createPlacementGroupCMD:     []string{"python", "/home/ray/test_scripts/create_detached_placement_group.py", "--num-cpus-per-bundle=1", "--num-bundles=2", "--strategy=SPREAD"},
				expectedWorkerGroupReplicas: 2,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				test := With(t)
				g := gomega.NewWithT(t)

				// Create a namespace
				namespace := test.NewTestNamespace()

				// Mount the scripts as a ConfigMap
				scriptsAC := newConfigMap(namespace.Name, Files(test, "create_detached_placement_group.py", "check_placement_group_ready.py"))
				scripts, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), scriptsAC, TestApplyOptions)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", scripts.Namespace, scripts.Name)

				rayClusterSpecAC := rayv1ac.RayClusterSpec().
					WithEnableInTreeAutoscaling(true).
					WithAutoscalerOptions(rayv1ac.AutoscalerOptions().
						WithIdleTimeoutSeconds(10)).
					WithRayVersion(GetRayVersion()).
					WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
						WithRayStartParams(map[string]string{"num-cpus": "0"}).
						WithTemplate(tc.HeadPodTemplateGetter())).
					WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
						WithReplicas(0).
						WithMinReplicas(0).
						WithMaxReplicas(setting.workerGroupMaxReplicas).
						WithGroupName("cpu-group").
						WithRayStartParams(setting.workerGroupRayStartParams).
						WithTemplate(tc.WorkerPodTemplateGetter()))
				rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
					WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

				rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

				// Wait for RayCluster to become ready
				g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
					Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
				g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

				headPod, err := GetHeadPod(test, rayCluster)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)

				// Create a detached placement group, and workers should be created.
				ExecPodCmd(test, headPod, common.RayHeadContainer, setting.createPlacementGroupCMD)
				g.Eventually(GroupPods(test, rayCluster, "cpu-group"), TestTimeoutMedium).Should(gomega.HaveLen(setting.expectedWorkerGroupReplicas))

				// check if the placement group is ready.
				ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/check_placement_group_ready.py"})

				// Delete those workers.
				oldPods, err := GetGroupPods(test, rayCluster, "cpu-group")
				g.Expect(err).NotTo(gomega.HaveOccurred())
				for _, pod := range oldPods {
					err := test.Client().Core().CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{})
					g.Expect(err).NotTo(gomega.HaveOccurred())
				}

				// Wait until all old pods are deleted and an equal number of new worker pods are created.
				g.Eventually(GroupPods(test, rayCluster, "cpu-group"), TestTimeoutMedium).Should(
					gomega.WithTransform(func(latestPods []corev1.Pod) []corev1.Pod {
						return slices.DeleteFunc(latestPods, func(pod corev1.Pod) bool {
							for _, old := range oldPods {
								if pod.Name == old.Name {
									return true
								}
							}
							return false
						})
					}, gomega.HaveLen(setting.expectedWorkerGroupReplicas)))

				// check if the placement group is ready again.
				ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/check_placement_group_ready.py"})
			})
		}
	}
}

func TestRayClusterAutoscalerGCSFT(t *testing.T) {
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

			checkRedisDBSize := DeployRedis(test, namespace.Name, RedisPassword)
			defer g.Eventually(checkRedisDBSize, time.Second*60, time.Second).Should(gomega.BeEquivalentTo("0"))

			rayClusterSpecAC := rayv1ac.RayClusterSpec().
				WithEnableInTreeAutoscaling(true).
				WithGcsFaultToleranceOptions(
					rayv1ac.GcsFaultToleranceOptions().
						WithRedisAddress(RedisAddress).
						WithRedisPassword(rayv1ac.RedisCredential().WithValue(RedisPassword)),
				).
				WithRayVersion(GetRayVersion()).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{
						"num-cpus": "0",
					}).
					WithTemplate(tc.HeadPodTemplateGetter()),
				).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithRayStartParams(map[string]string{
						"num-cpus": "1",
					}).
					WithGroupName("small-group").
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(2).
					WithTemplate(tc.WorkerPodTemplateGetter()),
				)
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

			// Terminate a detached actor, and a worker should be deleted.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "actor1"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			// Delete the head Pod and wait for the new head pod to be ready.
			newHeadPod, err := DeletePodAndWait(test, rayCluster, namespace, headPod)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			headPod = newHeadPod

			// Create a detached actor, and a worker should be created after the new head pod is ready.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/create_detached_actor.py", "actor1"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(1))))

			// Terminate a detached actor, and a worker should be deleted.
			ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"python", "/home/ray/test_scripts/terminate_detached_actor.py", "actor1"})
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
			g.Expect(err).NotTo(gomega.HaveOccurred())
		})
	}
}

func TestRayClusterAutoscalerUpscalingModeConservative(t *testing.T) {
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
				WithAutoscalerOptions(rayv1ac.AutoscalerOptions().
					WithUpscalingMode("Conservative")).
				WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
					WithRayStartParams(map[string]string{"num-cpus": "0"}).
					WithTemplate(tc.HeadPodTemplateGetter())).
				WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
					WithReplicas(0).
					WithMinReplicas(0).
					WithMaxReplicas(10).
					WithGroupName(groupName).
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(tc.WorkerPodTemplateGetter()))
			rayClusterAC := rayv1ac.RayCluster("ray-cluster", namespace.Name).
				WithSpec(Apply(rayClusterSpecAC, MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](scripts, "/home/ray/test_scripts")))

			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			// Wait for the RayCluster to become ready
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			g.Expect(GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)).To(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(0))))

			// Verify the Autoscaler's CRD patching behavior while scaling up 10 workers.
			// We create all 10 actors in a single Python script invocation (--count=10) rather
			// than asynchronous background bash jobs so that demand reaches 10 immediately
			// without any partial demand intermediate state.
			// In Default mode, replicas would jump straight to 10 to meet the demand.
			// In Conservative mode, the initial batch is capped at max(5, num_running).
			g.Expect(RateLimitedReplicas(test, rayCluster, 5, func() {
				headPod, err := GetHeadPod(test, rayCluster)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Found head pod %s/%s", headPod.Namespace, headPod.Name)
				ExecPodCmd(test, headPod, common.RayHeadContainer, []string{
					"python", "/home/ray/test_scripts/create_detached_actor.py", "actor", "--count=10",
				})
			})).To(gomega.BeTrue(), "Conservative mode should cap the initial scale-up to 5")

			// Eventually, all worker Pods should connect to the RayCluster.
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
				Should(gomega.WithTransform(RayClusterDesiredWorkerReplicas, gomega.Equal(int32(10))))
			g.Eventually(GroupPods(test, rayCluster, groupName), TestTimeoutLong).Should(gomega.HaveLen(10))
		})
	}
}

const (
	// Idle timeout should exceeds the durantion of Ready + job submit + ray.init() for the driver case.
	idleTimeoutSeconds      int32 = 30
	gcsFTIdleTimeoutSeconds int32 = 90
	// TODO(justinyeh1995): change it to rayproject/ray:2.60.0 once released (includes
	// https://github.com/ray-project/ray/pull/65763)
	idleTerminationRayImage = "rayproject/ray:nightly.260926.5b14d6"
)

func TestRayClusterIdleTerminationSuspendPolicy(t *testing.T) {
	testCases := []struct {
		name       string
		withDriver bool
	}{
		{name: "without a driver attached", withDriver: false},
		{name: "with a driver attached", withDriver: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			test := With(t)
			g := gomega.NewWithT(t)
			namespace := test.NewTestNamespace()

			spec := newIdleTerminationSpec(test, rayv1.IdleTerminationPolicySuspend, idleTimeoutSeconds)
			rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).
				Apply(test.Ctx(), rayv1ac.RayCluster("raycluster-idle-suspend", namespace.Name).WithSpec(spec), TestApplyOptions)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
				Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
			LogWithTimestamp(test.T(), "RayCluster %s/%s is ready", rayCluster.Namespace, rayCluster.Name)

			if tc.withDriver {
				headPod, err := GetHeadPod(test, rayCluster)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				LogWithTimestamp(test.T(), "Found head Pod %s/%s", headPod.Namespace, headPod.Name)

				sleepSeconds := int32(60)
				// Covers "driver attached" (sleepSeconds) plus "idle timer restarted after driver exit" (idleTimeoutSeconds).
				noIdleSuspendWindow := idleTimeoutSeconds + sleepSeconds
				jobSubmitCmd := fmt.Sprintf(
					`ray job submit --address http://localhost:8265 --submission-id hold-driver --no-wait -- python -c 'import ray, time; ray.init(); time.sleep(%d)'`,
					sleepSeconds)
				ExecPodCmd(test, headPod, common.RayHeadContainer, []string{"bash", "-c", jobSubmitCmd})

				jobStatus := func() string {
					out, _ := ExecPodCmd(test, headPod, common.RayHeadContainer,
						[]string{"bash", "-c", "ray job status --address http://localhost:8265 hold-driver"}, true)
					return strings.ToLower(out.String())
				}
				g.Eventually(jobStatus, TestTimeoutShort).Should(gomega.ContainSubstring("running"))
				LogWithTimestamp(test.T(), "Ray job hold-driver is RUNNING; RayCluster %s/%s should not be idle-suspended while it is attached", rayCluster.Namespace, rayCluster.Name)

				isIdleSuspend := func(c *rayv1.RayCluster) bool { return c.Spec.IdleSuspend != nil && *c.Spec.IdleSuspend }
				g.Consistently(RayCluster(test, rayCluster.Namespace, rayCluster.Name),
					time.Duration(noIdleSuspendWindow)*time.Second, time.Second).
					Should(gomega.WithTransform(isIdleSuspend, gomega.BeFalse()))

				g.Eventually(jobStatus, TestTimeoutMedium).Should(gomega.ContainSubstring("succeeded"))
				LogWithTimestamp(test.T(), "Ray job hold-driver SUCCEEDED; waiting for RayCluster %s/%s to be idle-suspended", rayCluster.Namespace, rayCluster.Name)
			}

			// The autoscaler flips spec.idleSuspend; the operator then walks
			// RayClusterSuspending -> RayClusterSuspended with reason RayClusterIdleSuspended.
			g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
				Should(gomega.WithTransform(StatusCondition(rayv1.RayClusterSuspended),
					MatchCondition(metav1.ConditionTrue, rayv1.RayClusterIdleSuspended)))
			LogWithTimestamp(test.T(), "RayCluster %s/%s is idle-suspended", rayCluster.Namespace, rayCluster.Name)

			// All Pods must be gone
			allPods, err := GetAllPods(test, rayCluster)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(allPods).To(gomega.BeEmpty())
			LogWithTimestamp(test.T(), "All Pods of RayCluster %s/%s are deleted", rayCluster.Namespace, rayCluster.Name)
		})
	}
}

func TestRayClusterIdleTerminationDeletePolicy(t *testing.T) {
	test := With(t)
	g := gomega.NewWithT(t)
	namespace := test.NewTestNamespace()

	spec := newIdleTerminationSpec(test, rayv1.IdleTerminationPolicyDelete, idleTimeoutSeconds)
	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).
		Apply(test.Ctx(), rayv1ac.RayCluster("raycluster-idle-delete", namespace.Name).WithSpec(spec), TestApplyOptions)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to be deleted", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(func(gg gomega.Gomega) {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		gg.Expect(k8serrors.IsNotFound(err)).To(gomega.BeTrue())
	}, TestTimeoutLong).Should(gomega.Succeed())
	LogWithTimestamp(test.T(), "RayCluster %s/%s is deleted", rayCluster.Namespace, rayCluster.Name)

	g.Eventually(func() ([]string, error) { return eventReasons(test, namespace.Name, rayCluster.Name) }, TestTimeoutShort).
		Should(gomega.ContainElement(string(utils.DeletedIdleRayCluster)))
	LogWithTimestamp(test.T(), "Event %s is recorded for RayCluster %s/%s", utils.DeletedIdleRayCluster, rayCluster.Namespace, rayCluster.Name)
}

func TestRayClusterIdleTerminationDeletePolicyWithGCSFaultTolerance(t *testing.T) {
	test := With(t)
	g := gomega.NewWithT(t)
	namespace := test.NewTestNamespace()

	// The Redis cleanup Job must empty the storage namespace once the RayCluster is gone.
	checkRedisDBSize := DeployRedis(test, namespace.Name, RedisPassword)
	defer g.Eventually(checkRedisDBSize, TestTimeoutShort, time.Second).Should(gomega.BeEquivalentTo("0"))

	spec := newIdleTerminationSpec(test, rayv1.IdleTerminationPolicyDelete, gcsFTIdleTimeoutSeconds).
		WithGcsFaultToleranceOptions(rayv1ac.GcsFaultToleranceOptions().
			WithRedisAddress(RedisAddress).
			WithRedisPassword(rayv1ac.RedisCredential().WithValue(RedisPassword)))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).
		Apply(test.Ctx(), rayv1ac.RayCluster("raycluster-idle-delete-gcsft", namespace.Name).WithSpec(spec), TestApplyOptions)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutMedium).
		Should(gomega.WithTransform(RayClusterState, gomega.Equal(rayv1.Ready)))
	LogWithTimestamp(test.T(), "RayCluster %s/%s is ready", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to be deleted", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(func(gg gomega.Gomega) {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		gg.Expect(k8serrors.IsNotFound(err)).To(gomega.BeTrue())
	}, TestTimeoutLong).Should(gomega.Succeed())

	LogWithTimestamp(test.T(), "RayCluster %s/%s is deleted", rayCluster.Namespace, rayCluster.Name)

	// Both events show the idle block and the GCS FT block ran for the same deletion.
	g.Eventually(func() ([]string, error) { return eventReasons(test, namespace.Name, rayCluster.Name) }, TestTimeoutShort).
		Should(gomega.ContainElements(string(utils.DeletedIdleRayCluster), string(utils.CreatedRedisCleanupJob)))
	LogWithTimestamp(test.T(), "Events %s and %s are recorded for RayCluster %s/%s", utils.DeletedIdleRayCluster, utils.CreatedRedisCleanupJob, rayCluster.Namespace, rayCluster.Name)
}

// newIdleTerminationSpec builds an autoscaler-v2 RayCluster with spec.idleTerminationOptions
func newIdleTerminationSpec(test Test, policy rayv1.IdleTerminationPolicy, timeoutSeconds int32) *rayv1ac.RayClusterSpecApplyConfiguration {
	test.T().Helper()
	// The idle termination feature is only supported by the V2 Autoscaler.
	tc := tests[1]
	headTemplate := tc.HeadPodTemplateGetter()
	headTemplate.Spec.Containers[0].WithImage(idleTerminationRayImage)
	workerTemplate := tc.WorkerPodTemplateGetter()
	workerTemplate.Spec.Containers[0].WithImage(idleTerminationRayImage)

	return rayv1ac.RayClusterSpec().
		WithEnableInTreeAutoscaling(true).
		WithRayVersion("2.60.0").
		WithIdleTerminationOptions(rayv1ac.IdleTerminationOptions().
			WithTimeoutSeconds(timeoutSeconds).
			WithPolicy(policy)).
		WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
			WithRayStartParams(map[string]string{"num-cpus": "0"}).
			WithTemplate(headTemplate)).
		WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
			WithGroupName("small-group").
			WithReplicas(0).
			WithMinReplicas(0).
			WithMaxReplicas(1).
			WithRayStartParams(map[string]string{"num-cpus": "1"}).
			WithTemplate(workerTemplate))
}

// eventReasons returns the Reason of every Event whose subject is regardingName in namespace.
func eventReasons(test Test, namespace, regardingName string) ([]string, error) {
	test.T().Helper()
	events, err := test.Client().Core().EventsV1().Events(namespace).List(test.Ctx(), metav1.ListOptions{
		FieldSelector: fmt.Sprintf("regarding.name=%s", regardingName),
	})
	if err != nil {
		return nil, err
	}
	reasons := make([]string, 0, len(events.Items))
	for _, e := range events.Items {
		reasons = append(reasons, e.Reason)
	}
	return reasons, nil
}
