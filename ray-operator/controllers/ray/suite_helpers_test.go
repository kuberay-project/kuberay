package ray

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
)

func getResourceFunc(ctx context.Context, key client.ObjectKey, obj client.Object) func() error {
	return func() error {
		return k8sClient.Get(ctx, key, obj)
	}
}

func listResourceFunc(ctx context.Context, workerPods *corev1.PodList, opt ...client.ListOption) func() (int, error) {
	return func() (int, error) {
		if err := k8sClient.List(ctx, workerPods, opt...); err != nil {
			return -1, err
		}

		count := 0
		for _, aPod := range workerPods.Items {
			if (reflect.DeepEqual(aPod.Status.Phase, corev1.PodRunning) || reflect.DeepEqual(aPod.Status.Phase, corev1.PodPending)) && (aPod.DeletionTimestamp == nil || len(aPod.Finalizers) != 0) {
				count++
			}
		}

		return count, nil
	}
}

func getClusterState(ctx context.Context, namespace string, clusterName string) func() rayv1.ClusterState {
	return func() rayv1.ClusterState {
		var cluster rayv1.RayCluster
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: clusterName}, &cluster); err != nil {
			log.Fatal(err)
		}
		return cluster.Status.State
	}
}

func getClusterStatus(ctx context.Context, namespace string, clusterName string) func() rayv1.RayClusterStatus {
	return func() rayv1.RayClusterStatus {
		var cluster rayv1.RayCluster
		if err := k8sClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: clusterName}, &cluster); err != nil {
			log.Fatal(err)
		}
		return cluster.Status
	}
}

func isAllPodsRunningByFilters(ctx context.Context, podlist corev1.PodList, opt []client.ListOption) bool {
	err := k8sClient.List(ctx, &podlist, opt...)
	gomega.Expect(err).ShouldNot(gomega.HaveOccurred(), "failed to list Pods")
	for _, pod := range podlist.Items {
		if pod.Status.Phase != corev1.PodRunning {
			return false
		}
	}
	return true
}

func cleanUpWorkersToDelete(ctx context.Context, rayCluster *rayv1.RayCluster) {
	// Updating WorkersToDelete is the responsibility of the Ray Autoscaler. In this function,
	// we simulate the behavior of the Ray Autoscaler after the scaling process has finished.
	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		gomega.Eventually(
			getResourceFunc(ctx, client.ObjectKey{Name: rayCluster.Name, Namespace: "default"}, rayCluster),
			time.Second*9, time.Millisecond*500).Should(gomega.Succeed(), "raycluster = %v", rayCluster)
		rayCluster.Spec.WorkerGroupSpecs[0].ScaleStrategy.WorkersToDelete = []string{}
		return k8sClient.Update(ctx, rayCluster)
	})
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to clean up WorkersToDelete")
}

func getRayJobDeploymentStatus(ctx context.Context, rayJob *rayv1.RayJob) func() (rayv1.JobDeploymentStatus, error) {
	return func() (rayv1.JobDeploymentStatus, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: rayJob.Namespace}, rayJob); err != nil {
			return "", err
		}
		return rayJob.Status.JobDeploymentStatus, nil
	}
}

func getRayJobSucceededStatus(ctx context.Context, rayJob *rayv1.RayJob) func() (int32, error) {
	return func() (int32, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: rayJob.Namespace}, rayJob); err != nil {
			return 0, err
		}
		return *rayJob.Status.Succeeded, nil
	}
}

func getRayJobFailedStatus(ctx context.Context, rayJob *rayv1.RayJob) func() (int32, error) {
	return func() (int32, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: "default"}, rayJob); err != nil {
			return 0, err
		}
		return *rayJob.Status.Failed, nil
	}
}

func getRayClusterNameForRayJob(ctx context.Context, rayJob *rayv1.RayJob) func() (string, error) {
	return func() (string, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: "default"}, rayJob); err != nil {
			return "", err
		}
		return rayJob.Status.RayClusterName, nil
	}
}

func getDashboardURLForRayJob(ctx context.Context, rayJob *rayv1.RayJob) func() (string, error) {
	return func() (string, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: "default"}, rayJob); err != nil {
			return "", err
		}
		return rayJob.Status.DashboardURL, nil
	}
}

func prepareFakeRayDashboardClient() *utils.FakeRayDashboardClient {
	client := &utils.FakeRayDashboardClient{}

	healthyStatus := generateServeStatus(rayv1.DeploymentStatusEnum.HEALTHY, rayv1.ApplicationStatusEnum.RUNNING)
	client.SetMultiApplicationStatuses(map[string]*utiltypes.ServeApplicationStatus{"app": &healthyStatus})

	return client
}

func generateServeStatus(deploymentStatus string, applicationStatus string) utiltypes.ServeApplicationStatus {
	return utiltypes.ServeApplicationStatus{
		Status: applicationStatus,
		Deployments: map[string]utiltypes.ServeDeploymentStatus{
			"shallow": {
				Name:    "shallow",
				Status:  deploymentStatus,
				Message: "",
			},
			"deep": {
				Name:    "deep",
				Status:  deploymentStatus,
				Message: "",
			},
			"one": {
				Name:    "one",
				Status:  deploymentStatus,
				Message: "",
			},
		},
	}
}

func getRayClusterNameFunc(ctx context.Context, rayService *rayv1.RayService) func() (string, error) {
	return func() (string, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Name, Namespace: "default"}, rayService); err != nil {
			return "", err
		}
		return rayService.Status.ActiveServiceStatus.RayClusterName, nil
	}
}

func getActiveRayClusterWorkerGroupSpecsFunc(ctx context.Context, rayService *rayv1.RayService) func() ([]rayv1.WorkerGroupSpec, error) {
	return func() ([]rayv1.WorkerGroupSpec, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Name, Namespace: "default"}, rayService); err != nil {
			return nil, err
		}
		rayCluster := &rayv1.RayCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Status.ActiveServiceStatus.RayClusterName, Namespace: "default"}, rayCluster); err != nil {
			return nil, err
		}
		return rayCluster.Spec.WorkerGroupSpecs, nil
	}
}

func getPreparingRayClusterNameFunc(ctx context.Context, rayService *rayv1.RayService) func() (string, error) {
	return func() (string, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Name, Namespace: "default"}, rayService); err != nil {
			return "", err
		}
		return rayService.Status.PendingServiceStatus.RayClusterName, nil
	}
}

func getPendingRayClusterWorkerGroupSpecsFunc(ctx context.Context, rayService *rayv1.RayService) func() ([]rayv1.WorkerGroupSpec, error) {
	return func() ([]rayv1.WorkerGroupSpec, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Name, Namespace: "default"}, rayService); err != nil {
			return nil, err
		}
		rayCluster := &rayv1.RayCluster{}
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Status.PendingServiceStatus.RayClusterName, Namespace: "default"}, rayCluster); err != nil {
			return nil, err
		}
		return rayCluster.Spec.WorkerGroupSpecs, nil
	}
}

func checkServiceHealth(ctx context.Context, rayService *rayv1.RayService) func() (bool, error) {
	return func() (bool, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Name, Namespace: rayService.Namespace}, rayService); err != nil {
			return false, err
		}

		for _, appStatus := range rayService.Status.ActiveServiceStatus.Applications {
			if appStatus.Status != rayv1.ApplicationStatusEnum.RUNNING {
				return false, nil
			}
			for _, deploymentStatus := range appStatus.Deployments {
				if deploymentStatus.Status != rayv1.DeploymentStatusEnum.HEALTHY {
					return false, nil
				}
			}
		}

		return true, nil
	}
}

func checkServeApplicationExists(ctx context.Context, rayService *rayv1.RayService, serveAppName string) func() (bool, error) {
	return func() (bool, error) {
		if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Name, Namespace: rayService.Namespace}, rayService); err != nil {
			return false, err
		}
		for appName := range rayService.Status.ActiveServiceStatus.Applications {
			fmt.Println("checkServeApplicationExists: appName", appName)
			if appName == serveAppName {
				return true, nil
			}
		}
		return false, nil
	}
}

// Update the status of the head Pod to Running.
// We need to manually update Pod statuses otherwise they'll always be Pending.
// envtest doesn't create a full K8s cluster. It's only the control plane.
// There's no container runtime or any other K8s controllers.
// So Pods are created, but no controller updates them from Pending to Running.
// See https://book.kubebuilder.io/reference/envtest.html for more details.
func updateHeadPodToRunningAndReady(ctx context.Context, rayClusterName string, namespace string) {
	updateHeadPodToPhaseAndConditions(ctx, rayClusterName, namespace, corev1.PodRunning, []corev1.PodCondition{
		{
			Type:   corev1.PodReady,
			Status: corev1.ConditionTrue,
		},
	})
}

func updateHeadPodToRunningNotReady(ctx context.Context, rayClusterName string, namespace string) {
	updateHeadPodToPhaseAndConditions(ctx, rayClusterName, namespace, corev1.PodRunning, []corev1.PodCondition{
		{
			Type:   corev1.PodScheduled,
			Status: corev1.ConditionTrue,
		},
		{
			Type:   corev1.PodInitialized,
			Status: corev1.ConditionTrue,
		},
		{
			Type:   corev1.PodReady,
			Status: corev1.ConditionFalse,
		},
		{
			Type:   corev1.ContainersReady,
			Status: corev1.ConditionFalse,
		},
	})
}

func updateHeadPodToPhaseAndConditions(ctx context.Context, rayClusterName string, namespace string, phase corev1.PodPhase, conditions []corev1.PodCondition) {
	var instance rayv1.RayCluster
	gomega.Eventually(
		getResourceFunc(ctx, client.ObjectKey{Name: rayClusterName, Namespace: namespace}, &instance),
		time.Second*3, time.Millisecond*500).Should(gomega.Succeed(), "RayCluster %v not found", rayClusterName)

	headPods := corev1.PodList{}
	headLabels := common.RayClusterHeadPodsAssociationOptions(&instance).ToListOptions()

	gomega.Eventually(
		listResourceFunc(ctx, &headPods, headLabels...),
		time.Second*3, time.Millisecond*500).Should(gomega.Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

	headPod := headPods.Items[0]
	headPod.Status.Phase = phase
	headPod.Status.Conditions = conditions
	err := k8sClient.Status().Update(ctx, &headPod)
	gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to update head Pod status to not ready")
}

// Update the status of the worker Pods to Running and Ready. Similar to updateHeadPodToRunningAndReady.
func updateWorkerPodsToRunningAndReady(ctx context.Context, rayClusterName string, namespace string) {
	rayCluster := &rayv1.RayCluster{}
	gomega.Eventually(
		getResourceFunc(ctx, client.ObjectKey{Name: rayClusterName, Namespace: namespace}, rayCluster),
		time.Second*3, time.Millisecond*500).Should(gomega.Succeed(), "RayCluster %v not found", rayClusterName)

	workerPods := corev1.PodList{}
	workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
	numWorkerPods := int(*rayCluster.Spec.WorkerGroupSpecs[0].Replicas)

	gomega.Eventually(
		listResourceFunc(ctx, &workerPods, workerLabels...),
		time.Second*3, time.Millisecond*500).Should(gomega.Equal(numWorkerPods), "workerGroup: %v", workerPods.Items)

	for _, pod := range workerPods.Items {
		pod.Status.Phase = corev1.PodRunning
		pod.Status.Conditions = []corev1.PodCondition{
			{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			},
		}
		err := k8sClient.Status().Update(ctx, &pod)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to update worker Pod status to PodRunning")
	}

	// Make sure all worker Pods are updated.
	gomega.Eventually(
		isAllPodsRunningByFilters).WithContext(ctx).WithArguments(workerPods, workerLabels).WithTimeout(time.Second*3).WithPolling(time.Millisecond*500).Should(gomega.BeTrue(), "Worker Pods should be running: %v", workerPods.Items)
}

func updateRayJobSuspendField(ctx context.Context, rayJob *rayv1.RayJob, suspend bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: rayJob.Name}, rayJob)
		if err != nil {
			return err
		}
		rayJob.Spec.Suspend = suspend
		return k8sClient.Update(ctx, rayJob)
	})
}

func setJobIdOnRayJob(ctx context.Context, rayJob *rayv1.RayJob, jobId string) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		err := k8sClient.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: rayJob.Name}, rayJob)
		if err != nil {
			return err
		}
		rayJob.Spec.JobId = jobId
		return k8sClient.Update(ctx, rayJob)
	})
}
