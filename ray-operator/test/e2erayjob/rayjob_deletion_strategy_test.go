package e2erayjob

import (
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const activeDeadlineAfterRunningSeconds int32 = 1

func TestDeletionStrategy(t *testing.T) {
	// Note: This test suite requires the RayJobDeletionPolicy feature gate to be enabled
	tests := []struct {
		testFunc func(Test, *WithT, *corev1.Namespace, *corev1.ConfigMap)
		name     string
	}{
		{
			name:     "DeletionRules with DeleteWorkers policy should delete only worker pods",
			testFunc: testDeletionRulesWithDeleteWorkersPolicy,
		},
		{
			name:     "DeletionRules with DeleteCluster policy should delete entire cluster",
			testFunc: testDeletionRulesWithDeleteClusterPolicy,
		},
		{
			name:     "DeletionRules with DeleteSelf policy should delete RayJob and cluster",
			testFunc: testDeletionRulesWithDeleteSelfPolicy,
		},
		{
			name:     "DeletionRules with DeleteNone policy should preserve all resources",
			testFunc: testDeletionRulesWithDeleteNonePolicy,
		},
		{
			name:     "Multi-stage deletion should execute in TTL order: Workers->Cluster->Self",
			testFunc: testMultiStageDeletion,
		},
		{
			name:     "Legacy OnSuccess DeleteCluster should still work",
			testFunc: testLegacyOnSuccessDeleteCluster,
		},
		{
			name:     "DeletionRules with JobDeploymentStatus Failed and DeleteWorkers policy should delete only worker pods",
			testFunc: testDeletionRulesWithJobDeploymentStatusFailedAndDeleteWorkersPolicy,
		},
		{
			name:     "DeletionRules with JobDeploymentStatus Failed and DeleteCluster policy should delete entire cluster",
			testFunc: testDeletionRulesWithJobDeploymentStatusFailedAndDeleteClusterPolicy,
		},
		{
			name:     "DeletionRules with JobDeploymentStatus Failed and DeleteSelf policy should delete RayJob and cluster",
			testFunc: testDeletionRulesWithJobDeploymentStatusFailedAndDeleteSelfPolicy,
		},
		{
			name:     "DeletionRules with JobDeploymentStatus Failed and DeleteNone policy should preserve all resources",
			testFunc: testDeletionRulesWithJobDeploymentStatusFailedAndDeleteNonePolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)
			namespace := test.NewTestNamespace()

			cm := applyJobScriptConfigMap(test, g, namespace)
			tt.testFunc(test, g, namespace, cm)
		})
	}
}

func testDeletionRulesWithDeleteWorkersPolicy(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	// Create RayJob with DeleteWorkers policy and short TTL for faster testing
	rayJobAC := rayv1ac.RayJob("delete-workers-test", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/counter.py").
			WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
			WithShutdownAfterJobFinishes(false). // Required when using DeletionStrategy
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteWorkers).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobStatus(rayv1.JobStatusSucceeded).
							WithTTLSeconds(10)), // 10 second TTL for testing
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForCompletion(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name. We assert it's non-empty explicitly so that
	// test failures surface here (clear message) rather than later when using an empty name.
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Verify cluster and workers exist initially
	LogWithTimestamp(test.T(), "Verifying cluster and workers exist initially...")
	g.Eventually(RayCluster(test, namespace.Name, rayClusterName), TestTimeoutShort).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	// Count initial worker pods
	cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	initialWorkerPods, err := GetWorkerPods(test, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(initialWorkerPods).ToNot(BeEmpty())
	LogWithTimestamp(test.T(), "Found %d worker pods initially", len(initialWorkerPods))

	// Verify resources persist during TTL wait period (first 8 seconds of 10s TTL)
	LogWithTimestamp(test.T(), "Verifying resources persist during TTL wait period...")
	g.Consistently(func(gg Gomega) {
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		workerPods, err := GetWorkerPods(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(workerPods).ToNot(BeEmpty())
		headPod, err := GetHeadPod(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(headPod).NotTo(BeNil())
		jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(jobObj).NotTo(BeNil())
	}, 8*time.Second, 2*time.Second).Should(Succeed()) // Check every 2s for 8s
	LogWithTimestamp(test.T(), "Resources confirmed stable during TTL wait period")

	// Wait for TTL to expire and workers to be deleted
	LogWithTimestamp(test.T(), "Waiting for TTL to expire and workers to be deleted...")
	g.Eventually(func(gg Gomega) {
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		workerPods, err := GetWorkerPods(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(workerPods).To(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed())
	LogWithTimestamp(test.T(), "Worker pods deleted successfully")

	// Verify cluster still exists (head pod should remain)
	LogWithTimestamp(test.T(), "Verifying cluster still exists (head pod should remain)...")
	g.Consistently(RayCluster(test, namespace.Name, rayClusterName), 10*time.Second, 1*time.Second).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	// Verify head pod still exists
	cluster, err = GetRayCluster(test, namespace.Name, rayClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	headPod, err := GetHeadPod(test, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod).NotTo(BeNil())
	LogWithTimestamp(test.T(), "Head pod preserved as expected")

	// Verify RayJob still exists
	jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(jobObj).NotTo(BeNil())
	LogWithTimestamp(test.T(), "RayJob preserved as expected")
}

func testDeletionRulesWithDeleteClusterPolicy(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	rayJobAC := rayv1ac.RayJob("delete-cluster-test", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/counter.py").
			WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
			WithShutdownAfterJobFinishes(false).
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteCluster).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobStatus(rayv1.JobStatusSucceeded).
							WithTTLSeconds(10)),
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForCompletion(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name (early assertion for clearer diagnostics)
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Verify cluster exists initially
	LogWithTimestamp(test.T(), "Verifying cluster exists initially...")
	g.Eventually(RayCluster(test, namespace.Name, rayClusterName), TestTimeoutShort).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	// Wait for TTL to expire and cluster to be deleted
	LogWithTimestamp(test.T(), "Waiting for TTL to expire and cluster to be deleted...")
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayClusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "RayCluster deleted successfully")

	// Verify RayJob still exists
	jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(jobObj).NotTo(BeNil())
	LogWithTimestamp(test.T(), "RayJob preserved as expected")
}

func testDeletionRulesWithDeleteSelfPolicy(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	rayJobAC := rayv1ac.RayJob("delete-self-test", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/counter.py").
			WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
			WithShutdownAfterJobFinishes(false).
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteSelf).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobStatus(rayv1.JobStatusSucceeded).
							WithTTLSeconds(10)),
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForCompletion(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name before verifying deletion sequence
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Wait for TTL to expire and RayJob (and cluster) to be deleted
	LogWithTimestamp(test.T(), "Waiting for TTL to expire and RayJob to be deleted...")
	g.Eventually(func() error {
		_, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "RayJob deleted successfully")

	// Verify associated cluster is also deleted
	LogWithTimestamp(test.T(), "Verifying associated cluster is also deleted...")
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayClusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "Associated RayCluster deleted successfully")
}

func testDeletionRulesWithDeleteNonePolicy(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	rayJobAC := rayv1ac.RayJob("delete-none-test", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/counter.py").
			WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
			WithShutdownAfterJobFinishes(false).
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteNone).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobStatus(rayv1.JobStatusSucceeded).
							WithTTLSeconds(5)), // Shorter TTL since we're testing preservation
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForCompletion(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name (assert early for clarity)
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Wait well past the TTL and verify everything is preserved
	LogWithTimestamp(test.T(), "Waiting past TTL to verify resources are preserved...")
	g.Consistently(func(gg Gomega) {
		jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(jobObj).NotTo(BeNil())
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		workerPods, err := GetWorkerPods(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(workerPods).ToNot(BeEmpty())
	}, 10*time.Second, 2*time.Second).Should(Succeed())
	LogWithTimestamp(test.T(), "All resources preserved as expected with DeleteNone policy")
}

func testMultiStageDeletion(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	rayJobAC := rayv1ac.RayJob("multi-stage-test", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/counter.py").
			WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
			WithShutdownAfterJobFinishes(false).
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteWorkers).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobStatus(rayv1.JobStatusSucceeded).
							WithTTLSeconds(15)), // Increased spacing for reliability
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteCluster).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobStatus(rayv1.JobStatusSucceeded).
							WithTTLSeconds(35)), // 20s gap between stages
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteSelf).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobStatus(rayv1.JobStatusSucceeded).
							WithTTLSeconds(55)), // 20s gap between stages
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForCompletion(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name (early assertion ensures meaningful failure)
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Verify cluster is ready initially
	LogWithTimestamp(test.T(), "Verifying cluster is ready initially...")
	g.Eventually(RayCluster(test, namespace.Name, rayClusterName), TestTimeoutShort).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	// Verify all resources exist before any TTL expires (first 12 seconds)
	LogWithTimestamp(test.T(), "Verifying all resources persist before any TTL expires...")
	g.Consistently(func(gg Gomega) {
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		workerPods, err := GetWorkerPods(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(workerPods).ToNot(BeEmpty())
		headPod, err := GetHeadPod(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(headPod).NotTo(BeNil())
		jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(jobObj).NotTo(BeNil())
	}, 12*time.Second, 2*time.Second).Should(Succeed())
	LogWithTimestamp(test.T(), "All resources confirmed stable before TTL expiration")

	// Stage 1: Wait for workers to be deleted (15s TTL)
	LogWithTimestamp(test.T(), "Stage 1: Waiting for workers to be deleted at 15s...")
	g.Eventually(func(gg Gomega) {
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		workerPods, err := GetWorkerPods(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(workerPods).To(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed())
	LogWithTimestamp(test.T(), "Stage 1 complete: Workers deleted successfully")

	// Verify cluster and job still exist after stage 1
	job, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(job).NotTo(BeNil())
	cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	headPod, err := GetHeadPod(test, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod).NotTo(BeNil())

	// Verify cluster persists during stage 2 wait period (15 seconds of 20s gap)
	LogWithTimestamp(test.T(), "Verifying cluster persists before stage 2 TTL expires...")
	g.Consistently(func(gg Gomega) {
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		headPod, err := GetHeadPod(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(headPod).NotTo(BeNil())
		jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(jobObj).NotTo(BeNil())
	}, 15*time.Second, 2*time.Second).Should(Succeed())
	LogWithTimestamp(test.T(), "Cluster and job confirmed stable before stage 2 TTL")

	// Stage 2: Wait for cluster to be deleted (35s TTL)
	LogWithTimestamp(test.T(), "Stage 2: Waiting for cluster to be deleted at 35s...")
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayClusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "Stage 2 complete: Cluster deleted successfully")

	// Verify job still exists after stage 2
	job, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(job).NotTo(BeNil())

	// Verify job persists during stage 3 wait period (15 seconds of 20s gap)
	LogWithTimestamp(test.T(), "Verifying RayJob persists before stage 3 TTL expires...")
	g.Consistently(func(gg Gomega) {
		jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(jobObj).NotTo(BeNil())
	}, 15*time.Second, 2*time.Second).Should(Succeed())
	LogWithTimestamp(test.T(), "RayJob confirmed stable before stage 3 TTL")

	// Stage 3: Wait for job to be deleted (55s TTL)
	LogWithTimestamp(test.T(), "Stage 3: Waiting for RayJob to be deleted at 55s...")
	g.Eventually(func() error {
		_, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "Stage 3 complete: RayJob deleted successfully")
	LogWithTimestamp(test.T(), "Multi-stage deletion completed in correct order")
}

func testLegacyOnSuccessDeleteCluster(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	rayJobAC := rayv1ac.RayJob("legacy-success-test", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/counter.py").
			WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
			WithShutdownAfterJobFinishes(true).
			WithTTLSecondsAfterFinished(10). // Legacy TTL for backward compatibility
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithOnSuccess(rayv1ac.DeletionPolicy().
					WithPolicy(rayv1.DeleteCluster)).
				WithOnFailure(rayv1ac.DeletionPolicy().
					WithPolicy(rayv1.DeleteCluster))).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForCompletion(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name (legacy path; same early assertion rationale)
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Wait for cluster to be deleted due to OnSuccess policy
	LogWithTimestamp(test.T(), "Waiting for legacy OnSuccess policy to delete cluster...")
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayClusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "Cluster deleted by legacy OnSuccess policy")

	// Verify RayJob still exists
	job, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(job).NotTo(BeNil())
	LogWithTimestamp(test.T(), "Legacy OnSuccess policy working correctly")
}

func testDeletionRulesWithJobDeploymentStatusFailedAndDeleteWorkersPolicy(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	// Create a RayJob with DeleteWorkers policy and short TTL for faster testing.
	rayJobAC := rayv1ac.RayJob("delete-workers-after-jobdeploymentstatus-failed", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/long_running.py").
			WithShutdownAfterJobFinishes(false). // Required when using DeletionStrategy
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteWorkers).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobDeploymentStatus(rayv1.JobDeploymentStatusFailed).
							WithTTLSeconds(10)), // 10 second TTL for testing
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForJobDeploymentStatusFailed(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name. We assert it's non-empty explicitly so that
	// test failures surface here (clear message) rather than later when using an empty name.
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Verify cluster and workers exist initially.
	LogWithTimestamp(test.T(), "Verifying cluster and workers exist initially...")
	g.Eventually(RayCluster(test, namespace.Name, rayClusterName), TestTimeoutShort).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	// Count initial worker pods.
	cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	initialWorkerPods, err := GetWorkerPods(test, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(initialWorkerPods).ToNot(BeEmpty())
	LogWithTimestamp(test.T(), "Found %d worker pods initially", len(initialWorkerPods))

	// Verify resources persist during TTL wait period (first 8 seconds of 10s TTL).
	LogWithTimestamp(test.T(), "Verifying resources persist during TTL wait period...")
	g.Consistently(func(gg Gomega) {
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		workerPods, err := GetWorkerPods(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(workerPods).ToNot(BeEmpty())
		headPod, err := GetHeadPod(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(headPod).NotTo(BeNil())
		jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(jobObj).NotTo(BeNil())
	}, 8*time.Second, 2*time.Second).Should(Succeed()) // Check every 2s for 8s
	LogWithTimestamp(test.T(), "Resources confirmed stable during TTL wait period")

	// Wait for TTL to expire and workers to be deleted.
	LogWithTimestamp(test.T(), "Waiting for TTL to expire and workers to be deleted...")
	g.Eventually(func(gg Gomega) {
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		workerPods, err := GetWorkerPods(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(workerPods).To(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed())
	LogWithTimestamp(test.T(), "Worker pods deleted successfully")

	// Verify cluster still exists (head pod should remain).
	LogWithTimestamp(test.T(), "Verifying cluster still exists (head pod should remain)...")
	g.Consistently(RayCluster(test, namespace.Name, rayClusterName), 10*time.Second, 1*time.Second).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	// Verify head pod still exists.
	cluster, err = GetRayCluster(test, namespace.Name, rayClusterName)
	g.Expect(err).NotTo(HaveOccurred())
	headPod, err := GetHeadPod(test, cluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod).NotTo(BeNil())
	LogWithTimestamp(test.T(), "Head pod preserved as expected")

	// Verify RayJob still exists.
	jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(jobObj).NotTo(BeNil())
	LogWithTimestamp(test.T(), "RayJob preserved as expected")
}

func testDeletionRulesWithJobDeploymentStatusFailedAndDeleteClusterPolicy(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	// Create a RayJob with DeleteCluster policy and short TTL for faster testing.
	rayJobAC := rayv1ac.RayJob("delete-cluster-after-jobdeploymentstatus-failed", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/long_running.py").
			WithShutdownAfterJobFinishes(false). // Required when using DeletionStrategy
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteCluster).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobDeploymentStatus(rayv1.JobDeploymentStatusFailed).
							WithTTLSeconds(10)), // 10 second TTL for testing
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForJobDeploymentStatusFailed(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name (early assertion for clearer diagnostics).
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Verify cluster exists initially.
	LogWithTimestamp(test.T(), "Verifying cluster exists initially...")
	g.Eventually(RayCluster(test, namespace.Name, rayClusterName), TestTimeoutShort).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	// Wait for TTL to expire and cluster to be deleted.
	LogWithTimestamp(test.T(), "Waiting for TTL to expire and cluster to be deleted...")
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayClusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "RayCluster deleted successfully")

	// Verify RayJob still exists.
	jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(jobObj).NotTo(BeNil())
	LogWithTimestamp(test.T(), "RayJob preserved as expected")
}

func testDeletionRulesWithJobDeploymentStatusFailedAndDeleteSelfPolicy(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	// Create a RayJob with DeleteSelf policy and short TTL for faster testing.
	rayJobAC := rayv1ac.RayJob("delete-self-after-jobdeploymentstatus-failed", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/long_running.py").
			WithShutdownAfterJobFinishes(false). // Required when using DeletionStrategy
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteSelf).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobDeploymentStatus(rayv1.JobDeploymentStatusFailed).
							WithTTLSeconds(10)), // 10 second TTL for testing
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForJobDeploymentStatusFailed(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name (early assertion for clearer diagnostics).
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Wait for TTL to expire and RayJob (and cluster) to be deleted.
	LogWithTimestamp(test.T(), "Waiting for TTL to expire and RayJob to be deleted...")
	g.Eventually(func() error {
		_, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "RayJob deleted successfully")

	// Verify associated cluster is also deleted.
	LogWithTimestamp(test.T(), "Verifying associated cluster is also deleted...")
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayClusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	LogWithTimestamp(test.T(), "Associated RayCluster deleted successfully")
}

func testDeletionRulesWithJobDeploymentStatusFailedAndDeleteNonePolicy(test Test, g *WithT, namespace *corev1.Namespace, cm *corev1.ConfigMap) {
	// Create a RayJob with DeleteNone policy and short TTL for faster testing.
	rayJobAC := rayv1ac.RayJob("delete-none-after-jobdeploymentstatus-failed", namespace.Name).
		WithSpec(rayv1ac.RayJobSpec().
			WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](cm, "/home/ray/jobs"))).
			WithEntrypoint("python /home/ray/jobs/long_running.py").
			WithShutdownAfterJobFinishes(false). // Required when using DeletionStrategy
			WithDeletionStrategy(rayv1ac.DeletionStrategy().
				WithDeletionRules(
					rayv1ac.DeletionRule().
						WithPolicy(rayv1.DeleteNone).
						WithCondition(rayv1ac.DeletionCondition().
							WithJobDeploymentStatus(rayv1.JobDeploymentStatusFailed).
							WithTTLSeconds(10)), // 10 second TTL for testing
				)).
			WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))
	rayJob := applyRayJobAndWaitForJobDeploymentStatusFailed(test, g, namespace, rayJobAC)

	// Get the associated RayCluster name (early assertion for clearer diagnostics).
	rayJob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
	g.Expect(err).NotTo(HaveOccurred())
	rayClusterName := rayJob.Status.RayClusterName
	g.Expect(rayClusterName).NotTo(BeEmpty())

	// Wait well past the TTL and verify everything is preserved.
	LogWithTimestamp(test.T(), "Waiting past TTL to verify resources are preserved...")
	g.Consistently(func(gg Gomega) {
		jobObj, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(jobObj).NotTo(BeNil())
		cluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cluster).NotTo(BeNil())
		workerPods, err := GetWorkerPods(test, cluster)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(workerPods).ToNot(BeEmpty())
	}, 10*time.Second, 2*time.Second).Should(Succeed())
	LogWithTimestamp(test.T(), "All resources preserved as expected with DeleteNone policy")
}

// applyJobScriptConfigMap applies the job scripts to the config map. The usages of these scripts are summarized as follows:
// - counter.py: for successful jobs
// - fail.py: for failed jobs
// - long_running.py: for long running jobs
func applyJobScriptConfigMap(test Test, g *WithT, namespace *corev1.Namespace) *corev1.ConfigMap {
	jobsAC := NewConfigMap(namespace.Name, Files(test, "counter.py", "fail.py", "long_running.py"))
	cm, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), jobsAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", cm.Namespace, cm.Name)
	return cm
}

// applyRayJobAndWaitForCompletion applies the ray job and waits for it to complete successfully.
func applyRayJobAndWaitForCompletion(test Test, g *WithT, namespace *corev1.Namespace, rayJobAC *rayv1ac.RayJobApplyConfiguration) *rayv1.RayJob {
	rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

	LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete successfully", rayJob.Namespace, rayJob.Name)
	g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
		Should(SatisfyAll(
			WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)),
			WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)),
		))
	LogWithTimestamp(test.T(), "RayJob %s/%s completed successfully", rayJob.Namespace, rayJob.Name)

	return rayJob
}

// applyRayJobAndWaitForJobDeploymentStatusFailed applies the RayJob, waits until it is running, then
// sets a short activeDeadlineSeconds and waits for JobDeploymentStatus to become Failed due to timeout.
func applyRayJobAndWaitForJobDeploymentStatusFailed(test Test, g *WithT, namespace *corev1.Namespace, rayJobAC *rayv1ac.RayJobApplyConfiguration) *rayv1.RayJob {
	rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

	LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to become Running before setting activeDeadlineSeconds...", rayJob.Namespace, rayJob.Name)
	g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
		Should(SatisfyAll(
			WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusRunning)),
			WithTransform(RayJobStatus, Equal(rayv1.JobStatusRunning)),
		))

	LogWithTimestamp(test.T(), "Setting activeDeadlineSeconds=%d for RayJob %s/%s", activeDeadlineAfterRunningSeconds, rayJob.Namespace, rayJob.Name)
	rayJobAC.Spec.WithActiveDeadlineSeconds(activeDeadlineAfterRunningSeconds)
	rayJob, err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for JobDeploymentStatus to become Failed and Reason to be DeadlineExceeded...")
	g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
		Should(SatisfyAll(
			WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)),
			WithTransform(RayJobReason, Equal(rayv1.DeadlineExceeded)),
			WithTransform(RayJobStatus, Equal(rayv1.JobStatusRunning)),
		))

	return rayJob
}
