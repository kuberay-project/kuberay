package e2erayjob

import (
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJob(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	// Job scripts
	jobsAC := NewConfigMap(namespace.Name, Files(test, "counter.py", "fail.py", "stop.py", "long_running.py"))
	jobs, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), jobsAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", jobs.Namespace, jobs.Name)

	test.T().Run("Successful RayJob", func(_ *testing.T) {
		// RayJob
		rayJobAC := rayv1ac.RayJob("counter", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		rayJobHeadSvcName, err := utils.GenerateHeadServiceName(utils.RayJobCRD, *rayJob.Spec.RayClusterSpec, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred(), "Failed to get Head Service Name: %v", rayJobHeadSvcName)

		g.Eventually(func() error {
			_, err = test.Client().Core().CoreV1().Services(namespace.Name).Get(test.Ctx(), rayJobHeadSvcName, metav1.GetOptions{})
			return err
		}).Should(Succeed())

		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

		// Assert the RayJob has completed successfully
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))

		// And the RayJob deployment status is updated accordingly
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		// Refresh the RayJob status
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		LogWithTimestamp(test.T(), "Checking that the RayJob status info has been set correctly.")
		g.Expect(rayJob.Status.RayJobStatusInfo.StartTime).NotTo(BeNil())
		g.Expect(rayJob.Status.RayJobStatusInfo.EndTime).NotTo(BeNil())

		// Assert the RayCluster has been torn down
		g.Eventually(func() error {
			_, err = GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}, TestTimeoutShort).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

		// Assert the submitter Job has not been deleted
		g.Eventually(Jobs(test, namespace.Name)).ShouldNot(BeEmpty())

		// TODO (kevin85421): Check whether the Pods associated with the RayCluster and the submitter Job have been deleted.
		// For Kubernetes Jobs, the default deletion behavior is "orphanDependents," which means the Pods will not be
		// cascadingly deleted with the Kubernetes Job by default.

		LogWithTimestamp(test.T(), "Update `suspend` to true. However, since the RayJob is completed, the status should not be updated to `Suspended`.")
		rayJobAC.Spec.WithSuspend(true)
		rayJob, err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		g.Consistently(RayJob(test, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		// Delete the RayJob
		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	test.T().Run("Failing RayJob without cluster shutdown after finished", func(_ *testing.T) {
		// RayJob
		rayJobAC := rayv1ac.RayJob("fail", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/fail.py").
				WithShutdownAfterJobFinishes(false).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

		// Assert the Ray job has failed
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusFailed)))

		// Assert that the RayJob deployment status and RayJob reason have been updated accordingly.
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.AppFailed)))

		// TODO (kevin85421): Ensure the RayCluster and Kubernetes Job are not deleted because `ShutdownAfterJobFinishes` is false.

		// Refresh the RayJob status
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		// Delete the RayJob
		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Assert the RayCluster has been cascade deleted
		g.Eventually(func() error {
			_, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

		// Assert the submitter Job has been cascade deleted
		g.Eventually(Jobs(test, namespace.Name)).Should(BeEmpty())
	})

	test.T().Run("Failing submitter K8s Job", func(_ *testing.T) {
		// RayJob
		rayJobAC := rayv1ac.RayJob("fail-k8s-job", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("The command will be overridden by the submitter Job").
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

		// In this test, we try to simulate the case where the submitter Job can't connect to the RayCluster successfully.
		// Hence, KubeRay can't get the Ray job information from the RayCluster. When the submitter Job reaches the backoff
		// limit, it will be marked as failed. Then, the RayJob should transition to `Complete`.
		rayJobAC.Spec.SubmitterPodTemplate.Spec.Containers[0].WithCommand("ray", "job", "submit", "--address", "http://do-not-exist:8265", "--", "echo 123")

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusNew)))
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.SubmissionFailed)))

		// Refresh the RayJob status
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		// Assert the RayCluster has been deleted because ShutdownAfterJobFinishes is true.
		g.Eventually(func() error {
			_, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
		// Asset submitter Job is not deleted yet
		g.Eventually(Jobs(test, namespace.Name)).ShouldNot(BeEmpty())

		// Delete the RayJob
		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	test.T().Run("Should transition to 'Complete' if the Ray job has stopped.", func(_ *testing.T) {
		// `stop.py` will sleep for 20 seconds so that the RayJob has enough time to transition to `RUNNING`
		// and then stop the Ray job. If the Ray job is stopped, the RayJob should transition to `Complete`.
		rayJobAC := rayv1ac.RayJob("stop", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithEntrypoint("python /home/ray/jobs/stop.py").
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()).
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to be 'Running'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusRunning)))

		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to be 'Complete'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		// Refresh the RayJob status
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusStopped)))

		// Delete the RayJob
		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	test.T().Run("RuntimeEnvYAML is not a valid YAML string", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("invalid-yamlstr", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`invalid_yaml_string`).
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// `RuntimeEnvYAML` is not a valid YAML string, so the RayJob controller should set status to ValidationFailed.
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusValidationFailed)))
	})

	test.T().Run("RayJob name too long with 48 characters", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob(strings.Repeat("a", 48), namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Rayjob name is too long, so the RayJob controller should set status to ValidationFailed.
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusValidationFailed)))
	})

	test.T().Run("RayJob has passed ActiveDeadlineSeconds", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("long-running", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/long_running.py").
				WithShutdownAfterJobFinishes(true).
				WithTTLSecondsAfterFinished(600).
				WithActiveDeadlineSeconds(5).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// The RayJob will transition to `Complete` because it has passed `ActiveDeadlineSeconds`.
		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to be 'Complete'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.DeadlineExceeded)))
	})

	test.T().Run("RayJob controller recreates the head Pod if it is deleted while the job is running", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("delete-head-after-submit", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec()).
				WithEntrypoint("python -c \"import time; time.sleep(60)\"").
				// BackoffLimit=0 prevents the submitter Job from retrying after the head Pod is deleted.
				// Otherwise it may resubmit to the recreated (non GCS FT) head pod and non-deterministically succeed.
				WithSubmitterConfig(rayv1ac.SubmitterConfig().WithBackoffLimit(0)).
				WithShutdownAfterJobFinishes(true))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Wait until the RayJob's job status transitions to Running
		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to be 'Running'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Equal(rayv1.JobStatusRunning)))

		// Fetch RayCluster and delete the head Pod
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		rayCluster, err := GetRayCluster(test, rayJob.Namespace, rayJob.Status.RayClusterName)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rayCluster.Labels[utils.RayJobSubmissionModeLabelKey]).To(Equal(string(rayv1.K8sJobMode)))
		g.Expect(rayCluster.Annotations[utils.DisableProvisionedHeadRestartAnnotationKey]).To(Equal(""))
		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleting head Pod %s/%s for RayCluster %s", headPod.Namespace, headPod.Name, rayCluster.Name)
		err = test.Client().Core().CoreV1().Pods(headPod.Namespace).Delete(test.Ctx(), headPod.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		// Head pod should be recreated for non-sidecar modes.
		g.Eventually(func() (*corev1.Pod, error) {
			return GetHeadPod(test, rayCluster)
		}, TestTimeoutMedium, 2*time.Second).ShouldNot(BeNil())
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobReason, Or(
				Equal(rayv1.AppFailed),
				Equal(rayv1.JobDeploymentStatusTransitionGracePeriodExceeded),
				Equal(rayv1.SubmissionFailed),
			)))
		// Cleanup
		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	test.T().Run("RayJob should be created, but not updated when managed externally", func(_ *testing.T) {
		// RayJob
		rayJobAC := rayv1ac.RayJob("managed-externally", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()).
				WithManagedBy("kueue.x-k8s.io/multikueue"))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Should not to be able to change managedBy field as it's immutable
		rayJobAC.Spec.WithManagedBy(utils.KubeRayController)
		_, err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).To(HaveOccurred())
		g.Eventually(RayJob(test, *rayJobAC.Namespace, *rayJobAC.Name)).
			Should(WithTransform(RayJobManagedBy, Equal(new("kueue.x-k8s.io/multikueue"))))

		// Refresh the RayJob status and assert it has not been updated
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusNew)))

		// Assert the associated RayCluster has not beed created
		rcList, err := test.Client().Ray().RayV1().RayClusters(rayJob.Namespace).List(test.Ctx(), metav1.ListOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		for _, rc := range rcList.Items {
			g.Expect(rc.Name).NotTo(HaveSuffix(*rayJobAC.Name))
		}

		// Assert the submitter Job has not been created
		g.Eventually(Jobs(test, namespace.Name)).Should(BeEmpty())

		// Delete the RayJob
		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), *rayJobAC.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", *rayJobAC.Namespace, *rayJobAC.Name)
	})

	test.T().Run("RayJob survives the submitter finishing while the job is still running", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("submitter-timeout", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/long_running.py").
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Wait until RayJob status and deployment status is running
		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s deployment status to be 'Running'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusRunning)))
		LogWithTimestamp(test.T(), "Waiting for Ray job %s/%s to be actually running in Ray cluster", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Equal(rayv1.JobStatusRunning)))

		// Wait for the submitter job to be created
		LogWithTimestamp(test.T(), "Waiting for submitter job to be created")
		g.Eventually(Jobs(test, namespace.Name)).ShouldNot(BeEmpty())

		// Force the submitter job to complete by updating its status
		// This simulates the submitter finishing but the Ray job still running
		LogWithTimestamp(test.T(), "Updating submitter job status to complete")
		job, err := test.Client().Core().BatchV1().Jobs(namespace.Name).Get(test.Ctx(), rayJob.Name, metav1.GetOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		now := metav1.Now()
		job.Status.Active = 0
		job.Status.Ready = nil
		job.Status.Conditions = append(job.Status.Conditions,
			batchv1.JobCondition{
				Type:               batchv1.JobSuccessCriteriaMet,
				Status:             corev1.ConditionTrue,
				LastProbeTime:      now,
				LastTransitionTime: now,
				Reason:             "SuccessCriteriaMet",
				Message:            "Job success criteria met for timeout test",
			},
			batchv1.JobCondition{
				Type:               batchv1.JobComplete,
				Status:             corev1.ConditionTrue,
				LastProbeTime:      now,
				LastTransitionTime: now,
				Reason:             "Completed",
				Message:            "Job completed successfully for timeout test",
			},
		)
		job.Status.CompletionTime = &now
		job.Status.Succeeded = 1

		_, err = test.Client().Core().BatchV1().Jobs(namespace.Name).UpdateStatus(test.Ctx(), job, metav1.UpdateOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Successfully marked submitter job as completed at %v", now.Time)

		// The submitter is gone but the head never restarted, so the RUNNING status it reports is
		// current and the job must be left to run. Watch well past the grace period.
		LogWithTimestamp(test.T(), "Asserting RayJob %s/%s outlives the submitter", rayJob.Namespace, rayJob.Name)
		g.Consistently(RayJob(test, rayJob.Namespace, rayJob.Name), 3*ray.DefaultSubmitterFinishedTimeout).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusRunning)))

		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(rayJob.Status.JobStatus).To(Equal(rayv1.JobStatusRunning))
		g.Expect(rayJob.Status.Reason).NotTo(Equal(rayv1.JobDeploymentStatusTransitionGracePeriodExceeded))
	})

	test.T().Run("RayCluster status update propagates to RayJob", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("cluster-status-update", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))).
				WithEntrypoint("python /home/ray/jobs/long_running.py").
				WithShutdownAfterJobFinishes(false).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Equal(rayv1.JobStatusRunning)))

		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		rayCluster, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
		g.Expect(err).NotTo(HaveOccurred())

		originalMaxWorkerReplica := rayCluster.Status.MaxWorkerReplicas
		g.Expect(rayJob.Status.RayClusterStatus.MaxWorkerReplicas).To(Equal(originalMaxWorkerReplica))

		newMaxWorkerReplica := originalMaxWorkerReplica + 2
		rayCluster.Status.MaxWorkerReplicas = newMaxWorkerReplica
		_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).UpdateStatus(test.Ctx(), rayCluster, metav1.UpdateOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		g.Eventually(func() int32 {
			job, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
			if err != nil {
				return originalMaxWorkerReplica
			}
			return job.Status.RayClusterStatus.MaxWorkerReplicas
		}, TestTimeoutShort).Should(Equal(newMaxWorkerReplica))

		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	test.T().Run("Successful RayJob in K8s Job mode with auth token", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("counter-auth", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.K8sJobMode).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
				WithSubmitterPodTemplate(JobSubmitterPodTemplateApplyConfiguration()).
				WithRayClusterSpec(NewRayClusterSpec(
					MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs")).
					WithAuthOptions(rayv1ac.AuthOptions().WithMode(rayv1.AuthModeToken))))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully with auth token", rayJob.Namespace, rayJob.Name)

		// Wait for RayCluster name to be populated
		LogWithTimestamp(test.T(), "Waiting for RayCluster to be created")
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobClusterName, Not(BeEmpty())))

		// Get RayCluster name
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		rayClusterName := rayJob.Status.RayClusterName

		// Wait for RayCluster to become ready
		LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", namespace.Name, rayClusterName)
		g.Eventually(RayCluster(test, namespace.Name, rayClusterName), TestTimeoutMedium).
			Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

		// Get RayCluster and verify auth token environment variables
		rayCluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		g.Expect(err).NotTo(HaveOccurred())

		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(headPod).NotTo(BeNil())

		// Verify Ray container has auth token env vars
		VerifyContainerAuthTokenEnvVars(test, rayCluster, &headPod.Spec.Containers[utils.RayContainerIndex])

		// Verify worker pods have auth token env vars
		workerPods, err := GetWorkerPods(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(workerPods).ToNot(BeEmpty())
		for _, workerPod := range workerPods {
			VerifyContainerAuthTokenEnvVars(test, rayCluster, &workerPod.Spec.Containers[utils.RayContainerIndex])
		}

		// Wait for submitter Job to be created
		LogWithTimestamp(test.T(), "Waiting for submitter Job to be created")
		g.Eventually(func(g Gomega) {
			Job(test, namespace.Name, rayJob.Name)(g)
		}, TestTimeoutShort).Should(Succeed())

		// Wait for submitter Job pod to be created and running
		LogWithTimestamp(test.T(), "Waiting for submitter Job pod to be created")
		g.Eventually(Pods(test, namespace.Name, LabelSelector("job-name="+rayJob.Name)), TestTimeoutShort).
			ShouldNot(BeEmpty())

		submitterPods := Pods(test, namespace.Name, LabelSelector("job-name="+rayJob.Name))(g)
		submitterPod := &submitterPods[0]

		// Verify submitter Job pod has auth token env vars in its Ray container
		VerifyContainerAuthTokenEnvVars(test, rayCluster, &submitterPod.Spec.Containers[utils.RayContainerIndex])

		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to complete", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Satisfy(rayv1.IsJobTerminal)))

		// Assert the RayJob has completed successfully
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))

		// And the RayJob deployment status is updated accordingly
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		LogWithTimestamp(test.T(), "RayJob %s/%s completed successfully with auth token", rayJob.Namespace, rayJob.Name)
	})

	test.T().Run("RayJob PreRunningDeadlineSeconds expires during Initializing state", func(_ *testing.T) {
		// Use an invalid image to force ImagePullBackOff on the head pod, guaranteeing the RayCluster never becomes Ready and the RayJob
		// stays in Initializing long enough for the TTL to fire.
		invalidImageOpt := func(spec *rayv1ac.RayClusterSpecApplyConfiguration) *rayv1ac.RayClusterSpecApplyConfiguration {
			spec.HeadGroupSpec.Template.Spec.Containers[0].Image = new("invalid-image-does-not-exist:v1.0.0")
			return spec
		}

		rayJobAC := rayv1ac.RayJob("ttl-initializing", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithRayClusterSpec(NewRayClusterSpec(invalidImageOpt)).
				WithShutdownAfterJobFinishes(true).
				WithPreRunningDeadlineSeconds(5))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Confirm it enters Initializing
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusInitializing)))

		// The RayJob will transition to `Failed` because it has passed `preRunningDeadlineSeconds`.
		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to be 'Failed'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.PreRunningDeadlineExceeded)))
	})

	test.T().Run("RayJob PreRunningDeadlineSeconds expires during Waiting state", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("ttl-waiting", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.InteractiveMode).
				WithRayClusterSpec(NewRayClusterSpec()).
				WithShutdownAfterJobFinishes(true).
				WithPreRunningDeadlineSeconds(60)) // larger value to reach Initializing state first

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())

		// First confirm it enters Waiting state
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusWaiting)))

		// The RayJob will transition to `Failed` because it has passed `preRunningDeadlineSeconds`.
		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to be 'Failed'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.PreRunningDeadlineExceeded)))
	})
}
