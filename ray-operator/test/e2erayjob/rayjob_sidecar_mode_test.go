package e2erayjob

import (
	"os/exec"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayJobSidecarMode(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	// Create a namespace
	namespace := test.NewTestNamespace()

	// Job scripts
	jobsAC := NewConfigMap(namespace.Name, Files(test, "counter.py", "fail.py", "stop.py"))
	jobs, err := test.Client().Core().CoreV1().ConfigMaps(namespace.Name).Apply(test.Ctx(), jobsAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created ConfigMap %s/%s successfully", jobs.Namespace, jobs.Name)

	test.T().Run("Successful RayJob", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("counter", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.SidecarMode).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithEntrypointNumCpus(2).
				WithEntrypointNumGpus(2).
				WithEntrypointResources(`{"R1": 2}`).
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
				WithRayClusterSpec(rayv1ac.RayClusterSpec().
					WithRayVersion(GetRayVersion()).
					WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
						WithRayStartParams(map[string]string{
							"dashboard-host": "0.0.0.0",
							"num-gpus":       "4",
							"num-cpus":       "4",
							"resources":      `'{"R1": 4}'`,
						}).
						WithTemplate(PodTemplateSpecApplyConfiguration(HeadPodTemplateApplyConfiguration(),
							MountConfigMap[corev1ac.PodTemplateSpecApplyConfiguration](jobs, "/home/ray/jobs"))))))

		rayJob := ApplyRayJobAndWaitForTerminal(test, rayJobAC)

		// Assert the RayJob has completed successfully
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusSucceeded)))

		// And the RayJob deployment status is updated accordingly
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		// Refresh the RayJob status
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		// TODO (kevin85421): We may need to use `Eventually` instead if the assertion is flaky.
		// Assert the RayCluster has been torn down
		_, err = GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
		g.Expect(k8serrors.IsNotFound(err)).To(BeTrue())
	})

	test.T().Run("Failing RayJob without cluster shutdown after finished", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("fail", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.SidecarMode).
				WithEntrypoint("python /home/ray/jobs/fail.py").
				WithShutdownAfterJobFinishes(false).
				WithRayClusterSpec(NewRayClusterSpec(MountConfigMap[rayv1ac.RayClusterSpecApplyConfiguration](jobs, "/home/ray/jobs"))))

		rayJob := ApplyRayJobAndWaitForTerminal(test, rayJobAC)

		// Assert the Ray job has failed
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusFailed)))

		// Assert that the RayJob deployment status and RayJob reason have been updated accordingly.
		// Assert that the RayJob failed reason is "AppFailed".
		// In the sidecar submission mode, the submitter Kubernetes Job should not be created.
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name)).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobReason, Equal(rayv1.AppFailed)))
		g.Eventually(Jobs(test, namespace.Name)).Should(BeEmpty())

		// Refresh the RayJob status
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())

		// Verify sidecar container injection
		rayCluster, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
		g.Expect(err).NotTo(HaveOccurred())

		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(headPod).NotTo(BeNil())

		containerNames := make(map[string]bool)
		for _, container := range headPod.Spec.Containers {
			containerNames[container.Name] = true
		}
		g.Expect(containerNames[utils.SubmitterContainerName]).To(BeTrue(), "submitter container should be present")

		// Delete the RayJob
		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Assert the RayCluster has been cascade deleted
		g.Eventually(func() error {
			_, err := GetRayCluster(test, namespace.Name, rayJob.Status.RayClusterName)
			return err
		}).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))
	})

	test.T().Run("Should transition to 'Complete' if the Ray job has stopped.", func(_ *testing.T) {
		// `stop.py` will sleep for 20 seconds so that the RayJob has enough time to transition to `RUNNING`
		// and then stop the Ray job. If the Ray job is stopped, the RayJob should transition to `Complete`.
		rayJobAC := rayv1ac.RayJob("stop", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.SidecarMode).
				WithEntrypoint("python /home/ray/jobs/stop.py").
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

	test.T().Run("RayJob fails when head Pod is deleted when job is running", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("delete-head-after-submit-sidecar-mode", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.SidecarMode).
				WithRayClusterSpec(NewRayClusterSpec()).
				WithEntrypoint("python -c \"import time; time.sleep(60)\"").
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
		g.Expect(rayCluster.Labels[utils.RayJobSubmissionModeLabelKey]).To(Equal(string(rayv1.SidecarMode)))
		g.Expect(rayCluster.Annotations[utils.DisableProvisionedHeadRestartAnnotationKey]).To(Equal("true"))
		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleting head Pod %s/%s for RayCluster %s", headPod.Namespace, headPod.Name, rayCluster.Name)
		err = test.Client().Core().CoreV1().Pods(headPod.Namespace).Delete(test.Ctx(), headPod.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		// Head pod should NOT be recreated for sidecar modes.
		g.Eventually(func() error {
			_, err := GetHeadPod(test, rayCluster)
			return err
		}, TestTimeoutMedium, 2*time.Second).Should(HaveOccurred())
		g.Consistently(func() error {
			_, err := GetHeadPod(test, rayCluster)
			return err
		}, TestTimeoutShort, 2*time.Second).Should(HaveOccurred())

		// After head pod deletion, controller should mark RayJob as Failed with a specific message
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusFailed)))
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobReason, Or(
				Equal(rayv1.AppFailed),
				Equal(rayv1.SubmissionFailed),
			)))

		// Cleanup
		err = test.Client().Ray().RayV1().RayJobs(namespace.Name).Delete(test.Ctx(), rayJob.Name, metav1.DeleteOptions{})
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Deleted RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)
	})

	test.T().Run("Successful RayJob in Sidecar mode with auth token", func(_ *testing.T) {
		rayJobAC := rayv1ac.RayJob("counter-auth", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.SidecarMode).
				WithEntrypoint("python /home/ray/jobs/counter.py").
				WithRuntimeEnvYAML(`
env_vars:
  counter_name: test_counter
`).
				WithShutdownAfterJobFinishes(true).
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

		// Verify submitter container has auth token env vars
		var submitterContainer *corev1.Container
		for i := range headPod.Spec.Containers {
			if headPod.Spec.Containers[i].Name == utils.SubmitterContainerName {
				submitterContainer = &headPod.Spec.Containers[i]
				break
			}
		}
		g.Expect(submitterContainer).NotTo(BeNil(), "submitter container should be present in head pod")
		VerifyContainerAuthTokenEnvVars(test, rayCluster, submitterContainer)

		// Verify worker pods have auth token env vars
		workerPods, err := GetWorkerPods(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(workerPods).ToNot(BeEmpty())
		for _, workerPod := range workerPods {
			VerifyContainerAuthTokenEnvVars(test, rayCluster, &workerPod.Spec.Containers[utils.RayContainerIndex])
		}

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

	test.T().Run("RayJob Sidecar Mode should restart submitter container on non-zero exit", func(t *testing.T) {
		// Check if the SidecarSubmitterRestart feature gate is enabled by inspecting the operator's args directly, rather than inferring from the container spec.
		// The k8s version >= 1.35 is already verified at operator startup (see main.go), so we only need to check the feature gate here.
		var sidecarSubmitterRestartEnabled bool
		// Search all namespaces since the operator may be deployed in any namespace
		operatorDeploymentList, err := test.Client().Core().AppsV1().Deployments("").List(t.Context(), metav1.ListOptions{
			LabelSelector: "app.kubernetes.io/component=kuberay-operator",
		})
		g.Expect(err).NotTo(HaveOccurred())
		// Assuming there should only be one kuberay-operator in the cluster
		g.Expect(operatorDeploymentList.Items).To(HaveLen(1), "expected exactly one kuberay-operator deployment during the test")

		for _, container := range operatorDeploymentList.Items[0].Spec.Template.Spec.Containers {
			if container.Name != "kuberay-operator" {
				continue
			}
			for _, arg := range container.Args {
				if strings.Contains(arg, "SidecarSubmitterRestart=true") {
					sidecarSubmitterRestartEnabled = true
					break
				}
			}

			if sidecarSubmitterRestartEnabled {
				break
			}
		}

		if !sidecarSubmitterRestartEnabled {
			t.Skip("SidecarSubmitterRestart feature gate is not active. SidecarSubmitterRestart is not set to true")
		}

		rayJobAC := rayv1ac.RayJob("submitter-container-should-restart", namespace.Name).
			WithSpec(rayv1ac.RayJobSpec().
				WithSubmissionMode(rayv1.SidecarMode).
				WithRayClusterSpec(NewRayClusterSpec()).
				WithEntrypoint("python -c \"import time; time.sleep(60)\"").
				WithShutdownAfterJobFinishes(true))

		rayJob, err := test.Client().Ray().RayV1().RayJobs(namespace.Name).Apply(test.Ctx(), rayJobAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayJob %s/%s successfully", rayJob.Namespace, rayJob.Name)

		// Wait until the RayJob to become Running
		LogWithTimestamp(test.T(), "Waiting for RayJob %s/%s to be 'Running'", rayJob.Namespace, rayJob.Name)
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutMedium).
			Should(WithTransform(RayJobStatus, Equal(rayv1.JobStatusRunning)))

		// Get RayCluster name
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		rayClusterName := rayJob.Status.RayClusterName

		// Get RayCluster
		rayCluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
		g.Expect(err).NotTo(HaveOccurred())

		// Get headPod
		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(headPod).NotTo(BeNil())

		// Get Submitter Container id
		var containerID string
		for _, containerStatus := range headPod.Status.ContainerStatuses {
			if containerStatus.Name == utils.SubmitterContainerName {
				containerID = strings.TrimPrefix(containerStatus.ContainerID, "containerd://")
				break
			}
		}
		g.Expect(containerID).ToNot(BeEmpty(), "Submitter's container ID should not be empty")
		LogWithTimestamp(test.T(), "Found submitter container ID: %s", containerID)

		// Record job id before restart
		initialJobID := rayJob.Status.JobId

		// stop the container with docker exec
		kindNodeName := headPod.Spec.NodeName
		cmd := exec.CommandContext(test.Ctx(), "docker", "exec", kindNodeName, "crictl", "stop", containerID)
		err = cmd.Run()
		g.Expect(err).NotTo(HaveOccurred())

		// Verify RayJob is still running
		g.Expect(GetRayJob(test, rayJob.Namespace, rayJob.Name)).
			To(WithTransform(RayJobStatus, Equal(rayv1.JobStatusRunning)))

		// Verify the restart count should > 0
		g.Eventually(func() int32 {
			headPod, err = GetHeadPod(test, rayCluster)
			if err != nil || headPod == nil {
				return 0
			}
			for _, cs := range headPod.Status.ContainerStatuses {
				if cs.Name == utils.SubmitterContainerName {
					return cs.RestartCount
				}
			}
			return 0
		}, TestTimeoutMedium, 2*time.Second).Should(BeNumerically(">", 0))

		// Verify RayJob is still running
		rayJob, err = GetRayJob(test, rayJob.Namespace, rayJob.Name)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "RayJob status after stopping submitter: jobStatus=%s deploymentStatus=%s", rayJob.Status.JobStatus, rayJob.Status.JobDeploymentStatus)

		// Verify there is no duplicate submission by checking the job id with initialJobID
		g.Consistently(func() string {
			rayjob, err := GetRayJob(test, rayJob.Namespace, rayJob.Name)
			g.Expect(err).NotTo(HaveOccurred())
			return rayjob.Status.JobId
		}, TestTimeoutShort, 2*time.Second).Should(Equal(initialJobID))
		LogWithTimestamp(test.T(), "RayJob JobID remains %s, there is no duplicate submission", initialJobID)

		// Verify RayJob does not transition to Failed
		g.Consistently(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutShort, 2*time.Second).
			Should(WithTransform(RayJobDeploymentStatus, Not(Equal(rayv1.JobDeploymentStatusFailed))))

		// Verify RayJob eventually completed.
		g.Eventually(RayJob(test, rayJob.Namespace, rayJob.Name), TestTimeoutLong).
			Should(WithTransform(RayJobDeploymentStatus, Equal(rayv1.JobDeploymentStatusComplete)))

		LogWithTimestamp(test.T(), "RayJob %s/%s completed successfully", rayJob.Namespace, rayJob.Name)
	})
}
