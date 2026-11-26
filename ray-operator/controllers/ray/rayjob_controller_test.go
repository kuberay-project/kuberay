/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ray

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
	"github.com/ray-project/kuberay/ray-operator/test/support"
)

func rayJobTemplate(name string, namespace string) *rayv1.RayJob {
	return &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				"foo": "bar",
			},
		},
		Spec: rayv1.RayJobSpec{
			Entrypoint:               "sleep 999",
			SubmissionMode:           rayv1.K8sJobMode,
			ShutdownAfterJobFinishes: true,
			RayClusterSpec: &rayv1.RayClusterSpec{
				RayVersion: support.GetRayVersion(),
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "ray-head",
									Image: support.GetRayImage(),
									Resources: corev1.ResourceRequirements{
										Limits: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("1"),
											corev1.ResourceMemory: resource.MustParse("2Gi"),
										},
										Requests: corev1.ResourceList{
											corev1.ResourceCPU:    resource.MustParse("1"),
											corev1.ResourceMemory: resource.MustParse("2Gi"),
										},
									},
									Ports: []corev1.ContainerPort{
										{
											Name:          utils.GcsServerPortName,
											ContainerPort: utils.DefaultGcsServerPort,
										},
										{
											Name:          utils.DashboardPortName,
											ContainerPort: utils.DefaultDashboardPort,
										},
										{
											Name:          utils.ClientPortName,
											ContainerPort: utils.DefaultClientPort,
										},
									},
								},
							},
						},
					},
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						Replicas:    ptr.To[int32](3),
						MinReplicas: ptr.To[int32](0),
						MaxReplicas: ptr.To[int32](10000),
						GroupName:   "small-group",
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "ray-worker",
										Image: support.GetRayImage(),
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// updateK8sJobToComplete updates a Kubernetes Job status to mark it as complete.
// This sets conditions (JobSuccessCriteriaMet and JobComplete) and
// timestamps (StartTime, CompletionTime) required by Kubernetes 1.33+.
func updateK8sJobToComplete(ctx context.Context, job *batchv1.Job) {
	startTime := metav1.Now()
	completionTime := metav1.NewTime(startTime.Add(time.Second))
	job.Status.Conditions = []batchv1.JobCondition{
		{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue, LastTransitionTime: completionTime},
		{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, LastTransitionTime: completionTime},
	}
	job.Status.StartTime = &startTime
	job.Status.CompletionTime = &completionTime
	Expect(k8sClient.Status().Update(ctx, job)).Should(Succeed())
}

var _ = Context("RayJob with different submission modes", func() {
	Context("RayJob in K8sJobMode", func() {
		Describe("RayJob SubmitterConfig BackoffLimit", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJobWithDefaultSubmitterConfigBackoffLimit := rayJobTemplate("rayjob-default", namespace)
			rayJobWithNonDefaultSubmitterConfigBackoffLimit := rayJobTemplate("rayjob-non-default", namespace)
			rayJobWithNonDefaultSubmitterConfigBackoffLimit.Spec.SubmitterConfig = &rayv1.SubmitterConfig{
				BackoffLimit: ptr.To[int32](88),
			}
			rayJobs := make(map[*rayv1.RayJob]int32)
			rayJobs[rayJobWithDefaultSubmitterConfigBackoffLimit] = int32(2)
			rayJobs[rayJobWithNonDefaultSubmitterConfigBackoffLimit] = int32(88)

			It("Verify RayJob spec", func() {
				for rayJob := range rayJobs {
					// Make sure the submission mode is K8sJobMode.
					Expect(rayJob.Spec.SubmissionMode).To(Equal(rayv1.K8sJobMode))
				}
			})

			It("Create RayJob custom resources", func() {
				for rayJob := range rayJobs {
					err := k8sClient.Create(ctx, rayJob)
					Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob: %v", rayJob.Name)
					Eventually(
						getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
						time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
				}
			})

			It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				for rayJob := range rayJobs {
					Eventually(
						getRayJobDeploymentStatus(ctx, rayJob),
						time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
				}
			})

			It("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				for rayJob := range rayJobs {
					rayCluster := &rayv1.RayCluster{}
					Eventually(
						getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
						time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

					// Make RayCluster.Status.State to be rayv1.Ready.
					updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
					updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

					// The RayCluster.Status.State should be Ready.
					Eventually(
						getClusterState(ctx, namespace, rayCluster.Name),
						time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))

					// RayJobs's JobDeploymentStatus transitions to Running.
					Eventually(
						getRayJobDeploymentStatus(ctx, rayJob),
						time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				}
			})

			It("Verify K8s Job BackoffLimit", func() {
				for rayJob, backoffLimit := range rayJobs {
					// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
					namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
					job := &batchv1.Job{}
					err := k8sClient.Get(ctx, namespacedName, job)
					Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
					Expect(*(job.Spec.BackoffLimit)).To(Equal(backoffLimit))
				}
			})
		})

		Describe("Successful RayJob in K8sJobMode", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test", namespace)
			rayCluster := &rayv1.RayCluster{}

			It("Verify RayJob spec", func() {
				// This test case simulates the most common scenario in the RayJob code path.
				// (1) The submission mode is K8sJobMode.
				// (2) `shutdownAfterJobFinishes` is true.
				// In this test, RayJob passes through the following states: New -> Initializing -> Running -> Complete
				Expect(rayJob.Spec.SubmissionMode).To(Equal(rayv1.K8sJobMode))
				Expect(rayJob.Spec.ShutdownAfterJobFinishes).To(BeTrue())

				// This test assumes that there is only one worker group.
				Expect(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs).To(HaveLen(1))
			})

			It("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			It("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			It("In Initializing state, the JobStatus should show the RayCluster status", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningNotReady(ctx, rayJob.Status.RayClusterName, namespace)

				// Now the cluster should have nonzero conditions.
				Eventually(
					func() int {
						status := getClusterStatus(ctx, namespace, rayCluster.Name)()
						return len(status.Conditions)
					},
					time.Second*3, time.Millisecond*500).ShouldNot(Equal(0))

				// We expect the RayJob's RayClusterStatus to eventually mirror the cluster's status.
				Eventually(
					func() (int, error) {
						currentRayJob := &rayv1.RayJob{}
						err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, currentRayJob)
						if err != nil {
							return 0, err
						}
						return len(currentRayJob.Status.RayClusterStatus.Conditions), nil
					},
					time.Second*3, time.Millisecond*500,
				).ShouldNot(Equal(0))
			})

			It("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			It("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			It("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			It("If shutdownAfterJobFinishes is true, RayCluster should be deleted but not the submitter Job.", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		Describe("Successful RayJob in K8sJobMode with a maximum name", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate(strings.Repeat("j", utils.MaxRayJobNameLength), namespace)
			rayCluster := &rayv1.RayCluster{}

			It("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			It("In Initializing state, the RayCluster should eventually be created.", func() {
				var svc corev1.Service
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)
				// Also check its service name.
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName + "-head-svc", Namespace: namespace}, &svc),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		Describe("Invalid RayJob in K8sJobMode", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-invalid-test", namespace)
			rayCluster := &rayv1.RayCluster{Spec: *rayJob.Spec.RayClusterSpec}
			template := common.GetSubmitterTemplate(&rayJob.Spec, &rayCluster.Spec)
			template.Spec.RestartPolicy = "" // Make it invalid to create a submitter. Ref: https://github.com/ray-project/kuberay/pull/2389#issuecomment-2359564334
			rayJob.Spec.SubmitterPodTemplate = &template

			It("Verify RayJob spec", func() {
				Expect(rayJob.Spec.SubmissionMode).To(Equal(rayv1.K8sJobMode))
			})

			It("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			It("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)
			})

			It("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			It("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					func() ([]eventsv1.Event, error) {
						eventList := &eventsv1.EventList{}
						if err := k8sClient.List(ctx, eventList, client.InNamespace(rayJob.Namespace)); err != nil {
							return nil, err
						}
						return eventList.Items, nil
					},
					time.Second*3, time.Millisecond*500).Should(ContainElement(HaveField("Note", ContainSubstring("Failed to create new Kubernetes Job default/rayjob-invalid-test"))))

				_ = k8sClient.Delete(ctx, rayJob)
			})
		})

		Describe("Successful RayJob in K8sjobMode with DELETE_RAYJOB_CR_AFTER_JOB_FINISHES", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-delete", namespace)
			rayCluster := &rayv1.RayCluster{}

			It("Verify RayJob spec", func() {
				// This test case simulates the most common scenario in the RayJob code path.
				// (1) The submission mode is K8sJobMode.
				// (2) `shutdownAfterJobFinishes` is true.
				// In this test, RayJob passes through the following states: New -> Initializing -> Running -> Complete
				Expect(rayJob.Spec.SubmissionMode).To(Equal(rayv1.K8sJobMode))
				Expect(rayJob.Spec.ShutdownAfterJobFinishes).To(BeTrue())

				// This test assumes that there is only one worker group.
				Expect(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs).To(HaveLen(1))
			})

			It("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			It("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			It("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			It("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			It("RayJobs's JobDeploymentStatus transitions from Running to Complete and RayJob is deleted when DELETE_RAYJOB_CR_AFTER_JOB_FINISHES is set.", func() {
				// Set the env var before the RayJob transitions to Complete so that
				// handleShutdownAfterJobFinishes sees it on the first reconciliation
				// after the job finishes. Setting it after the transition is racy because
				// the reconciler may have already processed the shutdown path without
				// the env var, returning with no requeue.
				os.Setenv(utils.DELETE_RAYJOB_CR_AFTER_JOB_FINISHES, "true")
				defer os.Unsetenv(utils.DELETE_RAYJOB_CR_AFTER_JOB_FINISHES)

				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete and is then deleted because DELETE_RAYJOB_CR_AFTER_JOB_FINISHES is set.
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob)())
					}, time.Second*10, time.Millisecond*500).Should(BeTrue())
			})
		})

		Describe("RayJob has passed the ActiveDeadlineSeconds", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			activeDeadlineSeconds := int32(3)
			rayJob := rayJobTemplate("rayjob-deadline", namespace)
			rayJob.Spec.ActiveDeadlineSeconds = new(activeDeadlineSeconds)

			It("Verify RayJob spec", func() {
				// In this test, RayJob passes through the following states: New -> Initializing -> Complete (because of ActiveDeadlineSeconds).
				Expect(rayJob.Spec.ActiveDeadlineSeconds).NotTo(BeNil())

				// This test assumes that there is only one worker group.
				Expect(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs).To(HaveLen(1))
			})

			It("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			It("RayJobs has passed the activeDeadlineSeconds, and the JobDeploymentStatus transitions from Initializing to Complete.", func() {
				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
				Expect(rayJob.Status.Reason).To(Equal(rayv1.DeadlineExceeded))
			})
		})

		Describe("Retrying RayJob in K8sJobMode", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-retry-test", namespace)
			rayJob.Spec.BackoffLimit = ptr.To[int32](1)
			rayCluster := &rayv1.RayCluster{}

			It("Verify RayJob spec", func() {
				// This test case simulates a retry scenario in the RayJob when:
				// (1) The submission mode is K8sJobMode.
				// (2) backoffLimit > 0
				// In this test, RayJob passes through the following states: New -> Initializing -> Running -> Retrying -> New
				Expect(rayJob.Spec.SubmissionMode).To(Equal(rayv1.K8sJobMode))
				Expect(*rayJob.Spec.BackoffLimit).To(Equal(int32(1)))
			})

			It("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			It("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			It("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			It("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			It("RayJobs's JobDeploymentStatus transitions from Running -> Retrying -> New -> Initializing", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				//nolint:unparam // this is a mock and the function signature cannot change
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) {
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// record the current cluster name
				oldClusterName := rayJob.Status.RayClusterName

				// RayJob transitions from Running -> Retrying -> New -> Initializing
				// We only check the final state "Initializing" because it's difficult to test transient states like "Retrying" and "New"
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// validate the RayCluster is deleted on retry
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: oldClusterName, Namespace: namespace}, rayCluster)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())

				// validate the submitter Job is deleted on retry
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, common.RayJobK8sJobNamespacedName(rayJob), job)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())
			})

			It("In Initializing state, the RayCluster should eventually be created (attempt 2)", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			It("Make RayCluster.Status.State to be rayv1.Ready (attempt 2)", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			It("RayJobs's JobDeploymentStatus transitions from Initializing to Running (attempt 2)", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			It("RayJobs's JobDeploymentStatus transitions from Running -> Complete (attempt 2)", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				//nolint:unparam // this is a mock and the function signature cannot change
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) {
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions from Running -> Complete
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			It("Validate RayJob succeeded and failed status", func() {
				Eventually(
					getRayJobSucceededStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(int32(1)), "succeeded = %v", rayJob.Status.Succeeded)

				Eventually(
					getRayJobFailedStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(int32(1)), "failed = %v", rayJob.Status.Failed)
			})
		})
	})

	Context("RayJob in InteractiveMode", func() {
		Describe("Successful RayJob", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-none-mode", namespace)
			rayJob.Spec.SubmissionMode = rayv1.InteractiveMode
			rayCluster := &rayv1.RayCluster{}
			testRayJobId := "fake-id"

			It("Verify RayJob spec", func() {
				Expect(rayJob.Spec.SubmissionMode).To(Equal(rayv1.InteractiveMode))
				Expect(rayJob.Spec.ShutdownAfterJobFinishes).To(BeTrue())
				Expect(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs).To(HaveLen(1))
			})

			It("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).To(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			It("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			It("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			It("RayJobs's JobDeploymentStatus transitions from Initializing to Waiting.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusWaiting), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			It("sets jobId in RayJob", func() {
				err := setJobIdOnRayJob(ctx, rayJob, testRayJobId)
				Expect(err).NotTo(HaveOccurred())
			})

			It("RayJobs's JobDeploymentStatus transitions from Waiting to Running if annotation is set.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			It("should set RayJob's JobId to the value of the annotation", func() {
				Expect(rayJob.Status.JobId).To(Equal(testRayJobId))
			})
		})
	})

	Describe("RayJob with DeletionStrategy", Ordered, func() {
		JustBeforeEach(func() {
			features.SetFeatureGateDuringTest(GinkgoTB(), features.RayJobDeletionPolicy, true)
		})

		It("Delete cluster on success", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-deletecluster-on-success", namespace)

			onSuccessPolicy := rayv1.DeleteCluster
			onFailurePolicy := rayv1.DeleteNone
			deletionStrategy := &rayv1.DeletionStrategy{
				OnSuccess: &rayv1.DeletionPolicy{
					Policy: &onSuccessPolicy,
				},
				OnFailure: &rayv1.DeletionPolicy{
					Policy: &onFailurePolicy,
				},
			}
			rayJob.Spec.DeletionStrategy = deletionStrategy
			rayJob.Spec.ShutdownAfterJobFinishes = false
			rayCluster := &rayv1.RayCluster{}

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: &onSuccessPolicy,
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: &onFailurePolicy,
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteCluster, RayCluster should be deleted, but not the submitter Job.", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Delete cluster on failure", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-deletecluster-on-failure", namespace)

			onSuccessPolicy := rayv1.DeleteNone
			onFailurePolicy := rayv1.DeleteCluster
			deletionStrategy := &rayv1.DeletionStrategy{
				OnSuccess: &rayv1.DeletionPolicy{
					Policy: &onSuccessPolicy,
				},
				OnFailure: &rayv1.DeletionPolicy{
					Policy: &onFailurePolicy,
				},
			}
			rayJob.Spec.DeletionStrategy = deletionStrategy
			rayJob.Spec.ShutdownAfterJobFinishes = false
			rayCluster := &rayv1.RayCluster{}

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: &onSuccessPolicy,
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: &onFailurePolicy,
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteCluster, RayCluster should be deleted, but not the submitter Job.", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Delete workers on success", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-deleteworkers-on-success", namespace)

			onSuccessPolicy := rayv1.DeleteWorkers
			onFailurePolicy := rayv1.DeleteNone
			deletionStrategy := rayv1.DeletionStrategy{
				OnSuccess: &rayv1.DeletionPolicy{
					Policy: &onSuccessPolicy,
				},
				OnFailure: &rayv1.DeletionPolicy{
					Policy: &onFailurePolicy,
				},
			}
			rayJob.Spec.DeletionStrategy = &deletionStrategy
			rayJob.Spec.ShutdownAfterJobFinishes = false
			rayCluster := &rayv1.RayCluster{}

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: &onSuccessPolicy,
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: &onFailurePolicy,
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteWorkers, all workers should be deleted, but not the Head pod and submitter Job", func() {
				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check worker group is suspended
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Suspend).To(BeTrue())

				// 0 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Eventually(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(0), "expected 0 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Delete workers on failure", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-deleteworkers-on-failure", namespace)

			onSuccessPolicy := rayv1.DeleteWorkers
			onFailurePolicy := rayv1.DeleteWorkers
			deletionStrategy := rayv1.DeletionStrategy{
				OnSuccess: &rayv1.DeletionPolicy{
					Policy: &onSuccessPolicy,
				},
				OnFailure: &rayv1.DeletionPolicy{
					Policy: &onFailurePolicy,
				},
			}
			rayJob.Spec.DeletionStrategy = &deletionStrategy
			rayJob.Spec.ShutdownAfterJobFinishes = false
			rayCluster := &rayv1.RayCluster{}

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: &onSuccessPolicy,
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: &onFailurePolicy,
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteWorkers, all workers should be deleted, but not the Head pod and submitter Job", func() {
				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check worker group is suspended
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Suspend).To(BeTrue())

				// 0 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Eventually(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(0), "expected 0 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Delete self on success", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-deleteself-on-success", namespace)

			onSuccessPolicy := rayv1.DeleteSelf
			onFailurePolicy := rayv1.DeleteNone
			deletionStrategy := rayv1.DeletionStrategy{
				OnSuccess: &rayv1.DeletionPolicy{
					Policy: &onSuccessPolicy,
				},
				OnFailure: &rayv1.DeletionPolicy{
					Policy: &onFailurePolicy,
				},
			}
			rayJob.Spec.DeletionStrategy = &deletionStrategy
			rayJob.Spec.ShutdownAfterJobFinishes = false
			rayCluster := &rayv1.RayCluster{}

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)
			})

			By("If DeletionStrategy=DeleteSelf, the RayJob is deleted", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob))
					}, time.Second*5, time.Millisecond*500).Should(BeTrue())
			})
		})

		It("Delete self on failure", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-deleteself-on-failure", namespace)

			onSuccessPolicy := rayv1.DeleteNone
			onFailurePolicy := rayv1.DeleteSelf
			deletionStrategy := rayv1.DeletionStrategy{
				OnSuccess: &rayv1.DeletionPolicy{
					Policy: &onSuccessPolicy,
				},
				OnFailure: &rayv1.DeletionPolicy{
					Policy: &onFailurePolicy,
				},
			}
			rayJob.Spec.DeletionStrategy = &deletionStrategy
			rayJob.Spec.ShutdownAfterJobFinishes = false
			rayCluster := &rayv1.RayCluster{}

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)
			})

			By("If DeletionStrategy=DeleteSelf, the RayJob is deleted", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob))
					}, time.Second*5, time.Millisecond*500).Should(BeTrue())
			})
		})

		It("Delete none on success", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-deletenone-on-success", namespace)

			onSuccessPolicy := rayv1.DeleteNone
			onFailurePolicy := rayv1.DeleteNone
			deletionStrategy := rayv1.DeletionStrategy{
				OnSuccess: &rayv1.DeletionPolicy{
					Policy: &onSuccessPolicy,
				},
				OnFailure: &rayv1.DeletionPolicy{
					Policy: &onFailurePolicy,
				},
			}
			rayJob.Spec.DeletionStrategy = &deletionStrategy
			rayJob.Spec.ShutdownAfterJobFinishes = false
			rayCluster := &rayv1.RayCluster{}

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: &onSuccessPolicy,
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: &onFailurePolicy,
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteNone, no resources are deleted", func() {
				// RayJob exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayJob %v not found", rayJob)

				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Worker replicas set to 3
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(int32(3)))

				// 3 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(3), "expected 3 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Delete none on failure", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-deletenone-on-failure", namespace)

			onSuccessPolicy := rayv1.DeleteCluster
			onFailurePolicy := rayv1.DeleteNone
			deletionStrategy := rayv1.DeletionStrategy{
				OnSuccess: &rayv1.DeletionPolicy{
					Policy: &onSuccessPolicy,
				},
				OnFailure: &rayv1.DeletionPolicy{
					Policy: &onFailurePolicy,
				},
			}
			rayJob.Spec.DeletionStrategy = &deletionStrategy
			rayJob.Spec.ShutdownAfterJobFinishes = false
			rayCluster := &rayv1.RayCluster{}

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: &onSuccessPolicy,
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: &onFailurePolicy,
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteNone, no resources are deleted", func() {
				// RayJob exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayJob %v not found", rayJob)

				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Worker replicas set to 3
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(int32(3)))

				// 3 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(3), "expected 3 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Should delete workers on success when a single 'DeleteWorkers' rule is set", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-rule-deleteworkers-on-success", namespace)
			rayCluster := &rayv1.RayCluster{}

			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteWorkers,
						Condition: rayv1.DeletionCondition{
							JobStatus: ptr.To(rayv1.JobStatusSucceeded),
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus: ptr.To(rayv1.JobStatusSucceeded),
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteWorkers, all workers should be deleted, but not the Head pod and submitter Job", func() {
				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check worker group is suspended
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Suspend).To(BeTrue())

				// 0 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Eventually(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(0), "expected 0 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Should delete workers on failure when a single 'DeleteWorkers' rule is set", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-rule-deleteworkers-on-failure", namespace)
			rayCluster := &rayv1.RayCluster{}

			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteWorkers,
						Condition: rayv1.DeletionCondition{
							JobStatus: ptr.To(rayv1.JobStatusFailed),
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus: ptr.To(rayv1.JobStatusFailed),
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Failed.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteWorkers, all workers should be deleted, but not the Head pod and submitter Job", func() {
				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check worker group is suspended
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Suspend).To(BeTrue())

				// 0 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Eventually(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(0), "expected 0 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Should delete cluster on success when a single 'DeleteCluster' rule is set", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-rule-deletecluster-on-success", namespace)
			rayCluster := &rayv1.RayCluster{}

			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteCluster,
						Condition: rayv1.DeletionCondition{
							JobStatus: ptr.To(rayv1.JobStatusSucceeded),
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus: ptr.To(rayv1.JobStatusSucceeded),
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteCluster, RayCluster should be deleted, but not the submitter Job.", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Should delete cluster on failure when a single 'DeleteCluster' rule is set", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-rule-deletecluster-on-failure", namespace)
			rayCluster := &rayv1.RayCluster{}

			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteCluster,
						Condition: rayv1.DeletionCondition{
							JobStatus: ptr.To(rayv1.JobStatusFailed),
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus: ptr.To(rayv1.JobStatusFailed),
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteCluster, RayCluster should be deleted, but not the submitter Job.", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Should delete self on success when a single 'DeleteSelf' rule is set", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-rule-deleteself-on-success", namespace)
			rayCluster := &rayv1.RayCluster{}

			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteSelf,
						Condition: rayv1.DeletionCondition{
							JobStatus: ptr.To(rayv1.JobStatusSucceeded),
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)
			})

			By("If DeletionStrategy=DeleteSelf, the RayJob is deleted", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob))
					}, time.Second*5, time.Millisecond*500).Should(BeTrue())
			})
		})

		It("Should delete self on failure when a single 'DeleteSelf' rule is set", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-rule-deleteself-on-failure", namespace)
			rayCluster := &rayv1.RayCluster{}

			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteSelf,
						Condition: rayv1.DeletionCondition{
							JobStatus: ptr.To(rayv1.JobStatusFailed),
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)
			})

			By("If DeletionStrategy=DeleteSelf, the RayJob is deleted", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob))
					}, time.Second*5, time.Millisecond*500).Should(BeTrue())
			})
		})

		It("Should delete none on success when a single 'DeleteNone' rule is set", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-rule-deletenone-on-success", namespace)
			rayCluster := &rayv1.RayCluster{}

			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteNone,
						Condition: rayv1.DeletionCondition{
							JobStatus: ptr.To(rayv1.JobStatusSucceeded),
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteNone,
							Condition: rayv1.DeletionCondition{
								JobStatus: ptr.To(rayv1.JobStatusSucceeded),
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteNone, no resources are deleted", func() {
				// RayJob exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayJob %v not found", rayJob)

				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Worker replicas set to 3
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(int32(3)))

				// 3 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(3), "expected 3 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Should delete none on failure when a single 'DeleteNone' rule is set", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-rule-deletenone-on-failure", namespace)
			rayCluster := &rayv1.RayCluster{}

			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteNone,
						Condition: rayv1.DeletionCondition{
							JobStatus: ptr.To(rayv1.JobStatusFailed),
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteNone,
							Condition: rayv1.DeletionCondition{
								JobStatus: ptr.To(rayv1.JobStatusFailed),
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("If DeletionStrategy=DeleteNone, no resources are deleted", func() {
				// RayJob exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayJob %v not found", rayJob)

				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Worker replicas set to 3
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(int32(3)))

				// 3 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(3), "expected 3 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})
		})

		It("Should execute MOST impactful rule (DeleteSelf) when all rules are overdue on success", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-impactful-rule-override-on-success", namespace)
			rayCluster := &rayv1.RayCluster{}

			// Define the multi-stage DeletionStrategy
			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteWorkers,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
							TTLSeconds: 0,
						},
					},
					{
						Policy: rayv1.DeleteCluster,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
							TTLSeconds: 0,
						},
					},
					{
						Policy: rayv1.DeleteSelf,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
							TTLSeconds: 0,
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 0,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 0,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 0,
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource with multi-stage deletion rules", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)
			})

			By("Verify RayJob itself is deleted", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob))
					}, time.Second*5, time.Millisecond*500).Should(BeTrue())
			})
		})

		It("Should execute MOST impactful rule (DeleteSelf) when all rules are overdue on failure", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-impactful-rule-override-on-failure", namespace)
			rayCluster := &rayv1.RayCluster{}

			// Define the multi-stage DeletionStrategy
			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteWorkers,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusFailed),
							TTLSeconds: 0,
						},
					},
					{
						Policy: rayv1.DeleteCluster,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusFailed),
							TTLSeconds: 0,
						},
					},
					{
						Policy: rayv1.DeleteSelf,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusFailed),
							TTLSeconds: 0,
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusFailed),
								TTLSeconds: 0,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusFailed),
								TTLSeconds: 0,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusFailed),
								TTLSeconds: 0,
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource with multi-stage deletion rules", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)
			})

			By("Verify RayJob itself is deleted", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob))
					}, time.Second*5, time.Millisecond*500).Should(BeTrue())
			})
		})

		It("Should process multi-stage deletions in order on success: Workers, then Cluster, then Self", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-multistage-deletion-on-success", namespace)
			rayCluster := &rayv1.RayCluster{}

			// Define the multi-stage DeletionStrategy
			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteWorkers,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
							TTLSeconds: 0, // Stage 1: Delete workers after 0 seconds
						},
					},
					{
						Policy: rayv1.DeleteCluster,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
							TTLSeconds: 5, // Stage 2: Delete cluster after 5 seconds
						},
					},
					{
						Policy: rayv1.DeleteSelf,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
							TTLSeconds: 10, // Stage 3: Delete self after 10 seconds
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 0,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 5,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource with multi-stage deletion rules", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Succeeded" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Complete.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusComplete), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("Stage 1: Verify workers are deleted, but cluster and job still exist", func() {
				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check worker group is suspended
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Suspend).To(BeTrue())

				// 0 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Eventually(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(0), "expected 0 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})

			By("Stage 2 (after 5s): Verify RayCluster is deleted, but job still exists", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})

			By("Stage 3 (after 10s): Verify RayJob itself is deleted", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob))
					}, time.Second*5, time.Millisecond*500).Should(BeTrue())
			})
		})

		It("Should process multi-stage deletions in order on failure: Workers, then Cluster, then Self", func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-test-multistage-deletion-on-failure", namespace)
			rayCluster := &rayv1.RayCluster{}

			// Define the multi-stage DeletionStrategy
			rayJob.Spec.DeletionStrategy = &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{
					{
						Policy: rayv1.DeleteWorkers,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusFailed),
							TTLSeconds: 0, // Stage 1: Delete workers after 0 seconds
						},
					},
					{
						Policy: rayv1.DeleteCluster,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusFailed),
							TTLSeconds: 5, // Stage 2: Delete cluster after 5 seconds
						},
					},
					{
						Policy: rayv1.DeleteSelf,
						Condition: rayv1.DeletionCondition{
							JobStatus:  ptr.To(rayv1.JobStatusFailed),
							TTLSeconds: 10, // Stage 3: Delete self after 10 seconds
						},
					},
				},
			}
			rayJob.Spec.ShutdownAfterJobFinishes = false

			By("Verify RayJob spec", func() {
				Expect(*rayJob.Spec.DeletionStrategy).To(Equal(rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusFailed),
								TTLSeconds: 0,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusFailed),
								TTLSeconds: 5,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusFailed),
								TTLSeconds: 10,
							},
						},
					},
				}))
			})

			By("Create a RayJob custom resource with multi-stage deletion rules", func() {
				err := k8sClient.Create(ctx, rayJob)
				Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Should be able to see RayJob: %v", rayJob.Name)
			})

			By("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Initializing state, Status.RayClusterName, Status.JobId, and Status.StartTime must be set.
				Expect(rayJob.Status.RayClusterName).NotTo(BeEmpty())
				Expect(rayJob.Status.JobId).NotTo(BeEmpty())
				Expect(rayJob.Status.StartTime).NotTo(BeNil())
			})

			By("In Initializing state, the RayCluster should eventually be created.", func() {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check whether RayCluster is consistent with RayJob's RayClusterSpec.
				Expect(rayCluster.Spec.WorkerGroupSpecs[0].Replicas).To(Equal(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas))
				Expect(rayCluster.Spec.RayVersion).To(Equal(rayJob.Spec.RayClusterSpec.RayVersion))

				// TODO (kevin85421): Check the RayCluster labels.
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRNameLabelKey, rayJob.Name))
				Expect(rayCluster.Labels).Should(HaveKeyWithValue(utils.RayOriginatedFromCRDLabelKey, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD)))

				Expect(rayCluster.Annotations).Should(Equal(rayJob.Annotations))
			})

			By("Make RayCluster.Status.State to be rayv1.Ready", func() {
				// The RayCluster is not 'Ready' yet because Pods are not running and ready.
				Expect(rayCluster.Status.State).NotTo(Equal(rayv1.Ready))

				updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
				updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)

				// The RayCluster.Status.State should be Ready.
				Eventually(
					getClusterState(ctx, namespace, rayCluster.Name),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			})

			By("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// In Running state, the RayJob's Status.DashboardURL must be set.
				Expect(rayJob.Status.DashboardURL).NotTo(BeEmpty())

				// In Running state, the submitter Kubernetes Job must be created if this RayJob is in K8sJobMode.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")
			})

			By("RayJobs's JobDeploymentStatus transitions from Running to Complete.", func() {
				// Update fake dashboard client to return job info with "Failed" status.
				getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) { //nolint:unparam // This is a mock function so parameters are required
					return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusFailed, EndTime: uint64(time.Now().UnixMilli())}, nil
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				// RayJob transitions to Complete if and only if the corresponding submitter Kubernetes Job is Complete or Failed.
				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

				// Update the submitter Kubernetes Job to Complete.
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				err := k8sClient.Get(ctx, namespacedName, job)
				Expect(err).NotTo(HaveOccurred(), "failed to get Kubernetes Job")

				updateK8sJobToComplete(ctx, job)

				// RayJob transitions to Failed.
				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed), "jobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
			})

			By("Stage 1: Verify workers are deleted, but cluster and job still exist", func() {
				// RayCluster exists
				Consistently(
					getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster %v not found", rayJob.Status.RayClusterName)

				// Check worker group is suspended
				Expect(*rayCluster.Spec.WorkerGroupSpecs[0].Suspend).To(BeTrue())

				// 0 worker Pods exist
				workerPods := corev1.PodList{}
				workerLabels := common.RayClusterWorkerPodsAssociationOptions(rayCluster).ToListOptions()
				Eventually(
					listResourceFunc(ctx, &workerPods, workerLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(0), "expected 0 workers")

				// Head Pod is still running
				headPods := corev1.PodList{}
				headLabels := common.RayClusterHeadPodsAssociationOptions(rayCluster).ToListOptions()
				Consistently(
					listResourceFunc(ctx, &headPods, headLabels...),
					time.Second*3, time.Millisecond*500).Should(Equal(1), "Head pod list should have only 1 Pod = %v", headPods.Items)

				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})

			By("Stage 2 (after 5s): Verify RayCluster is deleted, but job still exists", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster)())
					},
					time.Second*3, time.Millisecond*500).Should(BeTrue())
				namespacedName := common.RayJobK8sJobNamespacedName(rayJob)
				job := &batchv1.Job{}
				Consistently(
					getResourceFunc(ctx, namespacedName, job),
					time.Second*3, time.Millisecond*500).Should(Succeed())
			})

			By("Stage 3 (after 10s): Verify RayJob itself is deleted", func() {
				Eventually(
					func() bool {
						return apierrors.IsNotFound(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob))
					}, time.Second*5, time.Millisecond*500).Should(BeTrue())
			})
		})
	})

	Context("RayJob job status query timeout", func() {
		transitionRayJobToRunning := func(ctx context.Context, rayJob *rayv1.RayJob, rayCluster *rayv1.RayCluster, namespace string) {
			err := k8sClient.Create(ctx, rayJob)
			Expect(err).NotTo(HaveOccurred())
			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing))
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayJob.Status.RayClusterName, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed())
			updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
			updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
			Eventually(
				getClusterState(ctx, namespace, rayCluster.Name),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning))
		}

		assertJobStatusCheckTimeoutExceeded := func(ctx context.Context, rayJob *rayv1.RayJob, namespace string) {
			getJobInfo := func(_ context.Context, _ string) (*utiltypes.RayJobInfo, error) {
				return nil, fmt.Errorf("connection refused")
			}
			fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
			defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob)).Should(Succeed())
			pastTime := metav1.NewTime(time.Now().Add(-10 * time.Minute))
			rayJob.Status.JobStatusCheckFailureStartTime = &pastTime
			rayJob.Status.JobStatus = rayv1.JobStatusRunning
			Expect(k8sClient.Status().Update(ctx, rayJob)).Should(Succeed())

			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed))
			Eventually(func() (rayv1.JobFailedReason, error) {
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob); err != nil {
					return "", err
				}
				return rayJob.Status.Reason, nil
			}, time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobStatusCheckTimeoutExceeded))
		}

		assertJobStatusCheckFailureStartTimePersisted := func(ctx context.Context, rayJob *rayv1.RayJob, namespace string) {
			getJobInfo := func(_ context.Context, _ string) (*utiltypes.RayJobInfo, error) {
				return nil, fmt.Errorf("connection refused")
			}
			fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
			defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

			Eventually(func() (*metav1.Time, error) {
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob); err != nil {
					return nil, err
				}
				return rayJob.Status.JobStatusCheckFailureStartTime, nil
			}, time.Second*10, time.Millisecond*500).ShouldNot(BeNil())
		}

		assertJobStatusCheckTimeoutExceededOrganically := func(ctx context.Context, rayJob *rayv1.RayJob, namespace string) {
			os.Setenv(utils.RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS, "1")
			defer os.Unsetenv(utils.RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS)

			getJobInfo := func(_ context.Context, _ string) (*utiltypes.RayJobInfo, error) {
				return nil, fmt.Errorf("connection refused")
			}
			fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
			defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*15, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed))
			Eventually(func() (rayv1.JobFailedReason, error) {
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob); err != nil {
					return "", err
				}
				return rayJob.Status.Reason, nil
			}, time.Second*15, time.Millisecond*500).Should(Equal(rayv1.JobStatusCheckTimeoutExceeded))
		}

		Describe("K8sJobMode", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-query-timeout-k8s", namespace)
			rayCluster := &rayv1.RayCluster{}

			It("Create RayJob and transition to Running", func() {
				transitionRayJobToRunning(ctx, rayJob, rayCluster, namespace)
			})

			It("Persists JobStatusCheckFailureStartTime on first status check failure", func() {
				assertJobStatusCheckFailureStartTimePersisted(ctx, rayJob, namespace)
			})

			It("Transitions to Failed when job status checks exceed timeout", func() {
				assertJobStatusCheckTimeoutExceeded(ctx, rayJob, namespace)
			})
		})

		Describe("HTTPMode", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-query-timeout-http", namespace)
			rayJob.Spec.SubmissionMode = rayv1.HTTPMode
			rayCluster := &rayv1.RayCluster{}

			It("Create RayJob and transition to Running", func() {
				transitionRayJobToRunning(ctx, rayJob, rayCluster, namespace)
			})

			It("Persists JobStatusCheckFailureStartTime on first status check failure", func() {
				assertJobStatusCheckFailureStartTimePersisted(ctx, rayJob, namespace)
			})

			It("Transitions to Failed when job status checks exceed timeout organically", func() {
				assertJobStatusCheckTimeoutExceededOrganically(ctx, rayJob, namespace)
			})
		})

		Describe("HTTPMode recovery", Ordered, func() {
			ctx := context.Background()
			namespace := "default"
			rayJob := rayJobTemplate("rayjob-recover-timer-http", namespace)
			rayJob.Spec.SubmissionMode = rayv1.HTTPMode
			rayCluster := &rayv1.RayCluster{}

			It("Create RayJob and transition to Running", func() {
				transitionRayJobToRunning(ctx, rayJob, rayCluster, namespace)
			})

			It("Clears JobStatusCheckFailureStartTime after successful HTTP submit when the job has not been observed yet", func() {
				getJobInfo := func(_ context.Context, _ string) (*utiltypes.RayJobInfo, error) {
					return nil, apierrors.NewBadRequest("job not found")
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				Expect(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob)).Should(Succeed())
				recentTime := metav1.NewTime(time.Now().Add(-30 * time.Second))
				rayJob.Status.JobStatusCheckFailureStartTime = &recentTime
				rayJob.Status.JobStatus = rayv1.JobStatusNew
				Expect(k8sClient.Status().Update(ctx, rayJob)).Should(Succeed())

				Eventually(func() (*metav1.Time, error) {
					if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob); err != nil {
						return nil, err
					}
					return rayJob.Status.JobStatusCheckFailureStartTime, nil
				}, time.Second*10, time.Millisecond*500).Should(BeNil())

				Consistently(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning))
			})

			It("Marks the RayJob as Failed when a previously observed job no longer exists", func() {
				getJobInfo := func(_ context.Context, _ string) (*utiltypes.RayJobInfo, error) {
					return nil, apierrors.NewBadRequest("job not found")
				}
				fakeRayDashboardClient.GetJobInfoMock.Store(&getJobInfo)
				defer fakeRayDashboardClient.GetJobInfoMock.Store(nil)

				Expect(k8sClient.Get(ctx, client.ObjectKey{Name: rayJob.Name, Namespace: namespace}, rayJob)).Should(Succeed())
				rayJob.Status.JobStatus = rayv1.JobStatusRunning
				Expect(k8sClient.Status().Update(ctx, rayJob)).Should(Succeed())

				Eventually(
					getRayJobDeploymentStatus(ctx, rayJob),
					time.Second*10, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusFailed))
				Expect(rayJob.Status.Reason).To(Equal(rayv1.AppFailed))
			})
		})
	})
})
