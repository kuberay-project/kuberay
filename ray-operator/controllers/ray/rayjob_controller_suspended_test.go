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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
)

var _ = Context("RayJob with suspend operation", func() {
	Describe("When creating a rayjob with suspend == true", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayCluster := &rayv1.RayCluster{}
		rayJob := rayJobTemplate("rayjob-suspend", namespace)
		rayJob.Spec.Suspend = true

		It("should create a rayjob object", func() {
			err := k8sClient.Create(ctx, rayJob)
			Expect(err).NotTo(HaveOccurred(), "failed to create test RayJob resource")
		})

		It("should reach Suspended status without ever setting RayClusterName", func() {
			Consistently(
				getRayClusterNameForRayJob(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(BeEmpty())

			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*5, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusSuspended))

			Expect(getRayClusterNameForRayJob(ctx, rayJob)()).To(BeEmpty())
		})

		It("should unsuspend a rayjob object", func() {
			err := updateRayJobSuspendField(ctx, rayJob, false)
			Expect(err).NotTo(HaveOccurred(), "failed to update RayJob")
		})

		It("should create a raycluster object", func() {
			// Ray Cluster name can be present on RayJob's CRD
			Eventually(
				getRayClusterNameForRayJob(ctx, rayJob),
				time.Second*15, time.Millisecond*500).Should(Not(BeEmpty()))
			// The actual cluster instance and underlying resources SHOULD be created when suspend == false
			Eventually(

				getResourceFunc(ctx, common.RayJobRayClusterNamespacedName(rayJob), rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed())
		})

		It("should NOT create the underlying K8s job yet because the cluster is not ready", func() {
			underlyingK8sJob := &batchv1.Job{}
			Consistently(
				// k8sClient client throws error if resource not found
				func() bool {
					err := getResourceFunc(ctx, common.RayJobK8sJobNamespacedName(rayJob), underlyingK8sJob)()
					return errors.IsNotFound(err)
				},
				time.Second*3, time.Millisecond*500).Should(BeTrue())
		})

		It("should be able to update all Pods to Running", func() {
			updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
			updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
		})

		It("Dashboard URL should be set", func() {
			Eventually(
				getDashboardURLForRayJob(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(HavePrefix(rayJob.Name), "Dashboard URL = %v", rayJob.Status.DashboardURL)
		})

		It("should create the underlying Kubernetes Job object", func() {
			underlyingK8sJob := &batchv1.Job{}
			// The underlying Kubernetes Job should be created when the RayJob is created
			Eventually(
				getResourceFunc(ctx, common.RayJobK8sJobNamespacedName(rayJob), underlyingK8sJob),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Expected Kubernetes job to be present")
		})
	})

	Describe("RayJob suspend operation shoud be atomic", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		rayJob := rayJobTemplate("rayjob-atomic-suspend", namespace)
		rayCluster := &rayv1.RayCluster{}

		It("Create a RayJob custom resource", func() {
			err := k8sClient.Create(ctx, rayJob)
			Expect(err).NotTo(HaveOccurred(), "Failed to create RayJob")
		})

		It("RayJobs's JobDeploymentStatus transitions from New to Initializing.", func() {
			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusInitializing), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
		})

		It("Make RayCluster.Status.State to be rayv1.Ready", func() {
			updateHeadPodToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
			updateWorkerPodsToRunningAndReady(ctx, rayJob.Status.RayClusterName, namespace)
			Eventually(
				getClusterState(ctx, namespace, rayJob.Status.RayClusterName),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.Ready))
		})

		It("RayJobs's JobDeploymentStatus transitions from Initializing to Running.", func() {
			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusRunning), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
		})

		// The finalizer here is used to prevent the RayCluster from being deleted,
		// ensuring that the RayJob remains in Suspending status once the suspend field is set to true.
		It("Add finalizer to the RayCluster", func() {
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				err := k8sClient.Get(ctx, common.RayJobRayClusterNamespacedName(rayJob), rayCluster)
				if err != nil {
					return err
				}
				rayCluster.Finalizers = append(rayCluster.Finalizers, "ray.io/deletion-blocker")
				return k8sClient.Update(ctx, rayCluster)
			})
			Expect(err).NotTo(HaveOccurred(), "failed to add finalizer to RayCluster")
		})

		It("Suspend the RayJob", func() {
			err := updateRayJobSuspendField(ctx, rayJob, true)
			Expect(err).NotTo(HaveOccurred(), "failed to update RayJob")
		})

		It("RayJobs's JobDeploymentStatus transitions from Running to Suspending.", func() {
			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusSuspending), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
		})

		// The suspend operation is atomic; regardless of how the user sets the suspend field at this moment, the status should be Suspending.
		It("Change the suspend field of RayJob from true to false and then back to true.", func() {
			err := updateRayJobSuspendField(ctx, rayJob, false)
			Expect(err).NotTo(HaveOccurred(), "failed to update RayJob")
			Consistently(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusSuspending), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)

			err = updateRayJobSuspendField(ctx, rayJob, true)
			Expect(err).NotTo(HaveOccurred(), "failed to update RayJob")
			Consistently(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusSuspending), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
		})

		It("Remove finalizer from the RayCluster", func() {
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				err := k8sClient.Get(ctx, common.RayJobRayClusterNamespacedName(rayJob), rayCluster)
				if err != nil {
					return err
				}
				rayCluster.Finalizers = []string{}
				return k8sClient.Update(ctx, rayCluster)
			})
			Expect(err).NotTo(HaveOccurred(), "failed to remove finalizer from RayCluster")
		})

		It("RayJobs's JobDeploymentStatus transitions from Suspending to Suspended.", func() {
			Eventually(
				getRayJobDeploymentStatus(ctx, rayJob),
				time.Second*3, time.Millisecond*500).Should(Equal(rayv1.JobDeploymentStatusSuspended), "JobDeploymentStatus = %v", rayJob.Status.JobDeploymentStatus)
		})
	})

	Describe("When RayJob is suspended and should not requeue", Ordered, func() {
		var reconciler *RayJobReconciler
		var rayJob *rayv1.RayJob
		namespace := "default"

		BeforeAll(func() {
			newScheme := runtime.NewScheme()
			_ = rayv1.AddToScheme(newScheme)
			_ = corev1.AddToScheme(newScheme)

			rayJob = rayJobTemplate("test-suspended-rayjob", namespace)
			rayJob.Spec.Suspend = true
			rayJob.Status = rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusSuspended,
				JobStatus:           rayv1.JobStatusNew,
			}

			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(rayJob).WithStatusSubresource(rayJob).Build()
			recorder := events.NewFakeRecorder(100)

			reconciler = &RayJobReconciler{
				Client:   fakeClient,
				Recorder: recorder,
				Scheme:   newScheme,
			}
		})

		It("should reconcile without error", func() {
			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      rayJob.Name,
					Namespace: rayJob.Namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(Equal(time.Duration(0)), "Expected no requeue delay when RayJob is suspended")
		})

		It("should not modify the status", func() {
			updatedRayJob := &rayv1.RayJob{}
			err := reconciler.Client.Get(context.Background(), types.NamespacedName{
				Name:      rayJob.Name,
				Namespace: rayJob.Namespace,
			}, updatedRayJob)

			Expect(err).NotTo(HaveOccurred())
			Expect(updatedRayJob.Status.JobDeploymentStatus).To(Equal(rayv1.JobDeploymentStatusSuspended), "Expected JobDeploymentStatus to remain Suspended")
		})
	})

	Describe("When suspended RayJob transitions to New status", Ordered, func() {
		var reconciler *RayJobReconciler
		var rayJob *rayv1.RayJob
		namespace := "default"

		BeforeAll(func() {
			newScheme := runtime.NewScheme()
			_ = rayv1.AddToScheme(newScheme)
			_ = corev1.AddToScheme(newScheme)

			rayJob = rayJobTemplate("test-suspended-transition-rayjob", namespace)
			rayJob.Spec.Suspend = false
			rayJob.Status = rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusSuspended,
				JobStatus:           rayv1.JobStatusNew,
			}

			client := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(rayJob).WithStatusSubresource(rayJob).Build()
			recorder := events.NewFakeRecorder(100)

			reconciler = &RayJobReconciler{
				Client:   client,
				Recorder: recorder,
				Scheme:   newScheme,
			}
		})

		It("should reconcile without error and trigger requeue", func() {
			result, err := reconciler.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Name:      rayJob.Name,
					Namespace: rayJob.Namespace,
				},
			})

			Expect(err).NotTo(HaveOccurred())
			Expect(result.RequeueAfter).To(BeNumerically(">", 0), "Expected to requeue after transitioning from Suspended to New")
		})

		It("should transition to New status", func() {
			updatedRayJob := &rayv1.RayJob{}
			err := reconciler.Client.Get(context.Background(), types.NamespacedName{
				Name:      rayJob.Name,
				Namespace: rayJob.Namespace,
			}, updatedRayJob)

			Expect(err).NotTo(HaveOccurred())
			Expect(updatedRayJob.Status.JobDeploymentStatus).To(Equal(rayv1.JobDeploymentStatusNew), "Expected JobDeploymentStatus to transition to New")
			Expect(updatedRayJob.Status.JobStatus).To(Equal(rayv1.JobStatusNew), "Expected JobStatus to transition to New")
		})
	})
})
