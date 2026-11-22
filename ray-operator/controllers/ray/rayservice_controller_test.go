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
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	"github.com/ray-project/kuberay/ray-operator/test/support"
)

func serveConfigV2Template(serveAppName string) string {
	return fmt.Sprintf(`
    applications:
    - name: %s
      import_path: fruit.deployment_graph
      route_prefix: /fruit
      runtime_env:
        working_dir: "https://github.com/ray-project/test_dag/archive/41d09119cbdf8450599f993f51318e9e27c59098.zip"
      deployments:
        - name: MangoStand
          num_replicas: 1
          user_config:
            price: 3
          ray_actor_options:
            num_cpus: 0.1
        - name: OrangeStand
          num_replicas: 1
          user_config:
            price: 2
          ray_actor_options:
            num_cpus: 0.1
        - name: PearStand
          num_replicas: 1
          user_config:
            price: 1
          ray_actor_options:
            num_cpus: 0.1`, serveAppName)
}

func rayServiceTemplate(name string, namespace string, serveAppName string) *rayv1.RayService {
	serveConfigV2 := serveConfigV2Template(serveAppName)
	return &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				"foo": "bar",
			},
		},
		Spec: rayv1.RayServiceSpec{
			ServeConfigV2: serveConfigV2,
			UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
				Type: ptr.To(rayv1.RayServiceNewCluster),
			},
			RayClusterSpec: rayv1.RayClusterSpec{
				RayVersion: support.GetRayVersion(),
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "ray-head",
									Image: support.GetRayImage(),
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

func endpointSliceTemplate(name string, namespace string) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				discoveryv1.LabelServiceName: name,
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.9.8.7"},
				Conditions: discoveryv1.EndpointConditions{
					Ready: new(true),
				},
				// TargetRef should always be set in production to identify the backing Pod.
				// This allows proper deduplication when a Pod appears in multiple EndpointSlices.
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: namespace,
					Name:      "test-pod-1",
					UID:       "test-uid-12345",
				},
			},
		},
	}
}

func appendNewWorkerGroup(workerGroupSpecs []rayv1.WorkerGroupSpec, newGroupName string) []rayv1.WorkerGroupSpec {
	newWorkerGroupSpec := workerGroupSpecs[0].DeepCopy()
	newWorkerGroupSpec.GroupName = newGroupName
	return append(workerGroupSpecs, *newWorkerGroupSpec)
}

var _ = Context("RayService env tests", func() {
	Describe("RayService with a maximum name", Ordered, func() {
		ctx := context.Background()
		namespace := "default"
		serveAppName := "app1"
		rayService := rayServiceTemplate(strings.Repeat("s", utils.MaxRayServiceNameLength), namespace, serveAppName)
		rayCluster := &rayv1.RayCluster{}

		It("Create a RayService custom resource", func() {
			err := k8sClient.Create(ctx, rayService)
			Expect(err).NotTo(HaveOccurred(), "failed to create RayService resource")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "RayService: %v", rayService.Name)
		})

		It("Should create a pending RayCluster", func() {
			Eventually(
				getPreparingRayClusterNameFunc(ctx, rayService),
				time.Second*3, time.Millisecond*500).Should(Not(BeEmpty()), "Pending RayCluster name: %v", rayService.Status.PendingServiceStatus.RayClusterName)
		})

		It("Promote the pending RayCluster to the active RayCluster", func() {
			// Update the status of the head Pod to Running. Note that the default fake dashboard client
			// will return a healthy serve application status.
			pendingRayClusterName := rayService.Status.PendingServiceStatus.RayClusterName
			updateHeadPodToRunningAndReady(ctx, pendingRayClusterName, namespace)

			// Make sure the pending RayCluster becomes the active RayCluster.
			Eventually(
				getRayClusterNameFunc(ctx, rayService),
				time.Second*3, time.Millisecond*500).Should(Equal(pendingRayClusterName), "Active RayCluster name: %v", rayService.Status.ActiveServiceStatus.RayClusterName)

			// Initialize RayCluster for the following tests.
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayService.Status.ActiveServiceStatus.RayClusterName, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster: %v", rayCluster.Name)
		})

		It("Check RayService service names", func() {
			var svc corev1.Service
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name + "-head-svc", Namespace: namespace}, &svc),
				time.Second*3, time.Millisecond*500).Should(Succeed())
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name + "-serve-svc", Namespace: namespace}, &svc),
				time.Second*3, time.Millisecond*500).Should(Succeed())
		})
	})

	Describe("Autoscaler updates RayCluster should not trigger zero downtime upgrade", Ordered, func() {
		// If Autoscaler scales up the pending or active RayCluster, zero downtime upgrade should not be triggered.
		ctx := context.Background()
		namespace := "default"
		serveAppName := "app1"
		rayService := rayServiceTemplate("test-autoscaler", namespace, serveAppName)
		rayCluster := &rayv1.RayCluster{}

		It("Create a RayService custom resource", func() {
			err := k8sClient.Create(ctx, rayService)
			Expect(err).NotTo(HaveOccurred(), "failed to create RayService resource")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "RayService: %v", rayService.Name)
		})

		It("Should create a pending RayCluster", func() {
			Eventually(
				getPreparingRayClusterNameFunc(ctx, rayService),
				time.Second*15, time.Millisecond*500).Should(Not(BeEmpty()), "Pending RayCluster name: %v", rayService.Status.PendingServiceStatus.RayClusterName)
		})

		It("Autoscaler updates the pending RayCluster and should not switch to a new RayCluster", func() {
			// Simulate autoscaler by updating the pending RayCluster directly. Note that the autoscaler
			// will not update the RayService directly.
			clusterName, _ := getPreparingRayClusterNameFunc(ctx, rayService)()
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: clusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Pending RayCluster: %v", rayCluster.Name)
				*rayCluster.Spec.WorkerGroupSpecs[0].Replicas++
				return k8sClient.Update(ctx, rayCluster)
			})
			Expect(err).NotTo(HaveOccurred(), "Failed to update the pending RayCluster.")

			// Confirm not switch to a new RayCluster
			Consistently(
				getPreparingRayClusterNameFunc(ctx, rayService),
				time.Second*5, time.Millisecond*500).Should(Equal(clusterName), "Pending RayCluster: %v", rayService.Status.PendingServiceStatus.RayClusterName)
		})

		It("Promote the pending RayCluster to the active RayCluster", func() {
			// Update the status of the head Pod to Running. Note that the default fake dashboard client
			// will return a healthy serve application status.
			pendingRayClusterName := rayService.Status.PendingServiceStatus.RayClusterName
			updateHeadPodToRunningAndReady(ctx, pendingRayClusterName, namespace)

			// Make sure the pending RayCluster becomes the active RayCluster.
			Eventually(
				getRayClusterNameFunc(ctx, rayService),
				time.Second*15, time.Millisecond*500).Should(Equal(pendingRayClusterName), "Active RayCluster name: %v", rayService.Status.ActiveServiceStatus.RayClusterName)

			// Initialize RayCluster for the following tests.
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayService.Status.ActiveServiceStatus.RayClusterName, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster: %v", rayCluster.Name)
		})

		It("Autoscaler updates the active RayCluster and should not switch to a new RayCluster", func() {
			// Simulate autoscaler by updating the active RayCluster directly. Note that the autoscaler
			// will not update the RayService directly.
			clusterName, _ := getRayClusterNameFunc(ctx, rayService)()
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: clusterName, Namespace: namespace}, rayCluster),
					time.Second*3, time.Millisecond*500).Should(Succeed(), "Active RayCluster: %v", rayCluster.Name)
				*rayCluster.Spec.WorkerGroupSpecs[0].Replicas++
				return k8sClient.Update(ctx, rayCluster)
			})
			Expect(err).NotTo(HaveOccurred(), "Failed to update the active RayCluster.")

			// Confirm not switch to a new RayCluster
			Consistently(
				getRayClusterNameFunc(ctx, rayService),
				time.Second*5, time.Millisecond*500).Should(Equal(clusterName), "Active RayCluster: %v", rayService.Status.ActiveServiceStatus.RayClusterName)
		})
	})

	Describe("After a RayService is running", func() {
		ctx := context.Background()
		var rayService *rayv1.RayService
		var rayCluster *rayv1.RayCluster
		var endpointSlice *discoveryv1.EndpointSlice
		serveAppName := "app1"
		namespace := "default"

		BeforeEach(OncePerOrdered, func() {
			// This simulates the most common scenario in the RayService code path:
			// (1) Create a RayService custom resource
			// (2) The RayService controller creates a pending RayCluster
			// (3) The serve application becomes ready on the pending RayCluster
			// (4) The Kubernetes head and serve services are created
			// (5) The pending RayCluster transitions to become the active RayCluster
			rayService = rayServiceTemplate("test-base-path-"+strconv.Itoa(rand.IntN(1000)), namespace, serveAppName) //nolint:gosec // no need for cryptographically secure random number
			rayCluster = &rayv1.RayCluster{}

			By("Create a RayService custom resource")
			err := k8sClient.Create(ctx, rayService)
			Expect(err).NotTo(HaveOccurred(), "failed to create RayService resource")
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "RayService: %v", rayService.Name)

			By("Conditions should be initialized correctly")
			Eventually(
				func() bool {
					return meta.IsStatusConditionTrue(rayService.Status.Conditions, string(rayv1.UpgradeInProgress))
				},
				time.Second*3, time.Millisecond*500).Should(BeFalse(), "UpgradeInProgress condition: %v", rayService.Status.Conditions)
			Eventually(
				func() bool {
					return meta.IsStatusConditionTrue(rayService.Status.Conditions, string(rayv1.RayServiceReady))
				},
				time.Second*3, time.Millisecond*500).Should(BeFalse(), "RayServiceReady condition: %v", rayService.Status.Conditions)

			By("Should create a pending RayCluster")
			Eventually(
				getPreparingRayClusterNameFunc(ctx, rayService),
				time.Second*15, time.Millisecond*500).Should(Not(BeEmpty()), "Pending RayCluster name: %v", rayService.Status.PendingServiceStatus.RayClusterName)

			By("Promote the pending RayCluster to the active RayCluster")
			// Update the status of the head Pod to Running. Note that the default fake dashboard client
			// will return a healthy serve application status.
			pendingRayClusterName := rayService.Status.PendingServiceStatus.RayClusterName
			updateHeadPodToRunningAndReady(ctx, pendingRayClusterName, namespace)

			// Make sure the pending RayCluster becomes the active RayCluster.
			Eventually(
				getRayClusterNameFunc(ctx, rayService),
				time.Second*15, time.Millisecond*500).Should(Equal(pendingRayClusterName), "Active RayCluster name: %v", rayService.Status.ActiveServiceStatus.RayClusterName)

			// Initialize RayCluster for the following tests.
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: rayService.Status.ActiveServiceStatus.RayClusterName, Namespace: namespace}, rayCluster),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "RayCluster: %v", rayCluster.Name)

			// Make sure the annotations are populated to the RayCluster.
			Expect(rayCluster.Annotations).Should(Satisfy(func(annotations map[string]string) bool {
				for key, value := range rayService.Annotations {
					if val, ok := annotations[key]; !ok || val != value {
						return false
					}
				}
				return true
			}))

			By("Check the serve application status in the RayService status")
			// Check the serve application status in the RayService status.
			// The serve application should be healthy.
			Eventually(
				checkServiceHealth(ctx, rayService),
				time.Second*3, time.Millisecond*500).Should(BeTrue(), "RayService status: %v", rayService.Status)

			By("Should create a new head service resource")
			svc := &corev1.Service{}
			headSvcName, err := utils.GenerateHeadServiceName(utils.RayServiceCRD, rayService.Spec.RayClusterSpec, rayService.Name)
			Expect(err).ToNot(HaveOccurred())
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: headSvcName, Namespace: namespace}, svc),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Head service: %v", svc)
			// TODO: Verify the head service by checking labels and annotations.

			By("Should create a new serve service resource")
			svc = &corev1.Service{}
			Eventually(
				getResourceFunc(ctx, client.ObjectKey{Name: utils.GenerateServeServiceName(rayService.Name), Namespace: namespace}, svc),
				time.Second*3, time.Millisecond*500).Should(Succeed(), "Serve service: %v", svc)
			// TODO: Verify the serve service by checking labels and annotations.

			By("The RayServiceReady condition should be true when the number of endpoints is greater than 0")
			endpointSlice = endpointSliceTemplate(utils.GenerateServeServiceName(rayService.Name), namespace)
			err = k8sClient.Create(ctx, endpointSlice)
			Expect(err).NotTo(HaveOccurred(), "failed to create EndpointSlice resource")
			Eventually(func() int32 {
				if err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService); err != nil {
					return 0
				}
				return rayService.Status.NumServeEndpoints
			}, time.Second*3, time.Millisecond*500).Should(BeNumerically(">", 0), "RayService status: %v", rayService.Status)
			Expect(meta.IsStatusConditionTrue(rayService.Status.Conditions, string(rayv1.RayServiceReady))).Should(BeTrue())
		})

		AfterEach(OncePerOrdered, func() {
			By(fmt.Sprintf("Delete the RayService custom resource %v", rayService.Name))
			err := k8sClient.Delete(ctx, rayService)
			Expect(err).NotTo(HaveOccurred(), "failed to delete the test RayService resource")
			By(fmt.Sprintf("Delete the EndpointSlice %v", endpointSlice.Name))
			err = k8sClient.Delete(ctx, endpointSlice)
			Expect(err).NotTo(HaveOccurred(), "failed to delete the test EndpointSlice resource")
		})

		When("Testing in-place update: updating the serveConfigV2", Ordered, func() {
			// Update serveConfigV2 to trigger an in-place update. The new serve application should appear in the RayService status,
			// and a zero-downtime upgrade should not be triggered.
			var newConfigV2 string
			newServeAppName := "newAppName"

			BeforeAll(func() {
				newConfigV2 = serveConfigV2Template(newServeAppName)

				// Update the serveConfigV2 to trigger an in-place update.
				err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
					Eventually(
						getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService),
						time.Second*3, time.Millisecond*500).Should(Succeed(), "RayService: %v", rayService.Name)
					rayService.Spec.ServeConfigV2 = newConfigV2
					return k8sClient.Update(ctx, rayService)
				})

				Expect(err).NotTo(HaveOccurred(), "failed to update RayService resource")

				// Update the fake Ray dashboard client to return serve application statuses with the new serve application.
				healthyStatus := generateServeStatus(rayv1.DeploymentStatusEnum.HEALTHY, rayv1.ApplicationStatusEnum.RUNNING)
				fakeRayDashboardClient.SetMultiApplicationStatuses(map[string]*utiltypes.ServeApplicationStatus{newServeAppName: &healthyStatus})
			})

			It("New serve application should be shown in the RayService status", func() {
				Eventually(checkServeApplicationExists(ctx, rayService, "newAppName"), time.Second*10, time.Millisecond*500).Should(BeTrue())
			})

			It("Should create an UpdatedServeApplications event", func() {
				Eventually(func() bool {
					var eventList eventsv1.EventList
					err := k8sClient.List(ctx, &eventList, client.InNamespace(rayService.Namespace))
					if err != nil {
						return false
					}
					for _, event := range eventList.Items {
						if event.Regarding.UID == rayService.UID &&
							event.Reason == string(utils.UpdatedServeApplications) {
							return true
						}
					}
					return false
				}, time.Second*10, time.Millisecond*500).Should(BeTrue())
			})

			It("Refresh RayService", func() {
				err := k8sClient.Get(ctx, client.ObjectKey{Name: rayService.Name, Namespace: rayService.Namespace}, rayService)
				Expect(err).NotTo(HaveOccurred(), "failed to get RayService resource")
			})

			It("Should not create a new pending cluster", func() {
				Expect(rayService.Status.PendingServiceStatus.RayClusterName).To(BeEmpty())
			})

			It("Should not switch to a new active cluster", func() {
				Expect(rayService.Status.ActiveServiceStatus.RayClusterName).To(Equal(rayCluster.Name))
			})
		})

		When("Testing appending a new worker group to the active RayCluster", Ordered, func() {
			// When a RayService has only an active RayCluster and no pending RayCluster, appending a new worker group
			// to the RayService spec will update the active RayCluster CR instead of triggering a zero-downtime upgrade.
			BeforeAll(func() {
				// Verify RayService has only one worker group.
				Expect(rayService.Spec.RayClusterSpec.WorkerGroupSpecs).To(HaveLen(1))

				// Append a new worker group to the active RayCluster.
				workerGroupSpecs := appendNewWorkerGroup(rayService.Spec.RayClusterSpec.WorkerGroupSpecs, "worker-group-to-active-cluster")
				err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
					Eventually(
						getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService),
						time.Second*3, time.Millisecond*500).Should(Succeed(), "RayService: %v", rayService.Name)
					rayService.Spec.RayClusterSpec.WorkerGroupSpecs = workerGroupSpecs
					return k8sClient.Update(ctx, rayService)
				})
				Expect(err).NotTo(HaveOccurred(), "failed to update test RayService resource")
			})

			It("Should reflect the changes in the active cluster's WorkerGroupSpecs", func() {
				Eventually(
					getActiveRayClusterWorkerGroupSpecsFunc(ctx, rayService),
					time.Second*10, time.Millisecond*500).Should(HaveLen(2))
			})

			It("Should not create a new pending cluster", func() {
				Expect(rayService.Status.PendingServiceStatus.RayClusterName).To(BeEmpty())
			})

			It("Should not switch to a new active cluster", func() {
				clusterName := rayCluster.Name
				Consistently(
					getRayClusterNameFunc(ctx, rayService),
					time.Second*3, time.Millisecond*500).Should(Equal(clusterName), "Active RayCluster: %v", rayService.Status.ActiveServiceStatus.RayClusterName)
			})
		})

		When("During the zero-downtime upgrade process", func() {
			var pendingClusterName string

			BeforeEach(OncePerOrdered, func() {
				mockRayVersion := "mock-ray-version-1"
				err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
					Eventually(
						getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService),
						time.Second*3, time.Millisecond*500).Should(Succeed(), "RayService: %v", rayService.Name)
					Expect(rayService.Spec.RayClusterSpec.RayVersion).NotTo(Equal(mockRayVersion))
					rayService.Spec.RayClusterSpec.RayVersion = mockRayVersion
					return k8sClient.Update(ctx, rayService)
				})
				Expect(err).NotTo(HaveOccurred(), "failed to update test RayService resource")

				Eventually(
					getPreparingRayClusterNameFunc(ctx, rayService),
					time.Second*15, time.Millisecond*500).Should(Not(BeEmpty()), "Pending RayCluster name: %v", rayService.Status.PendingServiceStatus.RayClusterName)
				pendingClusterName = rayService.Status.PendingServiceStatus.RayClusterName

				// Wait until the pending RayCluster is created. If we update the RayService again before the cluster is created,
				// the RayCluster will be constructed based on the new RayService spec and zero-downtime upgrade will not be triggered.
				By(fmt.Sprintf("Waiting until the pending RayCluster %v is created", pendingClusterName))
				pendingCluster := &rayv1.RayCluster{}
				Eventually(
					getResourceFunc(ctx, client.ObjectKey{Name: pendingClusterName, Namespace: namespace}, pendingCluster),
					time.Second*15, time.Millisecond*500).Should(Succeed(), "Pending RayCluster: %v", pendingCluster.Name)
			})

			When("Promote the pending RayCluster to the active RayCluster", Ordered, func() {
				It("Update the head pod to Running and Ready", func() {
					updateHeadPodToRunningAndReady(ctx, pendingClusterName, namespace)
					Eventually(
						getRayClusterNameFunc(ctx, rayService),
						time.Second*60, time.Millisecond*500).Should(Equal(pendingClusterName), "Active RayCluster: %v", rayService.Status.ActiveServiceStatus.RayClusterName)
				})
			})

			When("Updating the RayVersion again to prepare a new pending cluster", Ordered, func() {
				BeforeAll(func() {
					mockRayVersion := "mock-ray-version-2"
					err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
						Eventually(
							getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService),
							time.Second*3, time.Millisecond*500).Should(Succeed(), "RayService: %v", rayService.Name)
						Expect(rayService.Spec.RayClusterSpec.RayVersion).NotTo(Equal(mockRayVersion))
						rayService.Spec.RayClusterSpec.RayVersion = mockRayVersion
						return k8sClient.Update(ctx, rayService)
					})
					Expect(err).NotTo(HaveOccurred(), "failed to update RayService resource")
				})

				It("Should prepare a new pending cluster", func() {
					Eventually(
						getPreparingRayClusterNameFunc(ctx, rayService),
						time.Second*10, time.Millisecond*500).ShouldNot(Equal(pendingClusterName), "Pending RayCluster name  = %v", rayService.Status.PendingServiceStatus.RayClusterName)
				})
			})

			When("Testing appending a new worker group to the pending RayCluster", Ordered, func() {
				BeforeAll(func() {
					// Verify RayService has only one worker group.
					Expect(rayService.Spec.RayClusterSpec.WorkerGroupSpecs).To(HaveLen(1))

					// Append a new worker group to the pending RayCluster.
					workerGroupSpecs := appendNewWorkerGroup(rayService.Spec.RayClusterSpec.WorkerGroupSpecs, "worker-group-to-active-cluster")
					err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
						Eventually(
							getResourceFunc(ctx, client.ObjectKey{Name: rayService.Name, Namespace: namespace}, rayService),
							time.Second*3, time.Millisecond*500).Should(Succeed(), "RayService: %v", rayService.Name)
						rayService.Spec.RayClusterSpec.WorkerGroupSpecs = workerGroupSpecs
						return k8sClient.Update(ctx, rayService)
					})
					Expect(err).NotTo(HaveOccurred(), "failed to update test RayService resource")
				})

				It("Should reflect the changes in the pending cluster's WorkerGroupSpecs", func() {
					Eventually(
						getPendingRayClusterWorkerGroupSpecsFunc(ctx, rayService),
						time.Second*15, time.Millisecond*500).Should(HaveLen(2))
				})

				It("Should not prepare a new pending cluster", func() {
					pendingClusterName := rayService.Status.PendingServiceStatus.RayClusterName
					Consistently(
						getPreparingRayClusterNameFunc(ctx, rayService),
						time.Second*3, time.Millisecond*500).Should(Equal(pendingClusterName), "Pending RayCluster: %v", rayService.Status.PendingServiceStatus.RayClusterName)
				})
			})
		})
	})

	Context("ClusterUpgradeOptions gatewayClassName/gatewayRef CEL validation", func() {
		ctx := context.Background()
		namespace := "default"
		serveAppName := "app"

		withIncrementalUpgradeOptions := func(rayService *rayv1.RayService, opts *rayv1.ClusterUpgradeOptions) *rayv1.RayService {
			rayService.Spec.UpgradeStrategy = &rayv1.RayServiceUpgradeStrategy{
				Type:                  ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
				ClusterUpgradeOptions: opts,
			}
			return rayService
		}
		baseOpts := func() *rayv1.ClusterUpgradeOptions {
			return &rayv1.ClusterUpgradeOptions{
				MaxSurgePercent: new(int32(100)),
				StepSizePercent: new(int32(10)),
				IntervalSeconds: new(int32(30)),
			}
		}

		It("accepts gatewayRef alone (gatewayClassName absent)", func() {
			opts := baseOpts()
			opts.GatewayRef = &rayv1.GatewayReference{Name: "shared-gateway"}
			rayService := withIncrementalUpgradeOptions(rayServiceTemplate("cel-gatewayref", namespace, serveAppName), opts)
			Expect(k8sClient.Create(ctx, rayService)).To(Succeed())
			Expect(k8sClient.Delete(ctx, rayService)).To(Succeed())
		})

		It("accepts gatewayClassName alone (gatewayRef absent)", func() {
			opts := baseOpts()
			opts.GatewayClassName = "istio"
			rayService := withIncrementalUpgradeOptions(rayServiceTemplate("cel-gatewayclass", namespace, serveAppName), opts)
			Expect(k8sClient.Create(ctx, rayService)).To(Succeed())
			Expect(k8sClient.Delete(ctx, rayService)).To(Succeed())
		})

		It("rejects both gatewayClassName and gatewayRef", func() {
			opts := baseOpts()
			opts.GatewayClassName = "istio"
			opts.GatewayRef = &rayv1.GatewayReference{Name: "shared-gateway"}
			rayService := withIncrementalUpgradeOptions(rayServiceTemplate("cel-both", namespace, serveAppName), opts)
			err := k8sClient.Create(ctx, rayService)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Exactly one of gatewayClassName or gatewayRef must be specified"))
		})

		It("rejects neither gatewayClassName nor gatewayRef", func() {
			rayService := withIncrementalUpgradeOptions(rayServiceTemplate("cel-neither", namespace, serveAppName), baseOpts())
			err := k8sClient.Create(ctx, rayService)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Exactly one of gatewayClassName or gatewayRef must be specified"))
		})

		It("rejects an explicit empty gatewayClassName with no gatewayRef", func() {
			opts := baseOpts()
			opts.GatewayClassName = "" // present but empty must count as "not set"
			rayService := withIncrementalUpgradeOptions(rayServiceTemplate("cel-empty-class", namespace, serveAppName), opts)
			err := k8sClient.Create(ctx, rayService)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("Exactly one of gatewayClassName or gatewayRef must be specified"))
		})

		It("accepts gatewayRef with an explicit empty gatewayClassName", func() {
			opts := baseOpts()
			opts.GatewayClassName = ""
			opts.GatewayRef = &rayv1.GatewayReference{Name: "shared-gateway"}
			rayService := withIncrementalUpgradeOptions(rayServiceTemplate("cel-ref-empty-class", namespace, serveAppName), opts)
			Expect(k8sClient.Create(ctx, rayService)).To(Succeed())
			Expect(k8sClient.Delete(ctx, rayService)).To(Succeed())
		})
	})
})
