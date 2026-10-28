package e2ekuberneteswas

import (
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// newWASRayClusterAC builds a RayCluster apply configuration opted in to gang
// scheduling via the ray.io/gang-scheduling-enabled label.
func newWASRayClusterAC(name, namespace string) *rayv1ac.RayClusterApplyConfiguration {
	return rayv1ac.RayCluster(name, namespace).
		WithLabels(map[string]string{utils.RayGangSchedulingEnabled: "true"})
}

func TestKubernetesWAS_CreatesWorkloadAndPodGroups(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("native-sched", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	LogWithTimestamp(test.T(), "Verifying Workload %s/%s exists", namespace.Name, rayCluster.Name)
	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())

	g.Expect(workload.Spec.ControllerRef).NotTo(BeNil())
	g.Expect(workload.Spec.ControllerRef.APIGroup).To(Equal("ray.io"))
	g.Expect(workload.Spec.ControllerRef.Kind).To(Equal("RayCluster"))
	g.Expect(workload.Spec.ControllerRef.Name).To(Equal(rayCluster.Name))

	g.Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(workload.Spec.PodGroupTemplates[0].Name).To(Equal("cluster"))
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
	// MinCount = 1 head + 1 worker replica.
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(2)))

	g.Expect(workload.OwnerReferences).To(HaveLen(1))
	g.Expect(workload.OwnerReferences[0].Kind).To(Equal("RayCluster"))
	g.Expect(workload.OwnerReferences[0].Name).To(Equal(rayCluster.Name))
	g.Expect(*workload.OwnerReferences[0].Controller).To(BeTrue())
	g.Expect(workload.Labels[utils.RayClusterLabelKey]).To(Equal(rayCluster.Name))

	LogWithTimestamp(test.T(), "Verifying the whole-cluster PodGroup exists")
	clusterPodGroup, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(clusterPodGroup.Spec.WorkloadRef).NotTo(BeNil())
	g.Expect(clusterPodGroup.Spec.WorkloadRef.WorkloadName).To(Equal(rayCluster.Name))
	g.Expect(clusterPodGroup.Spec.WorkloadRef.TemplateName).To(Equal("cluster"))
	g.Expect(clusterPodGroup.Spec.SchedulingPolicy.Gang).NotTo(BeNil())
	g.Expect(clusterPodGroup.Spec.SchedulingPolicy.Gang.MinCount).To(Equal(int32(2)))
	g.Expect(clusterPodGroup.OwnerReferences).To(HaveLen(1))
	g.Expect(clusterPodGroup.OwnerReferences[0].Kind).To(Equal("RayCluster"))
	g.Expect(clusterPodGroup.OwnerReferences[0].Name).To(Equal(rayCluster.Name))
	g.Expect(*clusterPodGroup.OwnerReferences[0].Controller).To(BeTrue())
	g.Expect(clusterPodGroup.Labels[utils.RayClusterLabelKey]).To(Equal(rayCluster.Name))

	LogWithTimestamp(test.T(), "Verifying PodGroupInitiallyScheduled condition on the PodGroup")
	g.Eventually(PodGroup(test, namespace.Name, rayCluster.Name+"-cluster"), TestTimeoutShort).
		Should(WithTransform(func(pg *schedulingv1alpha3.PodGroup) bool {
			return meta.IsStatusConditionTrue(pg.Status.Conditions, schedulingv1alpha3.PodGroupInitiallyScheduled)
		}, BeTrue()))
}

func TestKubernetesWAS_PodSchedulingGroup(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("sched-group", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod.Spec.SchedulerName).To(Equal(corev1.DefaultSchedulerName))
	g.Expect(headPod.Spec.SchedulingGroup).NotTo(BeNil())
	g.Expect(headPod.Spec.SchedulingGroup.PodGroupName).NotTo(BeNil())
	g.Expect(*headPod.Spec.SchedulingGroup.PodGroupName).To(Equal(rayCluster.Name + "-cluster"))

	workerPods, err := GetWorkerPods(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workerPods).NotTo(BeEmpty())
	for _, pod := range workerPods {
		g.Expect(pod.Spec.SchedulerName).To(Equal(corev1.DefaultSchedulerName))
		g.Expect(pod.Spec.SchedulingGroup).NotTo(BeNil())
		g.Expect(pod.Spec.SchedulingGroup.PodGroupName).NotTo(BeNil())
		g.Expect(*pod.Spec.SchedulingGroup.PodGroupName).To(Equal(rayCluster.Name + "-cluster"))
	}
}

func TestKubernetesWAS_MultipleWorkerGroups(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("multi-wg", namespace.Name).
		WithSpec(rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(HeadPodTemplateApplyConfiguration())).
			WithWorkerGroupSpecs(
				rayv1ac.WorkerGroupSpec().
					WithReplicas(1).
					WithMinReplicas(1).
					WithMaxReplicas(1).
					WithGroupName("group-a").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(WorkerPodTemplateApplyConfiguration()),
				rayv1ac.WorkerGroupSpec().
					WithReplicas(2).
					WithMinReplicas(2).
					WithMaxReplicas(2).
					WithGroupName("group-b").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(WorkerPodTemplateApplyConfiguration()),
			))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with 2 worker groups", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(workload.Spec.PodGroupTemplates[0].Name).To(Equal("cluster"))
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
	// MinCount = 1 head + 1 (group-a) + 2 (group-b) worker replicas.
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(4)))

	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))

	allWorkerPods, err := GetWorkerPods(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	for _, pod := range allWorkerPods {
		g.Expect(pod.Spec.SchedulingGroup).NotTo(BeNil(), "pod %s missing schedulingGroup", pod.Name)
		g.Expect(pod.Spec.SchedulingGroup.PodGroupName).NotTo(BeNil(), "pod %s missing podGroupName", pod.Name)
		g.Expect(*pod.Spec.SchedulingGroup.PodGroupName).To(Equal(rayCluster.Name+"-cluster"),
			"pod %s has wrong podGroupName", pod.Name)
	}
}

func TestKubernetesWAS_AutoscalingSkipped(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("autoscale-skip", namespace.Name).
		WithSpec(NewRayClusterSpec().WithEnableInTreeAutoscaling(true))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with autoscaling", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to start reconciling", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(StatusCondition(rayv1.HeadPodReady), MatchCondition(metav1.ConditionTrue, rayv1.HeadPodRunningAndReady)))

	_, err = GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(errors.IsNotFound(err)).To(BeTrue(), "expected NotFound for Workload, got: %v", err)

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod.Spec.SchedulerName).To(Equal(corev1.DefaultSchedulerName))
	g.Expect(headPod.Spec.SchedulingGroup).To(BeNil(), "head pod should not have schedulingGroup when autoscaling is enabled")
}

func TestKubernetesWAS_GangSchedules(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("gang-sched", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready (gang scheduling)", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	allPods, err := GetAllPods(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(allPods).NotTo(BeEmpty())
	for _, pod := range allPods {
		g.Expect(pod.Status.Phase).To(Equal(corev1.PodRunning))
	}

	_, err = GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Verifying PodGroupInitiallyScheduled condition on the whole-cluster PodGroup")
	g.Eventually(PodGroup(test, namespace.Name, rayCluster.Name+"-cluster"), TestTimeoutShort).
		Should(WithTransform(func(pg *schedulingv1alpha3.PodGroup) bool {
			return meta.IsStatusConditionTrue(pg.Status.Conditions, schedulingv1alpha3.PodGroupInitiallyScheduled)
		}, BeTrue()))
}

func TestKubernetesWAS_OwnerReferenceGC(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("gc-test", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	_, err = GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))

	LogWithTimestamp(test.T(), "Deleting RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)
	err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for Workload to be deleted")
	g.Eventually(func() bool {
		_, err := GetWorkload(test, namespace.Name, rayCluster.Name)
		return errors.IsNotFound(err)
	}, TestTimeoutShort).Should(BeTrue())

	LogWithTimestamp(test.T(), "Waiting for PodGroups to be deleted")
	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(BeEmpty())
}

func TestKubernetesWAS_Idempotent(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("idempotent", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	g.Eventually(Workloads(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))
	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))

	LogWithTimestamp(test.T(), "Verifying resource counts remain stable over time")
	g.Consistently(Workloads(test, namespace.Name), 10*time.Second, time.Second).Should(HaveLen(1))
	g.Consistently(PodGroups(test, namespace.Name), 10*time.Second, time.Second).Should(HaveLen(1))
}

func TestKubernetesWAS_SuspendPreservesResources(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("suspend-keep", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Eventually(Workloads(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))
	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))

	LogWithTimestamp(test.T(), "Suspending RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)
	rayClusterAC = rayClusterAC.WithSpec(rayClusterAC.Spec.WithSuspend(true))
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to be suspended", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(StatusCondition(rayv1.RayClusterSuspended), MatchCondition(metav1.ConditionTrue, string(rayv1.RayClusterSuspended))))

	LogWithTimestamp(test.T(), "Verifying the suspended RayCluster has no Pods")
	g.Eventually(func(inner Gomega) {
		workerPods, err := GetWorkerPods(test, rayCluster)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(workerPods).To(BeEmpty())
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying Workload and PodGroup persist while suspended")
	g.Consistently(Workloads(test, namespace.Name), 10*time.Second, time.Second).Should(HaveLen(1))
	g.Consistently(PodGroups(test, namespace.Name), 10*time.Second, time.Second).Should(HaveLen(1))
}

func TestKubernetesWAS_ResumeReusesResources(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("resume-reuse", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))
	g.Eventually(Workloads(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))
	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	originalWorkloadUID := workload.UID
	podGroup, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	originalPodGroupUID := podGroup.UID

	LogWithTimestamp(test.T(), "Suspending RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)
	rayClusterAC = rayClusterAC.WithSpec(rayClusterAC.Spec.WithSuspend(true))
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(StatusCondition(rayv1.RayClusterSuspended), MatchCondition(metav1.ConditionTrue, string(rayv1.RayClusterSuspended))))

	LogWithTimestamp(test.T(), "Verifying Workload and PodGroup persist while suspended")
	g.Consistently(Workloads(test, namespace.Name), 10*time.Second, time.Second).Should(HaveLen(1))
	g.Consistently(PodGroups(test, namespace.Name), 10*time.Second, time.Second).Should(HaveLen(1))

	LogWithTimestamp(test.T(), "Resuming RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)
	rayClusterAC = rayClusterAC.WithSpec(rayClusterAC.Spec.WithSuspend(false))
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready after resume", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	LogWithTimestamp(test.T(), "Verifying the Workload is reused (same UID) after resume")
	w, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(w.UID).To(Equal(originalWorkloadUID), "Workload should be reused with the same UID after resume")
	g.Expect(w.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(w.Spec.PodGroupTemplates[0].Name).To(Equal("cluster"))
	g.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
	g.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(2)))

	LogWithTimestamp(test.T(), "Verifying the PodGroup is reused (same UID) after resume")
	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))
	pg, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(pg.UID).To(Equal(originalPodGroupUID), "PodGroup should be reused with the same UID after resume")

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod.Spec.SchedulingGroup).NotTo(BeNil())
	g.Expect(headPod.Spec.SchedulingGroup.PodGroupName).NotTo(BeNil())
	g.Expect(*headPod.Spec.SchedulingGroup.PodGroupName).To(Equal(rayCluster.Name + "-cluster"))
}

func TestKubernetesWAS_ScaleUpRecreatesWorkload(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("scale-up", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	originalUID := workload.UID
	g.Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
	// MinCount = 1 head + 1 worker replica.
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(2)))

	LogWithTimestamp(test.T(), "Scaling up worker replicas from 1 to 3")
	rayClusterAC.Spec.WorkerGroupSpecs[0].WithReplicas(3).WithMinReplicas(3).WithMaxReplicas(3)
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready after scale-up", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(func(inner Gomega) {
		rc, err := GetRayCluster(test, namespace.Name, rayCluster.Name)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(RayClusterState(rc)).To(Equal(rayv1.Ready))
		inner.Expect(RayClusterDesiredWorkerReplicas(rc)).To(Equal(int32(3)))
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying Workload was recreated with updated minCount")
	g.Eventually(func(inner Gomega) {
		w, err := GetWorkload(test, namespace.Name, rayCluster.Name)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(w.UID).NotTo(Equal(originalUID), "Workload should have been recreated with a new UID")
		inner.Expect(w.Spec.PodGroupTemplates).To(HaveLen(1))
		inner.Expect(w.Spec.PodGroupTemplates[0].Name).To(Equal("cluster"))
		inner.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
		// MinCount = 1 head + 3 worker replicas.
		inner.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(4)))
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Waiting for the whole-cluster PodGroup to be recreated with updated minCount")
	g.Eventually(func() int32 {
		podGroup, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
		if err != nil || podGroup.DeletionTimestamp != nil || podGroup.Spec.SchedulingPolicy.Gang == nil {
			return -1
		}
		return podGroup.Spec.SchedulingPolicy.Gang.MinCount
	}, TestTimeoutShort).Should(Equal(int32(4)))
}

func TestKubernetesWAS_ScaleDownRecreatesWorkload(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("scale-down", namespace.Name).
		WithSpec(rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(HeadPodTemplateApplyConfiguration())).
			WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
				WithReplicas(3).
				WithMinReplicas(3).
				WithMaxReplicas(3).
				WithGroupName("small-group").
				WithRayStartParams(map[string]string{"num-cpus": "1"}).
				WithTemplate(WorkerPodTemplateApplyConfiguration())))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with 3 replicas", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	originalUID := workload.UID
	g.Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
	// MinCount = 1 head + 3 worker replicas.
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(4)))

	LogWithTimestamp(test.T(), "Scaling down worker replicas from 3 to 1")
	rayClusterAC.Spec.WorkerGroupSpecs[0].WithReplicas(1).WithMinReplicas(1).WithMaxReplicas(1)
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready after scale-down", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(func(inner Gomega) {
		rc, err := GetRayCluster(test, namespace.Name, rayCluster.Name)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(RayClusterState(rc)).To(Equal(rayv1.Ready))
		inner.Expect(RayClusterDesiredWorkerReplicas(rc)).To(Equal(int32(1)))
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying Workload was recreated with updated minCount")
	g.Eventually(func(inner Gomega) {
		w, err := GetWorkload(test, namespace.Name, rayCluster.Name)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(w.UID).NotTo(Equal(originalUID), "Workload should have a new UID after scale-down")
		inner.Expect(w.Spec.PodGroupTemplates).To(HaveLen(1))
		inner.Expect(w.Spec.PodGroupTemplates[0].Name).To(Equal("cluster"))
		inner.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
		// MinCount = 1 head + 1 worker replica.
		inner.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(2)))
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Waiting for the whole-cluster PodGroup to be recreated with updated minCount")
	g.Eventually(func() int32 {
		podGroup, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
		if err != nil || podGroup.DeletionTimestamp != nil || podGroup.Spec.SchedulingPolicy.Gang == nil {
			return -1
		}
		return podGroup.Spec.SchedulingPolicy.Gang.MinCount
	}, TestTimeoutShort).Should(Equal(int32(2)))
}

func TestKubernetesWAS_AddWorkerGroupRecreatesWorkload(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("add-wg", namespace.Name).
		WithSpec(NewRayClusterSpec())

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with 1 worker group", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	originalUID := workload.UID
	g.Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))

	LogWithTimestamp(test.T(), "Adding second worker group 'gpu-group' to RayCluster")
	rayClusterAC.Spec.WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
		WithReplicas(2).
		WithMinReplicas(2).
		WithMaxReplicas(2).
		WithGroupName("gpu-group").
		WithRayStartParams(map[string]string{"num-cpus": "1"}).
		WithTemplate(WorkerPodTemplateApplyConfiguration()))
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready after adding worker group", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	LogWithTimestamp(test.T(), "Verifying Workload was recreated with a single whole-cluster PodGroupTemplate")
	g.Eventually(func(inner Gomega) {
		w, err := GetWorkload(test, namespace.Name, rayCluster.Name)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(w.UID).NotTo(Equal(originalUID), "Workload should have a new UID after adding worker group")
		inner.Expect(w.Spec.PodGroupTemplates).To(HaveLen(1))
		inner.Expect(w.Spec.PodGroupTemplates[0].Name).To(Equal("cluster"))
		inner.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
		// MinCount = 1 head + 1 (small-group) + 2 (gpu-group) worker replicas.
		inner.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(4)))
	}, TestTimeoutShort).Should(Succeed())

	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))
	_, err = GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
	g.Expect(err).NotTo(HaveOccurred())
}

// TestKubernetesWAS_GangAtomicityIncludesHead verifies that the head pod is part
// of the whole-cluster gang: when the gang cannot be satisfied, the head pod is
// held Pending instead of being scheduled independently (the previous behavior,
// where the head used a Basic policy).
func TestKubernetesWAS_GangAtomicityIncludesHead(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	// A single worker group requests far more memory than any node can provide, so
	// the whole-cluster gang's minCount can never be met.
	unschedulableWorker := corev1ac.PodTemplateSpec().
		WithSpec(corev1ac.PodSpec().
			WithContainers(corev1ac.Container().
				WithName("ray-worker").
				WithImage(GetRayImage()).
				WithResources(corev1ac.ResourceRequirements().
					WithRequests(corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("100000Gi"),
					}))))

	rayClusterAC := newWASRayClusterAC("gang-atomic", namespace.Name).
		WithSpec(rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(HeadPodTemplateApplyConfiguration())).
			WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
				WithReplicas(1).
				WithMinReplicas(1).
				WithMaxReplicas(1).
				WithGroupName("huge-group").
				WithRayStartParams(map[string]string{"num-cpus": "1"}).
				WithTemplate(unschedulableWorker)))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created unschedulable RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Verifying the Workload and whole-cluster PodGroup are created")
	g.Eventually(func() error {
		_, err := GetWorkload(test, namespace.Name, rayCluster.Name)
		return err
	}, TestTimeoutShort).Should(Succeed())
	g.Eventually(func() error {
		_, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
		return err
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Waiting for the head pod to be created")
	g.Eventually(func() error {
		_, err := GetHeadPod(test, rayCluster)
		return err
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying the head pod stays Pending because the gang is unsatisfiable")
	g.Consistently(func(inner Gomega) {
		headPod, err := GetHeadPod(test, rayCluster)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(headPod.Status.Phase).To(Equal(corev1.PodPending))
	}, 20*time.Second, 2*time.Second).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying the PodGroup never reports Scheduled")
	g.Consistently(PodGroup(test, namespace.Name, rayCluster.Name+"-cluster"), 10*time.Second, 2*time.Second).
		Should(WithTransform(func(pg *schedulingv1alpha3.PodGroup) bool {
			return meta.IsStatusConditionTrue(pg.Status.Conditions, schedulingv1alpha3.PodGroupInitiallyScheduled)
		}, BeFalse()))
}

// TestKubernetesWAS_GangHoldsSchedulablePods verifies all-or-nothing scheduling
// with multiple replicas: worker pods that would schedule on their own are held
// Pending because another member of the whole-cluster gang is unschedulable, so
// the gang's minCount can never be met and no pod in the cluster is scheduled.
func TestKubernetesWAS_GangHoldsSchedulablePods(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	// One worker requests far more memory than any node can provide, so the
	// whole-cluster gang can never be satisfied.
	unschedulableWorker := corev1ac.PodTemplateSpec().
		WithSpec(corev1ac.PodSpec().
			WithContainers(corev1ac.Container().
				WithName("ray-worker").
				WithImage(GetRayImage()).
				WithResources(corev1ac.ResourceRequirements().
					WithRequests(corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("100000Gi"),
					}))))

	rayClusterAC := newWASRayClusterAC("gang-holds", namespace.Name).
		WithSpec(rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(HeadPodTemplateApplyConfiguration())).
			WithWorkerGroupSpecs(
				// These workers would schedule on their own (see other tests).
				rayv1ac.WorkerGroupSpec().
					WithReplicas(2).
					WithMinReplicas(2).
					WithMaxReplicas(2).
					WithGroupName("schedulable").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(WorkerPodTemplateApplyConfiguration()),
				// This one cannot, which makes the whole gang unschedulable.
				rayv1ac.WorkerGroupSpec().
					WithReplicas(1).
					WithMinReplicas(1).
					WithMaxReplicas(1).
					WithGroupName("unschedulable").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(unschedulableWorker)))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created partially-schedulable RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)

	// minCount = 1 head + 2 schedulable + 1 unschedulable = 4 pods.
	const expectedPods = 4
	LogWithTimestamp(test.T(), "Waiting for all %d pods to be created", expectedPods)
	g.Eventually(func() ([]corev1.Pod, error) {
		return GetAllPods(test, rayCluster)
	}, TestTimeoutMedium).Should(HaveLen(expectedPods))

	LogWithTimestamp(test.T(), "Verifying no pod is scheduled while the gang is unsatisfiable")
	g.Consistently(func(inner Gomega) {
		pods, err := GetAllPods(test, rayCluster)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(pods).To(HaveLen(expectedPods))
		for _, pod := range pods {
			inner.Expect(pod.Spec.NodeName).To(BeEmpty(), "pod %s should not be scheduled to a node", pod.Name)
			inner.Expect(pod.Status.Phase).To(Equal(corev1.PodPending), "pod %s should be Pending", pod.Name)
		}
	}, 20*time.Second, 2*time.Second).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying the PodGroup never reports Scheduled")
	g.Consistently(PodGroup(test, namespace.Name, rayCluster.Name+"-cluster"), 10*time.Second, 2*time.Second).
		Should(WithTransform(func(pg *schedulingv1alpha3.PodGroup) bool {
			return meta.IsStatusConditionTrue(pg.Status.Conditions, schedulingv1alpha3.PodGroupInitiallyScheduled)
		}, BeFalse()))
}

// TestKubernetesWAS_ManyWorkerGroups verifies that a RayCluster with more than
// seven worker groups schedules successfully. The previous per-worker-group
// design capped worker groups at seven (a Workload allows only eight PodGroup
// templates, one of which was the head); the single whole-cluster PodGroup
// removes that limit.
func TestKubernetesWAS_ManyWorkerGroups(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	const workerGroupCount = 8

	clusterSpec := rayv1ac.RayClusterSpec().
		WithRayVersion(GetRayVersion()).
		WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
			WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
			WithTemplate(HeadPodTemplateApplyConfiguration()))
	for i := range workerGroupCount {
		clusterSpec = clusterSpec.WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
			WithReplicas(1).
			WithMinReplicas(1).
			WithMaxReplicas(1).
			WithGroupName(fmt.Sprintf("group-%d", i)).
			WithRayStartParams(map[string]string{"num-cpus": "1"}).
			WithTemplate(WorkerPodTemplateApplyConfiguration()))
	}

	rayClusterAC := newWASRayClusterAC("many-wg", namespace.Name).WithSpec(clusterSpec)
	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(rayCluster.Spec.WorkerGroupSpecs).To(HaveLen(workerGroupCount))
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with %d worker groups", rayCluster.Namespace, rayCluster.Name, workerGroupCount)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutLong).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(workload.Spec.PodGroupTemplates[0].Name).To(Equal("cluster"))
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
	// MinCount = 1 head + one replica per worker group.
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(1 + workerGroupCount)))

	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))

	g.Eventually(func(inner Gomega) {
		allWorkerPods, err := GetWorkerPods(test, rayCluster)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(allWorkerPods).To(HaveLen(workerGroupCount))
		for _, pod := range allWorkerPods {
			inner.Expect(pod.Spec.SchedulingGroup).NotTo(BeNil(), "pod %s missing schedulingGroup", pod.Name)
			inner.Expect(pod.Spec.SchedulingGroup.PodGroupName).NotTo(BeNil(), "pod %s missing podGroupName", pod.Name)
			inner.Expect(*pod.Spec.SchedulingGroup.PodGroupName).To(Equal(rayCluster.Name+"-cluster"),
				"pod %s has wrong podGroupName", pod.Name)
		}
	}, TestTimeoutShort).Should(Succeed())
}

// TestKubernetesWAS_SuspendSingleWorkerGroup verifies that suspending one worker
// group of several recomputes the whole-cluster gang minCount and recreates the
// Workload and PodGroup while the head and the remaining worker group keep
// running. Per-worker-group suspension requires the RayJobDeletionPolicy feature
// gate, which the kubernetes-was-v1alpha3 overlay enables.
func TestKubernetesWAS_SuspendSingleWorkerGroup(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("suspend-wg", namespace.Name).
		WithSpec(rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(HeadPodTemplateApplyConfiguration())).
			WithWorkerGroupSpecs(
				rayv1ac.WorkerGroupSpec().
					WithReplicas(1).
					WithMinReplicas(1).
					WithMaxReplicas(1).
					WithGroupName("group-a").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(WorkerPodTemplateApplyConfiguration()),
				rayv1ac.WorkerGroupSpec().
					WithReplicas(2).
					WithMinReplicas(2).
					WithMaxReplicas(2).
					WithGroupName("group-b").
					WithRayStartParams(map[string]string{"num-cpus": "1"}).
					WithTemplate(WorkerPodTemplateApplyConfiguration()),
			))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with 2 worker groups", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
	// MinCount = 1 head + 1 (group-a) + 2 (group-b).
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(4)))
	originalWorkloadUID := workload.UID

	LogWithTimestamp(test.T(), "Suspending worker group 'group-b'")
	rayClusterAC.Spec.WorkerGroupSpecs[1].WithSuspend(true)
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Verifying the Workload was recreated with the reduced minCount")
	g.Eventually(func(inner Gomega) {
		w, err := GetWorkload(test, namespace.Name, rayCluster.Name)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(w.UID).NotTo(Equal(originalWorkloadUID), "Workload should be recreated after suspend")
		inner.Expect(w.Spec.PodGroupTemplates).To(HaveLen(1))
		inner.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
		// MinCount = 1 head + 1 (group-a); group-b is suspended and contributes 0.
		inner.Expect(w.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(2)))
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying the PodGroup minCount was updated")
	g.Eventually(func() int32 {
		podGroup, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
		if err != nil || podGroup.DeletionTimestamp != nil || podGroup.Spec.SchedulingPolicy.Gang == nil {
			return -1
		}
		return podGroup.Spec.SchedulingPolicy.Gang.MinCount
	}, TestTimeoutShort).Should(Equal(int32(2)))

	LogWithTimestamp(test.T(), "Verifying group-b pods were deleted while head and group-a keep running")
	g.Eventually(func(inner Gomega) {
		groupBPods, err := GetGroupPods(test, rayCluster, "group-b")
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(groupBPods).To(BeEmpty())
	}, TestTimeoutShort).Should(Succeed())

	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	g.Eventually(func(inner Gomega) {
		headPod, err := GetHeadPod(test, rayCluster)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(headPod.Status.Phase).To(Equal(corev1.PodRunning))
		inner.Expect(headPod.Spec.SchedulingGroup).NotTo(BeNil())
		inner.Expect(headPod.Spec.SchedulingGroup.PodGroupName).NotTo(BeNil())
		inner.Expect(*headPod.Spec.SchedulingGroup.PodGroupName).To(Equal(rayCluster.Name + "-cluster"))

		groupAPods, err := GetGroupPods(test, rayCluster, "group-a")
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(groupAPods).To(HaveLen(1))
		inner.Expect(groupAPods[0].Status.Phase).To(Equal(corev1.PodRunning))
		inner.Expect(groupAPods[0].Spec.SchedulingGroup).NotTo(BeNil())
		inner.Expect(groupAPods[0].Spec.SchedulingGroup.PodGroupName).NotTo(BeNil())
		inner.Expect(*groupAPods[0].Spec.SchedulingGroup.PodGroupName).To(Equal(rayCluster.Name + "-cluster"))
	}, TestTimeoutShort).Should(Succeed())
}

// TestKubernetesWAS_RecreateUpgradeReusesResources verifies that a Recreate
// upgrade (a pod-spec change that forces all pods to be recreated) does not
// disturb the whole-cluster Workload and PodGroup when the gang size is
// unchanged: the recreated pods rejoin the existing PodGroup.
func TestKubernetesWAS_RecreateUpgradeReusesResources(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	workerTemplate := func(marker string) *corev1ac.PodTemplateSpecApplyConfiguration {
		return WorkerPodTemplateApplyConfiguration().WithAnnotations(map[string]string{"was-test/upgrade-marker": marker})
	}

	rayClusterAC := newWASRayClusterAC("recreate-upg", namespace.Name).
		WithSpec(rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithUpgradeStrategy(rayv1ac.RayClusterUpgradeStrategy().WithType(rayv1.RayClusterRecreate)).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(HeadPodTemplateApplyConfiguration())).
			WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
				WithReplicas(1).
				WithMinReplicas(1).
				WithMaxReplicas(1).
				WithGroupName("small-group").
				WithRayStartParams(map[string]string{"num-cpus": "1"}).
				WithTemplate(workerTemplate("v1"))))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with Recreate upgrade strategy", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	originalWorkloadUID := workload.UID
	podGroup, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	originalPodGroupUID := podGroup.UID

	LogWithTimestamp(test.T(), "Changing the worker pod template to trigger a Recreate upgrade")
	rayClusterAC.Spec.WorkerGroupSpecs[0].WithTemplate(workerTemplate("v2"))
	_, err = test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())

	LogWithTimestamp(test.T(), "Waiting for the Recreate upgrade to settle and the cluster to return to ready")
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	LogWithTimestamp(test.T(), "Verifying the Workload and PodGroup are reused (same UID) because the gang size is unchanged")
	w, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(w.UID).To(Equal(originalWorkloadUID), "Workload should be reused because the Recreate upgrade did not change the gang size")
	g.Expect(w.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(w.Spec.PodGroupTemplates[0].Name).To(Equal("cluster"))

	g.Eventually(PodGroups(test, namespace.Name), TestTimeoutShort).Should(HaveLen(1))
	pg, err := GetPodGroup(test, namespace.Name, rayCluster.Name+"-cluster")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(pg.UID).To(Equal(originalPodGroupUID), "PodGroup should be reused because the Recreate upgrade did not change the gang size")
}

// TestKubernetesWAS_MultiHostWorkerGroup verifies that a multi-host worker group
// (NumOfHosts > 1) contributes replicas*numOfHosts pods to the whole-cluster gang
// minCount, and that every host pod joins the single cluster PodGroup.
func TestKubernetesWAS_MultiHostWorkerGroup(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	rayClusterAC := newWASRayClusterAC("multi-host", namespace.Name).
		WithSpec(rayv1ac.RayClusterSpec().
			WithRayVersion(GetRayVersion()).
			WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
				WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
				WithTemplate(HeadPodTemplateApplyConfiguration())).
			WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
				WithReplicas(1).
				WithMinReplicas(1).
				WithMaxReplicas(1).
				WithNumOfHosts(2).
				WithGroupName("multi-host-group").
				WithRayStartParams(map[string]string{"num-cpus": "1"}).
				WithTemplate(WorkerPodTemplateApplyConfiguration())))

	rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s with a multi-host worker group (numOfHosts=2)", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, namespace.Name, rayCluster.Name), TestTimeoutMedium).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	workload, err := GetWorkload(test, namespace.Name, rayCluster.Name)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(workload.Spec.PodGroupTemplates).To(HaveLen(1))
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(BeNil())
	// MinCount = 1 head + (1 replica * 2 hosts) = 3.
	g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(Equal(int32(3)))

	g.Eventually(func(inner Gomega) {
		workerPods, err := GetWorkerPods(test, rayCluster)
		inner.Expect(err).NotTo(HaveOccurred())
		inner.Expect(workerPods).To(HaveLen(2))
		for _, pod := range workerPods {
			inner.Expect(pod.Spec.SchedulingGroup).NotTo(BeNil(), "pod %s missing schedulingGroup", pod.Name)
			inner.Expect(pod.Spec.SchedulingGroup.PodGroupName).NotTo(BeNil(), "pod %s missing podGroupName", pod.Name)
			inner.Expect(*pod.Spec.SchedulingGroup.PodGroupName).To(Equal(rayCluster.Name+"-cluster"),
				"pod %s has wrong podGroupName", pod.Name)
		}
	}, TestTimeoutShort).Should(Succeed())
}
