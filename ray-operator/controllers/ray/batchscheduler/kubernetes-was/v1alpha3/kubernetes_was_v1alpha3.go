package v1alpha3

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	kuberneteswas "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/kubernetes-was"
	batchschedulerutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	podGroupProtectionFinalizer = "scheduling.k8s.io/podgroup-protection"
	// clusterPodGroupTemplateName is the name of the single PodGroupTemplate that
	// gang schedules the entire RayCluster (head + all worker groups) together.
	clusterPodGroupTemplateName = "cluster"
)

const (
	skipReasonGangSchedulingDisabled = "gang scheduling not enabled on RayCluster"
)

type KubernetesWASV1Alpha3Scheduler struct {
	cli client.Client
}

// Provider implements kuberneteswas.Provider for scheduling.k8s.io/v1alpha3.
type Provider struct{}

func init() {
	kuberneteswas.RegisterProvider(&Provider{})
}

func (k *KubernetesWASV1Alpha3Scheduler) Name() string { return kuberneteswas.GetPluginName() }

func (k *KubernetesWASV1Alpha3Scheduler) DoBatchSchedulingOnSubmission(ctx context.Context, object metav1.Object) error {
	rayCluster, ok := object.(*rayv1.RayCluster)
	if !ok {
		return nil
	}

	if reason := schedulingSkipReason(rayCluster); reason != "" {
		ctrl.LoggerFrom(ctx).WithName(kuberneteswas.GetPluginName()).Info("Skipping Kubernetes workload-aware scheduling", "reason", reason)
		_, err := k.CleanupOnCompletion(ctx, rayCluster)
		return err
	}

	return k.syncSchedulingResources(ctx, rayCluster)
}

func (k *KubernetesWASV1Alpha3Scheduler) AddMetadataToChildResource(_ context.Context, parent metav1.Object, child metav1.Object, _ string) {
	rayCluster, ok := parent.(*rayv1.RayCluster)
	if !ok || schedulingSkipReason(rayCluster) != "" {
		return
	}
	batchschedulerutils.AddSchedulerNameToObject(child, corev1.DefaultSchedulerName)
	// The entire RayCluster (head + every worker group) is gang scheduled as a
	// single PodGroup, so all pods reference the same PodGroup regardless of group.
	setSchedulingGroup(child, clusterPodGroupName(rayCluster.Name))
}

func (k *KubernetesWASV1Alpha3Scheduler) CleanupOnCompletion(ctx context.Context, object metav1.Object) (bool, error) {
	rayCluster, ok := object.(*rayv1.RayCluster)
	if !ok {
		return false, nil
	}
	return k.deleteSchedulingResources(ctx, rayCluster)
}

// The methods below adapt this package to kuberneteswas.Provider.

func (p *Provider) GroupVersion() schema.GroupVersion {
	return schedulingv1alpha3.SchemeGroupVersion
}

func (p *Provider) Available(config *rest.Config) error {
	return schedulingV1alpha3Available(config)
}

func (p *Provider) AddToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(schedulingv1alpha3.AddToScheme(scheme))
}

func (p *Provider) NewScheduler(cli client.Client) schedulerinterface.BatchScheduler {
	return &KubernetesWASV1Alpha3Scheduler{cli: cli}
}

func (p *Provider) ConfigureReconciler(b *builder.Builder) *builder.Builder {
	return b.Owns(&schedulingv1alpha3.Workload{}).
		Owns(&schedulingv1alpha3.PodGroup{})
}

// syncSchedulingResources creates the Workload and PodGroup on the first reconcile and
// patches gang.minCount in place on later reconciles (v1alpha3 minCount is mutable).
func (k *KubernetesWASV1Alpha3Scheduler) syncSchedulingResources(ctx context.Context, rayCluster *rayv1.RayCluster) error {
	workload, podGroup, err := k.buildSchedulingResources(rayCluster)
	if err != nil {
		return fmt.Errorf("failed to build scheduling resources for RayCluster %s/%s: %w", rayCluster.Namespace, rayCluster.Name, err)
	}
	if err := k.syncWorkload(ctx, rayCluster, workload); err != nil {
		return err
	}
	return k.syncPodGroup(ctx, rayCluster, podGroup)
}

func (k *KubernetesWASV1Alpha3Scheduler) syncWorkload(ctx context.Context, rayCluster *rayv1.RayCluster, desired *schedulingv1alpha3.Workload) error {
	existing := &schedulingv1alpha3.Workload{}
	found, err := k.getSchedulingResource(ctx, "Workload", client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		return err
	}
	if !found {
		if err := k.cli.Create(ctx, desired); err != nil {
			return fmt.Errorf("failed to create Workload %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}
	// A same-named Workload we do not own is a name collision; fail loudly rather
	// than fight its real owner every reconcile.
	// TODO: also emit a Warning event once the scheduler plugin has an event recorder.
	if !metav1.IsControlledBy(existing, rayCluster) {
		return fmt.Errorf("Workload %s/%s already exists and is not owned by this RayCluster; rename it or use a different RayCluster name to avoid the collision", existing.Namespace, existing.Name)
	}
	if existing.DeletionTimestamp != nil {
		return fmt.Errorf("Workload %s/%s is being deleted, will retry", existing.Namespace, existing.Name)
	}
	// gang.minCount is mutable in v1alpha3, so a RayCluster resize edits minCount on the
	// existing Workload in place instead of deleting and recreating it.
	// A RayCluster currently maps to a single PodGroup, so the Workload has exactly one template.
	existingGang := workloadClusterGang(existing)
	desiredMinCount := desired.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount
	if !gangNeedsMinCountPatch(existingGang, desiredMinCount) {
		return nil
	}
	patch := client.MergeFromWithOptions(existing.DeepCopy(), client.MergeFromWithOptimisticLock{})
	existingGang.MinCount = desiredMinCount
	if err := k.cli.Patch(ctx, existing, patch); err != nil {
		return fmt.Errorf("failed to patch Workload %s/%s minCount: %w", existing.Namespace, existing.Name, err)
	}
	return nil
}

func (k *KubernetesWASV1Alpha3Scheduler) syncPodGroup(ctx context.Context, rayCluster *rayv1.RayCluster, desired *schedulingv1alpha3.PodGroup) error {
	existing := &schedulingv1alpha3.PodGroup{}
	found, err := k.getSchedulingResource(ctx, "PodGroup", client.ObjectKeyFromObject(desired), existing)
	if err != nil {
		return err
	}
	if !found {
		if err := k.cli.Create(ctx, desired); err != nil {
			return fmt.Errorf("failed to create PodGroup %s/%s: %w", desired.Namespace, desired.Name, err)
		}
		return nil
	}
	// A same-named PodGroup we do not own is a name collision; fail loudly rather
	// than fight its real owner every reconcile.
	// TODO: also emit a Warning event once the scheduler plugin has an event recorder.
	if !metav1.IsControlledBy(existing, rayCluster) {
		return fmt.Errorf("PodGroup %s/%s already exists and is not owned by this RayCluster; rename it or use a different RayCluster name to avoid the collision", existing.Namespace, existing.Name)
	}
	// A terminating PodGroup stays usable; Kubernetes releases it once no pods reference it.
	// gang.minCount is mutable in v1alpha3, so a RayCluster resize edits minCount on the
	// existing PodGroup in place.
	existingGang := existing.Spec.SchedulingPolicy.Gang
	desiredMinCount := desired.Spec.SchedulingPolicy.Gang.MinCount
	if !gangNeedsMinCountPatch(existingGang, desiredMinCount) {
		return nil
	}
	patch := client.MergeFromWithOptions(existing.DeepCopy(), client.MergeFromWithOptimisticLock{})
	existingGang.MinCount = desiredMinCount
	if err := k.cli.Patch(ctx, existing, patch); err != nil {
		return fmt.Errorf("failed to patch PodGroup %s/%s minCount: %w", existing.Namespace, existing.Name, err)
	}
	return nil
}

func (k *KubernetesWASV1Alpha3Scheduler) deletePodGroup(ctx context.Context, podGroup *schedulingv1alpha3.PodGroup) (bool, error) {
	// Kubernetes uses this finalizer to protect a PodGroup while Pods still
	// reference it. The only time KubeRay deletes an owned PodGroup while Pods
	// are still referencing it is when gang scheduling is disabled on a running
	// RayCluster. Without removing the finalizer the PodGroup will stay Terminating.
	didDelete := controllerutil.RemoveFinalizer(podGroup, podGroupProtectionFinalizer)
	if didDelete {
		if err := k.cli.Update(ctx, podGroup); err != nil {
			if errors.IsNotFound(err) {
				return true, nil
			}
			return false, fmt.Errorf("failed to remove finalizer from PodGroup %s/%s: %w", podGroup.Namespace, podGroup.Name, err)
		}
	}
	if podGroup.DeletionTimestamp != nil {
		return didDelete, nil
	}
	if err := k.deleteWithUIDPrecondition(ctx, podGroup); err != nil {
		if errors.IsNotFound(err) {
			return didDelete, nil
		}
		return didDelete, fmt.Errorf("failed to delete PodGroup %s/%s: %w", podGroup.Namespace, podGroup.Name, err)
	}
	return true, nil
}

// buildClusterSchedulingPolicy gang schedules the whole cluster at the gang floor.
func buildClusterSchedulingPolicy(rayCluster *rayv1.RayCluster) schedulingv1alpha3.PodGroupSchedulingPolicy {
	return schedulingv1alpha3.PodGroupSchedulingPolicy{
		Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: clusterGangMinCount(rayCluster)},
	}
}

// gangNeedsMinCountPatch reports whether the live gang size differs from the desired size.
// A nil gang is left alone because the gang policy cannot be added after creation.
func gangNeedsMinCountPatch(existing *schedulingv1alpha3.GangSchedulingPolicy, desiredMinCount int32) bool {
	return existing != nil && existing.MinCount != desiredMinCount
}

// workloadClusterGang returns the gang policy of the single whole-cluster template, or nil.
// A RayCluster currently maps to a single PodGroup, so a template count other than 1 is a
// shape we don't know how to patch; this will change when composite pod groups land.
func workloadClusterGang(workload *schedulingv1alpha3.Workload) *schedulingv1alpha3.GangSchedulingPolicy {
	if len(workload.Spec.PodGroupTemplates) != 1 {
		return nil
	}
	return workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang
}

// clusterGangMinCount is the whole-cluster gang floor: the full desired size
// (1 head + all desired workers) normally, or 1 head + minReplicas when the Ray
// autoscaler is enabled so it can grow above the floor without deadlocking the gang.
func clusterGangMinCount(rayCluster *rayv1.RayCluster) int32 {
	if utils.IsAutoscalingEnabled(&rayCluster.Spec) {
		return int32(1) + utils.CalculateMinReplicas(rayCluster)
	}
	return int32(1) + utils.CalculateDesiredReplicas(rayCluster)
}

func (k *KubernetesWASV1Alpha3Scheduler) buildSchedulingResources(rayCluster *rayv1.RayCluster) (*schedulingv1alpha3.Workload, *schedulingv1alpha3.PodGroup, error) {
	policy := buildClusterSchedulingPolicy(rayCluster)
	workload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rayCluster.Name,
			Namespace: rayCluster.Namespace,
			Labels: map[string]string{
				utils.RayClusterLabelKey: rayCluster.Name,
			},
		},
		Spec: schedulingv1alpha3.WorkloadSpec{
			// ControllerRef is a back-reference to the owning RayCluster for tooling; it is
			// distinct from the owner reference set via SetControllerReference (used for GC).
			ControllerRef: &schedulingv1alpha3.TypedLocalObjectReference{
				APIGroup: rayv1.GroupVersion.Group,
				Kind:     "RayCluster",
				Name:     rayCluster.Name,
			},
			PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{{
				Name:             clusterPodGroupTemplateName,
				SchedulingPolicy: policy,
			}},
		},
	}
	podGroup := &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterPodGroupName(rayCluster.Name),
			Namespace: rayCluster.Namespace,
			Labels: map[string]string{
				utils.RayClusterLabelKey: rayCluster.Name,
			},
		},
		Spec: schedulingv1alpha3.PodGroupSpec{
			WorkloadRef: &schedulingv1alpha3.WorkloadReference{
				WorkloadName: rayCluster.Name,
				TemplateName: clusterPodGroupTemplateName,
			},
			SchedulingPolicy: policy,
		},
	}

	for _, object := range []client.Object{workload, podGroup} {
		if err := ctrl.SetControllerReference(rayCluster, object, k.cli.Scheme()); err != nil {
			return nil, nil, err
		}
	}
	return workload, podGroup, nil
}

// deleteSchedulingResources tears down the owned PodGroup before the Workload (dependency
// order). While teardown is in progress it returns a non-nil error so the caller requeues;
// the bool reports whether anything was actually deleted on this pass.
func (k *KubernetesWASV1Alpha3Scheduler) deleteSchedulingResources(ctx context.Context, rayCluster *rayv1.RayCluster) (bool, error) {
	podGroup := &schedulingv1alpha3.PodGroup{}
	podGroupKey := client.ObjectKey{Name: clusterPodGroupName(rayCluster.Name), Namespace: rayCluster.Namespace}
	podGroupFound, err := k.getSchedulingResource(ctx, "PodGroup", podGroupKey, podGroup)
	if err != nil {
		return false, err
	}
	// Only act on resources we own; a same-named foreign object is ignored.
	podGroupExists := podGroupFound && metav1.IsControlledBy(podGroup, rayCluster)

	workload := &schedulingv1alpha3.Workload{}
	workloadKey := client.ObjectKey{Name: rayCluster.Name, Namespace: rayCluster.Namespace}
	workloadFound, err := k.getSchedulingResource(ctx, "Workload", workloadKey, workload)
	if err != nil {
		return false, err
	}
	workloadExists := workloadFound && metav1.IsControlledBy(workload, rayCluster)

	didDelete := false
	if podGroupExists {
		var err error
		didDelete, err = k.deletePodGroup(ctx, podGroup)
		if err != nil {
			return didDelete, err
		}
		return didDelete, fmt.Errorf("waiting for PodGroup %s/%s to finish deleting", podGroupKey.Namespace, podGroupKey.Name)
	}

	if !workloadExists {
		return didDelete, nil
	}
	if workload.DeletionTimestamp != nil {
		return didDelete, fmt.Errorf("Workload %s/%s is being deleted, will retry", workload.Namespace, workload.Name)
	}
	if err := k.deleteWithUIDPrecondition(ctx, workload); err != nil {
		if !errors.IsNotFound(err) {
			return didDelete, fmt.Errorf("failed to delete Workload %s/%s: %w", workload.Namespace, workload.Name, err)
		}
	} else {
		didDelete = true
	}

	return didDelete, nil
}

func (k *KubernetesWASV1Alpha3Scheduler) getSchedulingResource(ctx context.Context, kind string, key client.ObjectKey, object client.Object) (bool, error) {
	if err := k.cli.Get(ctx, key, object); err != nil {
		if errors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to get %s %s: %w", kind, key, err)
	}
	return true, nil
}

func (k *KubernetesWASV1Alpha3Scheduler) deleteWithUIDPrecondition(ctx context.Context, object client.Object) error {
	uid := object.GetUID()
	return k.cli.Delete(ctx, object, client.Preconditions{UID: &uid})
}

func schedulingSkipReason(rayCluster *rayv1.RayCluster) string {
	// Gang scheduling is opt-in per RayCluster via the gang-scheduling label.
	if !strings.EqualFold(rayCluster.GetLabels()[utils.RayGangSchedulingEnabled], "true") {
		return skipReasonGangSchedulingDisabled
	}
	return ""
}

func clusterPodGroupName(clusterName string) string {
	return clusterName + "-" + clusterPodGroupTemplateName
}

func setSchedulingGroup(obj metav1.Object, podGroupName string) {
	switch obj := obj.(type) {
	case *corev1.Pod:
		obj.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: &podGroupName}
	case *corev1.PodTemplateSpec:
		obj.Spec.SchedulingGroup = &corev1.PodSchedulingGroup{PodGroupName: &podGroupName}
	}
}

func schedulingV1alpha3Available(config *rest.Config) error {
	if config == nil {
		return nil
	}
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create discovery client: %w", err)
	}
	if _, err := discoveryClient.ServerResourcesForGroupVersion(schedulingv1alpha3.SchemeGroupVersion.String()); err != nil {
		return fmt.Errorf("scheduling.k8s.io/v1alpha3 API is not available: %w", err)
	}
	return nil
}
