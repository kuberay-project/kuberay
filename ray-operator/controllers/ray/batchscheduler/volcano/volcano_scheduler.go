package volcano

import (
	"context"
	"fmt"
	"maps"
	"strconv"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	quotav1 "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	volcanobatchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	volcanoschedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

const (
	pluginName                                = "volcano"
	QueueNameLabelKey                         = "volcano.sh/queue-name"
	NetworkTopologyModeLabelKey               = "volcano.sh/network-topology-mode"
	NetworkTopologyHighestTierAllowedLabelKey = "volcano.sh/network-topology-highest-tier-allowed"
)

type VolcanoBatchScheduler struct {
	cli client.Client
}

type VolcanoBatchSchedulerFactory struct{}

func GetPluginName() string { return pluginName }

func (v *VolcanoBatchScheduler) Name() string {
	return GetPluginName()
}

func (v *VolcanoBatchScheduler) DoBatchSchedulingOnSubmission(ctx context.Context, object metav1.Object) error {
	switch obj := object.(type) {
	case *rayv1.RayCluster:
		return v.handleRayCluster(ctx, obj)
	case *rayv1.RayJob:
		return v.handleRayJob(ctx, obj)
	default:
		return fmt.Errorf("unsupported object type %T, only RayCluster and RayJob are supported", object)
	}
}

// handleRayCluster calculates the PodGroup MinMember and MinResources for a RayCluster
func (v *VolcanoBatchScheduler) handleRayCluster(ctx context.Context, raycluster *rayv1.RayCluster) error {
	// A RayCluster created by a RayJob does not own its PodGroup. The RayJob creates it
	// from the same cluster template and updates it during the supported RayJob lifecycle.
	if crdType, ok := raycluster.Labels[utils.RayOriginatedFromCRDLabelKey]; ok && crdType == utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD) {
		return nil
	}

	minMember, totalResource := v.calculatePodGroupParams(&raycluster.Spec)
	subGroupPolicy := calculateSubGroupPolicy(raycluster, &raycluster.Spec)

	_, err := v.syncPodGroup(ctx, raycluster, minMember, totalResource, subGroupPolicy)
	return err
}

// handleRayJob calculates the PodGroup MinMember and MinResources for a RayJob
func (v *VolcanoBatchScheduler) handleRayJob(ctx context.Context, rayJob *rayv1.RayJob) error {
	if rayJob.Spec.RayClusterSpec == nil {
		return fmt.Errorf("gang scheduling does not support RayJob %s/%s referencing an existing RayCluster", rayJob.Namespace, rayJob.Name)
	}

	var totalResourceList []corev1.ResourceList
	minMember, totalResource := v.calculatePodGroupParams(rayJob.Spec.RayClusterSpec)
	subGroupPolicy := calculateSubGroupPolicy(rayJob, rayJob.Spec.RayClusterSpec)
	totalResourceList = append(totalResourceList, totalResource)

	// MinMember intentionally excludes the submitter pod to avoid a startup deadlock
	// (submitter waits for cluster; gang would wait for submitter). We still add the
	// submitter's resource requests into MinResources so capacity is reserved.
	submitterResource := getSubmitterResource(rayJob)
	totalResourceList = append(totalResourceList, submitterResource)
	_, err := v.syncPodGroup(ctx, rayJob, minMember, utils.SumResourceList(totalResourceList), subGroupPolicy)
	return err
}

func getSubmitterResource(rayJob *rayv1.RayJob) corev1.ResourceList {
	switch rayJob.Spec.SubmissionMode {
	case rayv1.K8sJobMode:
		submitterTemplate := common.GetSubmitterTemplate(&rayJob.Spec, rayJob.Spec.RayClusterSpec)
		return utils.CalculatePodResource(submitterTemplate.Spec)
	case rayv1.SidecarMode:
		submitterContainer := common.GetDefaultSubmitterContainer(rayJob.Spec.RayClusterSpec)
		containerResource := submitterContainer.Resources.Requests
		for name, quantity := range submitterContainer.Resources.Limits {
			if _, ok := containerResource[name]; !ok {
				containerResource[name] = quantity
			}
		}
		return containerResource
	default:
		return corev1.ResourceList{}
	}
}

func getAppPodGroupName(object metav1.Object) string {
	// Prefer the RayJob name if this object originated from a RayJob
	name := object.GetName()
	if labels := object.GetLabels(); labels != nil &&
		labels[utils.RayOriginatedFromCRDLabelKey] == utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD) {
		if rayJobName := labels[utils.RayOriginatedFromCRNameLabelKey]; rayJobName != "" {
			name = rayJobName
		}
	}
	return fmt.Sprintf("ray-%s-pg", name)
}

func addSchedulerName(obj metav1.Object, schedulerName string) {
	switch obj := obj.(type) {
	case *corev1.Pod:
		obj.Spec.SchedulerName = schedulerName
	case *corev1.PodTemplateSpec:
		obj.Spec.SchedulerName = schedulerName
	}
}

func populateAnnotations(parent metav1.Object, child metav1.Object, groupName string) {
	annotations := child.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[volcanoschedulingv1beta1.KubeGroupNameAnnotationKey] = getAppPodGroupName(parent)
	annotations[volcanobatchv1alpha1.TaskSpecKey] = groupName
	child.SetAnnotations(annotations)
}

func populateLabelsFromObject(parent metav1.Object, child metav1.Object, key string) {
	labels := child.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	if parentLabel, exist := parent.GetLabels()[key]; exist && parentLabel != "" {
		labels[key] = parentLabel
	}
	child.SetLabels(labels)
}

// syncPodGroup ensures a Volcano PodGroup exists/updated for the given object
// with the provided size (MinMember), total resources, and subgroup policy.
// It returns true if the PodGroup was created or updated, false if no changes were needed.
func (v *VolcanoBatchScheduler) syncPodGroup(ctx context.Context, owner metav1.Object, size int32, totalResource corev1.ResourceList, subGroupPolicy []volcanoschedulingv1beta1.SubGroupPolicySpec) (createdOrUpdated bool, err error) {
	logger := ctrl.LoggerFrom(ctx).WithName(pluginName)

	createdOrUpdated = false
	podGroupName := getAppPodGroupName(owner)
	podGroup := volcanoschedulingv1beta1.PodGroup{}
	if err = v.cli.Get(ctx, types.NamespacedName{Namespace: owner.GetNamespace(), Name: podGroupName}, &podGroup); err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "failed to get PodGroup", "podGroupName", podGroupName, "ownerKind", utils.GetCRDType(owner.GetLabels()[utils.RayOriginatedFromCRDLabelKey]), "ownerName", owner.GetName(), "ownerNamespace", owner.GetNamespace())
			return
		}

		podGroup, err = createPodGroup(owner, podGroupName, size, totalResource, subGroupPolicy)
		if err != nil {
			logger.Error(err, "Failed to create pod group specification", "PodGroup.Error", err)
			return
		}
		if err = v.cli.Create(ctx, &podGroup); err != nil {
			if errors.IsAlreadyExists(err) {
				logger.Info("podGroup already exists, no need to create", "name", podGroupName)
				err = nil
				return
			}

			logger.Error(err, "failed to create PodGroup", "name", podGroupName, "ownerKind", utils.GetCRDType(owner.GetLabels()[utils.RayOriginatedFromCRDLabelKey]), "ownerName", owner.GetName(), "ownerNamespace", owner.GetNamespace())
			return
		}
		createdOrUpdated = true
		return
	}

	// Pre-v1.14 Volcano CRDs prune SubGroupPolicy, so a nonempty desired policy causes
	// a redundant PUT on each reconciliation while global gang settings remain usable.
	// We accept this overhead to avoid the long-term maintenance of capability detection,
	// cached or persistent state, and upgrade invalidation/re-detection logic. Keeping this
	// path stateless lets the next reconciliation persist the policy after a CRD upgrade
	// without restarting KubeRay. Upgrade Volcano and its matching CRDs to use subgroup
	// scheduling and avoid these redundant requests.
	if podGroup.Spec.MinMember != size || podGroup.Spec.MinResources == nil || !quotav1.Equals(*podGroup.Spec.MinResources, totalResource) || !apiequality.Semantic.DeepEqual(podGroup.Spec.SubGroupPolicy, subGroupPolicy) {
		podGroup.Spec.MinMember = size
		podGroup.Spec.MinResources = &totalResource
		podGroup.Spec.SubGroupPolicy = subGroupPolicy
		if err = v.cli.Update(ctx, &podGroup); err != nil {
			logger.Error(err, "failed to update PodGroup", "name", podGroupName, "ownerKind", utils.GetCRDType(owner.GetLabels()[utils.RayOriginatedFromCRDLabelKey]), "ownerName", owner.GetName(), "ownerNamespace", owner.GetNamespace())
			return
		}
		createdOrUpdated = true
		return
	}

	return
}

func (v *VolcanoBatchScheduler) calculatePodGroupParams(rayClusterSpec *rayv1.RayClusterSpec) (int32, corev1.ResourceList) {
	rayCluster := &rayv1.RayCluster{Spec: *rayClusterSpec}

	if !utils.IsAutoscalingEnabled(rayClusterSpec) {
		return utils.CalculateDesiredReplicas(rayCluster) + 1, utils.CalculateDesiredResources(rayCluster)
	}
	return utils.CalculateMinReplicas(rayCluster) + 1, utils.CalculateMinResources(rayCluster)
}

// calculateSubGroupPolicy maps every required logical Ray replica to a Volcano subgroup.
// A subgroup contains NumOfHosts Pods, and MinSubGroups follows the same autoscaling or
// effective desired replica semantics used to calculate the PodGroup's global MinMember.
// Pre-v1.14 Volcano CRDs prune this field and retain the legacy global PodGroup settings.
func calculateSubGroupPolicy(owner metav1.Object, rayClusterSpec *rayv1.RayClusterSpec) []volcanoschedulingv1beta1.SubGroupPolicySpec {
	if !features.Enabled(features.RayMultiHostIndexing) {
		return nil
	}

	// The existing top-level topology labels apply topology constraints to the entire PodGroup.
	// Generating per-replica subgroups without copying that policy would change those semantics.
	// Keep the existing behavior until per-worker-group topology configuration is defined.
	if _, topologyConfigured := owner.GetLabels()[NetworkTopologyModeLabelKey]; topologyConfigured {
		return nil
	}

	subGroupPolicy := []volcanoschedulingv1beta1.SubGroupPolicySpec{
		{
			Name:         utils.RayNodeHeadGroupLabelValue,
			SubGroupSize: ptr.To[int32](1),
			MinSubGroups: ptr.To[int32](1),
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayNodeGroupLabelKey: utils.RayNodeHeadGroupLabelValue,
				},
			},
			MatchLabelKeys: []string{utils.RayNodeGroupLabelKey},
		},
	}

	autoscalingEnabled := utils.IsAutoscalingEnabled(rayClusterSpec)
	for _, workerGroupSpec := range rayClusterSpec.WorkerGroupSpecs {
		if workerGroupSpec.Suspend != nil && *workerGroupSpec.Suspend {
			continue
		}

		numOfHosts := workerGroupSpec.NumOfHosts
		if numOfHosts < 1 {
			continue
		}

		var minSubGroups int32
		if autoscalingEnabled {
			minSubGroups = ptr.Deref(workerGroupSpec.MinReplicas, int32(0))
		} else {
			minSubGroups = utils.GetWorkerGroupDesiredReplicas(workerGroupSpec) / numOfHosts
		}
		if minSubGroups < 0 {
			continue
		}

		matchLabelKey := utils.RayWorkerReplicaIndexKey
		if numOfHosts > 1 {
			matchLabelKey = utils.RayWorkerReplicaNameKey
		}

		subGroupPolicy = append(subGroupPolicy, volcanoschedulingv1beta1.SubGroupPolicySpec{
			Name:         workerGroupSpec.GroupName,
			SubGroupSize: new(numOfHosts),
			MinSubGroups: new(minSubGroups),
			LabelSelector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					utils.RayNodeGroupLabelKey: workerGroupSpec.GroupName,
				},
			},
			MatchLabelKeys: []string{matchLabelKey},
		})
	}

	return subGroupPolicy
}

func createPodGroup(owner metav1.Object, podGroupName string, size int32, totalResource corev1.ResourceList, subGroupPolicy []volcanoschedulingv1beta1.SubGroupPolicySpec) (volcanoschedulingv1beta1.PodGroup, error) {
	var ownerRef metav1.OwnerReference
	switch obj := owner.(type) {
	case *rayv1.RayCluster:
		ownerRef = *metav1.NewControllerRef(obj, rayv1.SchemeGroupVersion.WithKind("RayCluster"))
	case *rayv1.RayJob:
		ownerRef = *metav1.NewControllerRef(obj, rayv1.SchemeGroupVersion.WithKind("RayJob"))
	}

	annotations := make(map[string]string, len(owner.GetAnnotations()))
	maps.Copy(annotations, owner.GetAnnotations())

	podGroup := volcanoschedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       owner.GetNamespace(),
			Name:            podGroupName,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Annotations:     annotations,
		},
		Spec: volcanoschedulingv1beta1.PodGroupSpec{
			MinMember:      size,
			MinResources:   &totalResource,
			SubGroupPolicy: subGroupPolicy,
		},
		Status: volcanoschedulingv1beta1.PodGroupStatus{
			Phase: volcanoschedulingv1beta1.PodGroupPending,
		},
	}

	// Handle network topology configuration
	mode, modeOk := owner.GetLabels()[NetworkTopologyModeLabelKey]
	if modeOk {
		podGroup.Spec.NetworkTopology = &volcanoschedulingv1beta1.NetworkTopologySpec{
			Mode: volcanoschedulingv1beta1.NetworkTopologyMode(mode),
		}
		highestTier, tierOk := owner.GetLabels()[NetworkTopologyHighestTierAllowedLabelKey]
		if tierOk {
			highestTierInt, err := strconv.Atoi(highestTier)
			if err != nil {
				return podGroup, fmt.Errorf("failed to convert %s label to int: %w for podgroup %s in namespace %s", NetworkTopologyHighestTierAllowedLabelKey, err, podGroupName, owner.GetNamespace())
			}
			podGroup.Spec.NetworkTopology.HighestTierAllowed = &highestTierInt
		}
	}

	if queue, ok := owner.GetLabels()[QueueNameLabelKey]; ok {
		podGroup.Spec.Queue = queue
	}
	if priorityClassName, ok := owner.GetLabels()[utils.RayPriorityClassName]; ok {
		podGroup.Spec.PriorityClassName = priorityClassName
	}

	return podGroup, nil
}

func (v *VolcanoBatchScheduler) AddMetadataToChildResource(_ context.Context, parent metav1.Object, child metav1.Object, groupName string) {
	populateLabelsFromObject(parent, child, QueueNameLabelKey)
	populateLabelsFromObject(parent, child, utils.RayPriorityClassName)
	populateAnnotations(parent, child, groupName)
	addSchedulerName(child, v.Name())
}

// CleanupOnCompletion recalculates and updates the PodGroup resources when a RayJob finishes.
// This is called when the RayJob reaches terminal state (Complete/Failed).
//
// For RayCluster objects, this is a no-op because the PodGroup is cleaned up by the OwnerReference of the RayCluster.
//
// For RayJob objects, the PodGroup's MinMember and MinResources are recalculated based on the
// live RayCluster state. This correctly handles deletion strategies:
//   - If workers are suspended by DeleteWorkers policy, calculatePodGroupParams automatically
//     excludes suspended groups, so the PodGroup reflects only the head pod.
//   - If the RayCluster is deleted by DeleteCluster or ShutdownAfterJobFinishes, the PodGroup is
//     updated with empty resources.
func (v *VolcanoBatchScheduler) CleanupOnCompletion(ctx context.Context, object metav1.Object) (bool, error) {
	logger := ctrl.LoggerFrom(ctx).WithName(pluginName)

	// Only handle RayJob. RayCluster PodGroups will be cleaned up by the OwnerReference.
	rayJob, ok := object.(*rayv1.RayJob)
	if !ok {
		return false, nil
	}

	if len(rayJob.Spec.ClusterSelector) > 0 {
		// Batch scheduling is not supported for RayJob with ClusterSelector.
		return false, nil
	}

	var minMembers int32
	var totalResourceList []corev1.ResourceList
	var subGroupPolicy []volcanoschedulingv1beta1.SubGroupPolicySpec

	if len(rayJob.Status.RayClusterName) == 0 {
		// The RayClusterName has not been assigned so that there is no PodGroup to update.
		return false, nil
	}

	cluster := &rayv1.RayCluster{}
	clusterKey := types.NamespacedName{Namespace: rayJob.Namespace, Name: rayJob.Status.RayClusterName}
	if err := v.cli.Get(ctx, clusterKey, cluster); err != nil {
		if !errors.IsNotFound(err) {
			return false, err
		}

		logger.Info("RayCluster not found, updating PodGroup with empty resources", "RayCluster", clusterKey)
	} else if cluster.ObjectMeta.DeletionTimestamp.IsZero() {
		// RayCluster exists. Recalculate based on live spec (suspended workers are automatically excluded).
		// If the RayJob is SidecarMode, the submitter is included. Even if the submitter container has terminated, it still takes into account
		// If it is K8sJobMode, the submitter pod is not taken into account. The completed pod doesn't have resources allocated.
		clusterMinMembers, clusterMinResources := v.calculatePodGroupParams(&cluster.Spec)
		minMembers = clusterMinMembers
		totalResourceList = append(totalResourceList, clusterMinResources)
		subGroupPolicy = calculateSubGroupPolicy(rayJob, &cluster.Spec)
	}

	didUpdate, err := v.syncPodGroup(ctx, rayJob, minMembers, utils.SumResourceList(totalResourceList), subGroupPolicy)
	if err != nil {
		return false, err
	}

	return didUpdate, nil
}

func (vf *VolcanoBatchSchedulerFactory) New(_ context.Context, _ *rest.Config, cli client.Client) (schedulerinterface.BatchScheduler, error) {
	if err := volcanoschedulingv1beta1.AddToScheme(cli.Scheme()); err != nil {
		return nil, fmt.Errorf("failed to add volcano to scheme with error %w", err)
	}
	return &VolcanoBatchScheduler{
		cli: cli,
	}, nil
}

func (vf *VolcanoBatchSchedulerFactory) AddToScheme(scheme *runtime.Scheme) {
	utilruntime.Must(volcanoschedulingv1beta1.AddToScheme(scheme))
}

func (vf *VolcanoBatchSchedulerFactory) ConfigureReconciler(b *builder.Builder) *builder.Builder {
	return b.Owns(&volcanoschedulingv1beta1.PodGroup{})
}
