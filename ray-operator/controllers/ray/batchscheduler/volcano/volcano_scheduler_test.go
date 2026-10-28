package volcano

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	volcanobatchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	volcanoschedulingv1beta1 "volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// legacyVolcanoClient simulates an API server using a pre-v1.14 Volcano CRD,
// whose structural schema prunes the unknown spec.subGroupPolicy field.
type legacyVolcanoClient struct {
	client.Client
}

func (c *legacyVolcanoClient) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	if podGroup, ok := obj.(*volcanoschedulingv1beta1.PodGroup); ok {
		legacyPodGroup := podGroup.DeepCopy()
		legacyPodGroup.Spec.SubGroupPolicy = nil
		return c.Client.Create(ctx, legacyPodGroup, opts...)
	}
	return c.Client.Create(ctx, obj, opts...)
}

func (c *legacyVolcanoClient) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if podGroup, ok := obj.(*volcanoschedulingv1beta1.PodGroup); ok {
		legacyPodGroup := podGroup.DeepCopy()
		legacyPodGroup.Spec.SubGroupPolicy = nil
		return c.Client.Update(ctx, legacyPodGroup, opts...)
	}
	return c.Client.Update(ctx, obj, opts...)
}

func createTestRayCluster(numOfHosts int32) rayv1.RayCluster {
	headSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "ray-head",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("256m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			},
		},
	}

	workerSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "ray-worker",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
						"nvidia.com/gpu":      resource.MustParse("1"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("256m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			},
		},
	}

	return rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: headSpec,
				},
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName: "small-group",
					Template: corev1.PodTemplateSpec{
						Spec: workerSpec,
					},
					Replicas:    ptr.To[int32](2),
					NumOfHosts:  numOfHosts,
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](4),
				},
			},
		},
	}
}

func createTestRayJob(numOfHosts int32) rayv1.RayJob {
	headSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "ray-head",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("256m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			},
		},
	}

	workerSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name: "ray-worker",
				Resources: corev1.ResourceRequirements{
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("512Mi"),
					},
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("256m"),
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			},
		},
	}

	return rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayjob-sample",
			Namespace: "default",
			Labels: map[string]string{
				QueueNameLabelKey:          "test-queue",
				utils.RayPriorityClassName: "test-priority",
			},
		},
		Spec: rayv1.RayJobSpec{
			RayClusterSpec: &rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: corev1.PodTemplateSpec{
						Spec: headSpec,
					},
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "small-group",
						Template: corev1.PodTemplateSpec{
							Spec: workerSpec,
						},
						Replicas:    ptr.To[int32](2),
						NumOfHosts:  numOfHosts,
						MinReplicas: ptr.To[int32](1),
						MaxReplicas: ptr.To[int32](4),
					},
				},
			},
		},
	}
}

func TestCreatePodGroupForRayCluster(t *testing.T) {
	a := assert.New(t)

	cluster := createTestRayCluster(1)

	minMember := utils.CalculateDesiredReplicas(&cluster) + 1
	totalResource := utils.CalculateDesiredResources(&cluster)
	pg, err := createPodGroup(&cluster, getAppPodGroupName(&cluster), minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))
	require.NoError(t, err)

	a.Equal(cluster.Namespace, pg.Namespace)

	// 1 head + 2 workers (desired, not min replicas)
	a.Equal(int32(3), pg.Spec.MinMember)

	// 256m * 3 (requests, not limits)
	a.Equal("768m", pg.Spec.MinResources.Cpu().String())

	// 256Mi * 3 (requests, not limits)
	a.Equal("768Mi", pg.Spec.MinResources.Memory().String())

	// 2 GPUs total
	a.Equal("2", pg.Spec.MinResources.Name("nvidia.com/gpu", resource.BinarySI).String())

	require.Len(t, pg.Spec.SubGroupPolicy, 2)
	a.Equal(utils.RayNodeHeadGroupLabelValue, pg.Spec.SubGroupPolicy[0].Name)
	a.Equal("small-group", pg.Spec.SubGroupPolicy[1].Name)
	a.Equal(int32(1), ptr.Deref(pg.Spec.SubGroupPolicy[1].SubGroupSize, int32(0)))
	a.Equal(int32(2), ptr.Deref(pg.Spec.SubGroupPolicy[1].MinSubGroups, int32(0)))
}

func TestCreatePodGroupForRayCluster_NumOfHosts2(t *testing.T) {
	a := assert.New(t)

	cluster := createTestRayCluster(2)

	minMember := utils.CalculateDesiredReplicas(&cluster) + 1
	totalResource := utils.CalculateDesiredResources(&cluster)
	pg, err := createPodGroup(&cluster, getAppPodGroupName(&cluster), minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))
	require.NoError(t, err)

	a.Equal(cluster.Namespace, pg.Namespace)

	// 2 workers (desired, not min replicas) * 2 (num of hosts) + 1 head
	// 2 * 2 + 1 = 5
	a.Equal(int32(5), pg.Spec.MinMember)

	// 256m * (2 (requests, not limits) * 2 (num of hosts) + 1 head)
	// 256m * 5 = 1280m
	a.Equal("1280m", pg.Spec.MinResources.Cpu().String())

	// 256Mi * (2 (requests, not limits) * 2 (num of hosts) + 1 head)
	// 256Mi * 5 = 1280Mi
	a.Equal("1280Mi", pg.Spec.MinResources.Memory().String())

	// 2 GPUs * 2 (num of hosts) total
	// 2 GPUs * 2 = 4 GPUs
	a.Equal("4", pg.Spec.MinResources.Name("nvidia.com/gpu", resource.BinarySI).String())

	require.Len(t, pg.Spec.SubGroupPolicy, 2)
	a.Equal("small-group", pg.Spec.SubGroupPolicy[1].Name)
	a.Equal(int32(2), ptr.Deref(pg.Spec.SubGroupPolicy[1].SubGroupSize, int32(0)))
	a.Equal(int32(2), ptr.Deref(pg.Spec.SubGroupPolicy[1].MinSubGroups, int32(0)))
	a.Equal([]string{utils.RayWorkerReplicaNameKey}, pg.Spec.SubGroupPolicy[1].MatchLabelKeys)
}

func createTestRayClusterWithLabels(labels map[string]string) rayv1.RayCluster {
	cluster := createTestRayCluster(1)
	if cluster.ObjectMeta.Labels == nil {
		cluster.ObjectMeta.Labels = make(map[string]string)
	}
	maps.Copy(cluster.ObjectMeta.Labels, labels)
	return cluster
}

func TestCreatePodGroup_NetworkTopologyBothLabels(t *testing.T) {
	a := assert.New(t)

	// Test with both network topology mode and highest tier allowed
	cluster := createTestRayClusterWithLabels(map[string]string{
		NetworkTopologyModeLabelKey:               "soft",
		NetworkTopologyHighestTierAllowedLabelKey: "3",
	})

	minMember := utils.CalculateDesiredReplicas(&cluster) + 1
	totalResource := utils.CalculateDesiredResources(&cluster)
	pg, err := createPodGroup(&cluster, getAppPodGroupName(&cluster), minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))
	require.NoError(t, err)

	a.Equal(cluster.Namespace, pg.Namespace)
	a.Equal(volcanoschedulingv1beta1.NetworkTopologyMode("soft"), pg.Spec.NetworkTopology.Mode)
	a.NotNil(pg.Spec.NetworkTopology.HighestTierAllowed)
	a.Equal(3, *pg.Spec.NetworkTopology.HighestTierAllowed)
}

func TestCreatePodGroup_NetworkTopologyOnlyModeLabel(t *testing.T) {
	a := assert.New(t)

	// Test with only network topology mode set
	cluster := createTestRayClusterWithLabels(map[string]string{
		NetworkTopologyModeLabelKey: "hard",
	})

	minMember := utils.CalculateDesiredReplicas(&cluster) + 1
	totalResource := utils.CalculateDesiredResources(&cluster)
	pg, err := createPodGroup(&cluster, getAppPodGroupName(&cluster), minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))
	require.NoError(t, err)

	a.Equal(cluster.Namespace, pg.Namespace)
	a.NotNil(pg.Spec.NetworkTopology)
	a.Equal(volcanoschedulingv1beta1.NetworkTopologyMode("hard"), pg.Spec.NetworkTopology.Mode)
	a.Nil(pg.Spec.NetworkTopology.HighestTierAllowed)
}

func TestCreatePodGroup_NetworkTopologyHighestTierAllowedNotInt(t *testing.T) {
	a := assert.New(t)

	// Test with network topology mode set and highest tier allowed is not an int
	cluster := createTestRayClusterWithLabels(map[string]string{
		NetworkTopologyModeLabelKey:               "soft",
		NetworkTopologyHighestTierAllowedLabelKey: "not-an-int",
	})

	minMember := utils.CalculateDesiredReplicas(&cluster) + 1
	totalResource := utils.CalculateDesiredResources(&cluster)
	pg, err := createPodGroup(&cluster, getAppPodGroupName(&cluster), minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))

	require.Error(t, err)
	a.Contains(err.Error(), "failed to convert "+NetworkTopologyHighestTierAllowedLabelKey+" label to int")
	a.Equal(cluster.Namespace, pg.Namespace)
}

func TestCreatePodGroupForRayJob(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	scheme := runtime.NewScheme()
	a.NoError(rayv1.AddToScheme(scheme))
	a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
	fakeCli := fake.NewClientBuilder().WithScheme(scheme).Build()
	scheduler := &VolcanoBatchScheduler{cli: fakeCli}

	t.Run("No submitter pod resources", func(_ *testing.T) {
		rayJob := createTestRayJob(1)
		rayJob.Spec.SubmissionMode = rayv1.HTTPMode

		err := scheduler.handleRayJob(ctx, &rayJob)
		require.NoError(t, err)

		var pg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &pg)
		require.NoError(t, err)

		// 1 head + 2 workers (desired, not min replicas)
		a.Equal(int32(3), pg.Spec.MinMember)
		// 256m * 3 (requests, not limits)
		a.Equal("768m", pg.Spec.MinResources.Cpu().String())
		// 256m * 3 (requests, not limits)
		a.Equal("768Mi", pg.Spec.MinResources.Memory().String())
		a.Equal("test-queue", pg.Spec.Queue)
		a.Equal("test-priority", pg.Spec.PriorityClassName)
		a.Len(pg.OwnerReferences, 1)
		a.Equal("RayJob", pg.OwnerReferences[0].Kind)
		require.Len(t, pg.Spec.SubGroupPolicy, 2)
		a.Equal(utils.RayNodeHeadGroupLabelValue, pg.Spec.SubGroupPolicy[0].Name)
		a.Equal("small-group", pg.Spec.SubGroupPolicy[1].Name)
	})

	t.Run("K8sJobMode includes submitter pod resources", func(_ *testing.T) {
		rayJob := createTestRayJob(1)
		rayJob.Spec.SubmissionMode = rayv1.K8sJobMode

		err := scheduler.handleRayJob(ctx, &rayJob)
		require.NoError(t, err)

		var pg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &pg)
		require.NoError(t, err)

		// 1 head + 2 workers (desired, not min replicas)
		a.Equal(int32(3), pg.Spec.MinMember)
		// 768m + 500m = 1268m
		a.Equal("1268m", pg.Spec.MinResources.Cpu().String())
		// 768Mi + 200Mi = 968Mi
		a.Equal("968Mi", pg.Spec.MinResources.Memory().String())
		a.Equal("test-queue", pg.Spec.Queue)
		a.Equal("test-priority", pg.Spec.PriorityClassName)
		a.Len(pg.OwnerReferences, 1)
		a.Equal("RayJob", pg.OwnerReferences[0].Kind)
	})

	t.Run("SidecarMode includes submitter pod resources", func(_ *testing.T) {
		rayJob := createTestRayJob(1)
		rayJob.Spec.SubmissionMode = rayv1.SidecarMode

		err := scheduler.handleRayJob(ctx, &rayJob)
		require.NoError(t, err)

		var pg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &pg)
		require.NoError(t, err)

		// 1 head + 2 workers (desired, not min replicas)
		a.Equal(int32(3), pg.Spec.MinMember)
		// 768m + 500m = 1268m
		a.Equal("1268m", pg.Spec.MinResources.Cpu().String())
		// 768Mi + 200Mi = 968Mi
		a.Equal("968Mi", pg.Spec.MinResources.Memory().String())
		a.Equal("test-queue", pg.Spec.Queue)
		a.Equal("test-priority", pg.Spec.PriorityClassName)
		a.Len(pg.OwnerReferences, 1)
		a.Equal("RayJob", pg.OwnerReferences[0].Kind)
	})
}

func TestCreatePodGroupForRayJob_NumOfHosts2(t *testing.T) {
	a := assert.New(t)
	ctx := context.Background()

	scheme := runtime.NewScheme()
	a.NoError(rayv1.AddToScheme(scheme))
	a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
	fakeCli := fake.NewClientBuilder().WithScheme(scheme).Build()
	scheduler := &VolcanoBatchScheduler{cli: fakeCli}

	t.Run("No submitter pod resources", func(_ *testing.T) {
		rayJob := createTestRayJob(2)
		rayJob.Spec.SubmissionMode = rayv1.HTTPMode

		err := scheduler.handleRayJob(ctx, &rayJob)
		require.NoError(t, err)

		var pg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &pg)
		require.NoError(t, err)

		// 2 workers (desired, not min replicas) * 2 (num of hosts) + 1 head
		// 2 * 2 + 1 = 5
		a.Equal(int32(5), pg.Spec.MinMember)
		// 256m * (2 (requests, not limits) * 2 (num of hosts) + 1 head)
		// 256m * 5 = 1280m
		a.Equal("1280m", pg.Spec.MinResources.Cpu().String())
		// 256Mi * (2 (requests, not limits) * 2 (num of hosts) + 1 head)
		// 256Mi * 5 = 1280Mi
		a.Equal("1280Mi", pg.Spec.MinResources.Memory().String())
		a.Equal("test-queue", pg.Spec.Queue)
		a.Equal("test-priority", pg.Spec.PriorityClassName)
		a.Len(pg.OwnerReferences, 1)
		a.Equal("RayJob", pg.OwnerReferences[0].Kind)
	})

	t.Run("K8sJobMode includes submitter pod resources", func(_ *testing.T) {
		rayJob := createTestRayJob(2)
		rayJob.Spec.SubmissionMode = rayv1.K8sJobMode

		err := scheduler.handleRayJob(ctx, &rayJob)
		require.NoError(t, err)

		var pg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &pg)
		require.NoError(t, err)

		// 2 workers (desired, not min replicas) * 2 (num of hosts) + 1 head
		// 2 * 2 + 1 = 5
		a.Equal(int32(5), pg.Spec.MinMember)
		// 1280m + 500m = 1780m
		a.Equal("1780m", pg.Spec.MinResources.Cpu().String())
		// 1280Mi + 200Mi = 1480Mi
		a.Equal("1480Mi", pg.Spec.MinResources.Memory().String())
		a.Equal("test-queue", pg.Spec.Queue)
		a.Equal("test-priority", pg.Spec.PriorityClassName)
		a.Len(pg.OwnerReferences, 1)
		a.Equal("RayJob", pg.OwnerReferences[0].Kind)
	})

	t.Run("SidecarMode includes submitter pod resources", func(_ *testing.T) {
		rayJob := createTestRayJob(2)
		rayJob.Spec.SubmissionMode = rayv1.SidecarMode

		err := scheduler.handleRayJob(ctx, &rayJob)
		require.NoError(t, err)

		var pg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &pg)
		require.NoError(t, err)

		// 2 workers (desired, not min replicas) * 2 (num of hosts) + 1 head
		// 2 * 2 + 1 = 5
		a.Equal(int32(5), pg.Spec.MinMember)
		// 1280m + 500m = 1780m
		a.Equal("1780m", pg.Spec.MinResources.Cpu().String())
		// 1280Mi + 200Mi = 1480Mi
		a.Equal("1480Mi", pg.Spec.MinResources.Memory().String())
		a.Equal("test-queue", pg.Spec.Queue)
		a.Equal("test-priority", pg.Spec.PriorityClassName)
		a.Len(pg.OwnerReferences, 1)
		a.Equal("RayJob", pg.OwnerReferences[0].Kind)
	})
}

func TestAddMetadataToSubmitterPod(t *testing.T) {
	a := assert.New(t)
	scheduler := &VolcanoBatchScheduler{}

	rayJob := createTestRayJob(1)
	rayCluster := &rayv1.RayCluster{Spec: *rayJob.Spec.RayClusterSpec}
	submitterTemplate := common.GetSubmitterTemplate(&rayJob.Spec, &rayCluster.Spec)

	scheduler.AddMetadataToChildResource(
		context.Background(),
		&rayJob,
		&submitterTemplate,
		utils.RayNodeSubmitterGroupLabelValue,
	)

	// Check annotations
	a.Equal(getAppPodGroupName(&rayJob), submitterTemplate.Annotations[volcanoschedulingv1beta1.KubeGroupNameAnnotationKey])
	a.Equal(utils.RayNodeSubmitterGroupLabelValue, submitterTemplate.Annotations[volcanobatchv1alpha1.TaskSpecKey])

	// Check labels
	a.Equal("test-queue", submitterTemplate.Labels[QueueNameLabelKey])
	a.Equal("test-priority", submitterTemplate.Labels[utils.RayPriorityClassName])

	// Check scheduler name
	a.Equal(pluginName, submitterTemplate.Spec.SchedulerName)
}

func TestCalculatePodGroupParams(t *testing.T) {
	a := assert.New(t)
	scheduler := &VolcanoBatchScheduler{}

	t.Run("Autoscaling disabled", func(_ *testing.T) {
		cluster := createTestRayCluster(1)

		minMember, totalResource := scheduler.calculatePodGroupParams(&cluster.Spec)

		// 1 head + 2 workers (desired replicas)
		a.Equal(int32(3), minMember)

		// 256m * 3 (requests, not limits)
		a.Equal("768m", totalResource.Cpu().String())

		// 256Mi * 3 (requests, not limits)
		a.Equal("768Mi", totalResource.Memory().String())
	})

	t.Run("Autoscaling enabled", func(_ *testing.T) {
		cluster := createTestRayCluster(1)
		cluster.Spec.EnableInTreeAutoscaling = new(true)

		minMember, totalResource := scheduler.calculatePodGroupParams(&cluster.Spec)

		// 1 head + 1 worker (min replicas)
		a.Equal(int32(2), minMember)

		// 256m * 2 (requests, not limits)
		a.Equal("512m", totalResource.Cpu().String())

		// 256Mi * 2 (requests, not limits)
		a.Equal("512Mi", totalResource.Memory().String())
	})
}

func TestCalculateSubGroupPolicy(t *testing.T) {
	t.Run("autoscaling uses each active worker group's minimum logical replicas", func(t *testing.T) {
		cluster := createTestRayCluster(1)
		cluster.Spec.EnableInTreeAutoscaling = new(true)
		cluster.Spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
			{
				GroupName:   "cpu-group",
				Replicas:    ptr.To[int32](10),
				MinReplicas: ptr.To[int32](4),
				MaxReplicas: ptr.To[int32](10),
				NumOfHosts:  1,
			},
			{
				GroupName:   "gpu-group",
				Replicas:    ptr.To[int32](4),
				MinReplicas: ptr.To[int32](4),
				MaxReplicas: ptr.To[int32](4),
				NumOfHosts:  2,
			},
			{
				GroupName:   "suspended-group",
				Replicas:    ptr.To[int32](2),
				MinReplicas: ptr.To[int32](2),
				MaxReplicas: ptr.To[int32](2),
				NumOfHosts:  1,
				Suspend:     new(true),
			},
			{
				GroupName:   "zero-minimum-group",
				Replicas:    ptr.To[int32](2),
				MinReplicas: ptr.To[int32](0),
				MaxReplicas: ptr.To[int32](2),
				NumOfHosts:  2,
			},
		}

		policies := calculateSubGroupPolicy(&cluster, &cluster.Spec)
		scheduler := &VolcanoBatchScheduler{}
		minMember, _ := scheduler.calculatePodGroupParams(&cluster.Spec)

		assert.Equal(t, int32(13), minMember)
		require.Len(t, policies, 4)
		assert.Equal(t, utils.RayNodeHeadGroupLabelValue, policies[0].Name)
		assert.Equal(t, int32(1), ptr.Deref(policies[0].SubGroupSize, int32(0)))
		assert.Equal(t, int32(1), ptr.Deref(policies[0].MinSubGroups, int32(0)))
		assert.Equal(t, map[string]string{utils.RayNodeGroupLabelKey: utils.RayNodeHeadGroupLabelValue}, policies[0].LabelSelector.MatchLabels)
		assert.Equal(t, []string{utils.RayNodeGroupLabelKey}, policies[0].MatchLabelKeys)

		assert.Equal(t, "cpu-group", policies[1].Name)
		assert.Equal(t, int32(1), ptr.Deref(policies[1].SubGroupSize, int32(0)))
		assert.Equal(t, int32(4), ptr.Deref(policies[1].MinSubGroups, int32(0)))
		assert.Equal(t, map[string]string{utils.RayNodeGroupLabelKey: "cpu-group"}, policies[1].LabelSelector.MatchLabels)
		assert.Equal(t, []string{utils.RayWorkerReplicaIndexKey}, policies[1].MatchLabelKeys)

		assert.Equal(t, "gpu-group", policies[2].Name)
		assert.Equal(t, int32(2), ptr.Deref(policies[2].SubGroupSize, int32(0)))
		assert.Equal(t, int32(4), ptr.Deref(policies[2].MinSubGroups, int32(0)))
		assert.Equal(t, map[string]string{utils.RayNodeGroupLabelKey: "gpu-group"}, policies[2].LabelSelector.MatchLabels)
		assert.Equal(t, []string{utils.RayWorkerReplicaNameKey}, policies[2].MatchLabelKeys)

		assert.Equal(t, "zero-minimum-group", policies[3].Name)
		assert.Equal(t, int32(2), ptr.Deref(policies[3].SubGroupSize, int32(0)))
		assert.Equal(t, int32(0), ptr.Deref(policies[3].MinSubGroups, int32(-1)))
		assert.Equal(t, []string{utils.RayWorkerReplicaNameKey}, policies[3].MatchLabelKeys)
	})

	t.Run("non-autoscaling uses effective desired logical replicas", func(t *testing.T) {
		cluster := createTestRayCluster(1)
		cluster.Spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
			{
				GroupName:   "clamped-to-min",
				Replicas:    ptr.To[int32](1),
				MinReplicas: ptr.To[int32](2),
				MaxReplicas: ptr.To[int32](8),
				NumOfHosts:  2,
			},
			{
				GroupName:   "clamped-to-max",
				Replicas:    ptr.To[int32](10),
				MinReplicas: ptr.To[int32](2),
				MaxReplicas: ptr.To[int32](8),
				NumOfHosts:  1,
			},
		}

		policies := calculateSubGroupPolicy(&cluster, &cluster.Spec)

		require.Len(t, policies, 3)
		assert.Equal(t, "clamped-to-min", policies[1].Name)
		assert.Equal(t, int32(2), ptr.Deref(policies[1].SubGroupSize, int32(0)))
		assert.Equal(t, int32(2), ptr.Deref(policies[1].MinSubGroups, int32(0)))
		assert.Equal(t, "clamped-to-max", policies[2].Name)
		assert.Equal(t, int32(1), ptr.Deref(policies[2].SubGroupSize, int32(0)))
		assert.Equal(t, int32(8), ptr.Deref(policies[2].MinSubGroups, int32(0)))
	})

	t.Run("multi-host indexing feature gate disabled omits subgroup policy", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.RayMultiHostIndexing, false)
		cluster := createTestRayCluster(2)

		assert.Nil(t, calculateSubGroupPolicy(&cluster, &cluster.Spec))
	})

	t.Run("top-level network topology preserves legacy behavior", func(t *testing.T) {
		cluster := createTestRayClusterWithLabels(map[string]string{
			NetworkTopologyModeLabelKey: "soft",
		})

		assert.Nil(t, calculateSubGroupPolicy(&cluster, &cluster.Spec))
	})
}

func TestSyncPodGroup_SubGroupPolicy(t *testing.T) {
	ctx := context.Background()
	cluster := createTestRayCluster(2)
	cluster.Spec.WorkerGroupSpecs[0].GroupName = "gpu-group"

	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	require.NoError(t, volcanoschedulingv1beta1.AddToScheme(scheme))
	fakeCli := fake.NewClientBuilder().WithScheme(scheme).Build()
	scheduler := &VolcanoBatchScheduler{cli: fakeCli}

	require.NoError(t, scheduler.handleRayCluster(ctx, &cluster))

	podGroup := &volcanoschedulingv1beta1.PodGroup{}
	podGroupKey := client.ObjectKey{Namespace: cluster.Namespace, Name: getAppPodGroupName(&cluster)}
	require.NoError(t, fakeCli.Get(ctx, podGroupKey, podGroup))
	require.Len(t, podGroup.Spec.SubGroupPolicy, 2)
	assert.Equal(t, int32(2), ptr.Deref(podGroup.Spec.SubGroupPolicy[1].MinSubGroups, int32(0)))

	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](3)
	require.NoError(t, scheduler.handleRayCluster(ctx, &cluster))
	require.NoError(t, fakeCli.Get(ctx, podGroupKey, podGroup))
	require.Len(t, podGroup.Spec.SubGroupPolicy, 2)
	assert.Equal(t, int32(3), ptr.Deref(podGroup.Spec.SubGroupPolicy[1].MinSubGroups, int32(0)))
	assert.Equal(t, int32(2), ptr.Deref(podGroup.Spec.SubGroupPolicy[1].SubGroupSize, int32(0)))

	minMember, totalResource := scheduler.calculatePodGroupParams(&cluster.Spec)
	didUpdate, err := scheduler.syncPodGroup(ctx, &cluster, minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))
	require.NoError(t, err)
	assert.False(t, didUpdate)
}

func TestSyncPodGroup_OlderVolcanoCRDPrunesSubGroupPolicy(t *testing.T) {
	ctx := context.Background()
	cluster := createTestRayCluster(2)
	cluster.Spec.WorkerGroupSpecs[0].GroupName = "gpu-group"

	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	require.NoError(t, volcanoschedulingv1beta1.AddToScheme(scheme))
	fakeCli := fake.NewClientBuilder().WithScheme(scheme).Build()
	scheduler := &VolcanoBatchScheduler{cli: &legacyVolcanoClient{Client: fakeCli}}

	// A pre-v1.14 CRD drops SubGroupPolicy but still accepts the PodGroup and
	// preserves the existing global gang-scheduling fields.
	require.NoError(t, scheduler.handleRayCluster(ctx, &cluster))

	podGroup := &volcanoschedulingv1beta1.PodGroup{}
	podGroupKey := client.ObjectKey{Namespace: cluster.Namespace, Name: getAppPodGroupName(&cluster)}
	require.NoError(t, fakeCli.Get(ctx, podGroupKey, podGroup))
	assert.Empty(t, podGroup.Spec.SubGroupPolicy)
	assert.Equal(t, int32(5), podGroup.Spec.MinMember)
	assert.Equal(t, "1280m", podGroup.Spec.MinResources.Cpu().String())

	// Updates also continue to synchronize the legacy fields without failing.
	cluster.Spec.WorkerGroupSpecs[0].Replicas = ptr.To[int32](3)
	require.NoError(t, scheduler.handleRayCluster(ctx, &cluster))
	require.NoError(t, fakeCli.Get(ctx, podGroupKey, podGroup))
	assert.Empty(t, podGroup.Spec.SubGroupPolicy)
	assert.Equal(t, int32(7), podGroup.Spec.MinMember)
	assert.Equal(t, "1792m", podGroup.Spec.MinResources.Cpu().String())
}

func TestGetAppPodGroupName(t *testing.T) {
	a := assert.New(t)

	rayCluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "raycluster-sample", Namespace: "default"}}
	a.Equal("ray-raycluster-sample-pg", getAppPodGroupName(rayCluster))

	rayJob := createTestRayJob(1)
	a.Equal("ray-rayjob-sample-pg", getAppPodGroupName(&rayJob))
}

func TestCreatePodGroup_OwnerAnnotationsCopied(t *testing.T) {
	a := assert.New(t)

	t.Run("RayCluster with annotations", func(t *testing.T) {
		cluster := createTestRayCluster(1)
		cluster.Annotations = map[string]string{
			"custom.io/team":       "ml-platform",
			"custom.io/scheduling": "volcano",
		}

		minMember := utils.CalculateDesiredReplicas(&cluster) + 1
		totalResource := utils.CalculateDesiredResources(&cluster)
		pg, err := createPodGroup(&cluster, getAppPodGroupName(&cluster), minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))
		require.NoError(t, err)

		a.NotNil(pg.Annotations)
		a.Equal(cluster.Annotations["custom.io/team"], pg.Annotations["custom.io/team"])
		a.Equal(cluster.Annotations["custom.io/scheduling"], pg.Annotations["custom.io/scheduling"])
		a.Len(pg.Annotations, 2)
	})

	t.Run("RayJob with annotations", func(t *testing.T) {
		rayJob := createTestRayJob(1)
		rayJob.Annotations = map[string]string{
			"job-type": "training",
			"owner":    "data-team",
		}

		minMember := utils.CalculateDesiredReplicas(&rayv1.RayCluster{Spec: *rayJob.Spec.RayClusterSpec}) + 1
		totalResource := utils.CalculateDesiredResources(&rayv1.RayCluster{Spec: *rayJob.Spec.RayClusterSpec})
		pg, err := createPodGroup(&rayJob, getAppPodGroupName(&rayJob), minMember, totalResource, calculateSubGroupPolicy(&rayJob, rayJob.Spec.RayClusterSpec))
		require.NoError(t, err)

		a.NotNil(pg.Annotations)
		a.Equal(rayJob.Annotations["job-type"], pg.Annotations["job-type"])
		a.Equal(rayJob.Annotations["owner"], pg.Annotations["owner"])
		a.Len(pg.Annotations, 2)
	})

	t.Run("RayCluster with nil annotations", func(t *testing.T) {
		cluster := createTestRayCluster(1)
		cluster.Annotations = nil

		minMember := utils.CalculateDesiredReplicas(&cluster) + 1
		totalResource := utils.CalculateDesiredResources(&cluster)
		pg, err := createPodGroup(&cluster, getAppPodGroupName(&cluster), minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))
		require.NoError(t, err)

		a.NotNil(pg.Annotations)
		a.Empty(pg.Annotations)
	})

	t.Run("RayCluster with empty annotations", func(t *testing.T) {
		cluster := createTestRayCluster(1)
		cluster.Annotations = map[string]string{}

		minMember := utils.CalculateDesiredReplicas(&cluster) + 1
		totalResource := utils.CalculateDesiredResources(&cluster)
		pg, err := createPodGroup(&cluster, getAppPodGroupName(&cluster), minMember, totalResource, calculateSubGroupPolicy(&cluster, &cluster.Spec))
		require.NoError(t, err)

		a.NotNil(pg.Annotations)
		a.Empty(pg.Annotations)
	})
}

func TestCleanupOnCompletion(t *testing.T) {
	t.Run("RayCluster - should be no-op", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayCluster := createTestRayCluster(1)
		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		// Call CleanupOnCompletion with RayCluster - should be no-op
		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayCluster)
		require.NoError(err)
		a.False(didUpdate) // No cleanup should have happened for RayCluster
	})

	t.Run("RayJob - RayJob has been deleted before calling CleanupOnCompletion", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.False(didUpdate)
	})

	t.Run("RayJob - RayJob has been mark deleted before calling CleanupOnCompletion", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		// Add a finalizer so that Delete sets DeletionTimestamp without removing the object
		rayJob.Finalizers = []string{"test-finalizer"}

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rayJob).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		oldRayJob := rayJob.DeepCopy()
		// Delete the RayJob to set DeletionTimestamp (finalizer prevents actual removal)
		err := fakeCli.Delete(ctx, &rayJob)
		require.NoError(err)

		didUpdate, err := scheduler.CleanupOnCompletion(ctx, oldRayJob)
		require.NoError(err)
		a.False(didUpdate)

		// remove finalizer to clean up the object
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: rayJob.Name}, &rayJob)
		require.NoError(err)
		rayJob.Finalizers = nil
		err = fakeCli.Update(ctx, &rayJob)
		require.NoError(err)
	})

	t.Run("RayJob - cluster selector is not supported", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		rayJob.Spec.ClusterSelector = map[string]string{
			"foo": "bar",
		}
		rayJob.Spec.RayClusterSpec = nil
		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rayJob).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		// Call CleanupOnCompletion with RayJob - cluster selector is not supported
		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.False(didUpdate)
	})

	t.Run("RayJob - empty RayClusterName returns early", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		// Ensure RayClusterName is empty (default)
		rayJob.Status.RayClusterName = ""

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rayJob).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.False(didUpdate) // No cleanup when RayClusterName is empty
	})

	t.Run("RayJob - RayCluster not found, updates existing PodGroup with empty resources", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		rayJob.Status.RayClusterName = "nonexistent-cluster"

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		// Pre-create a PodGroup with non-zero values
		podGroupName := getAppPodGroupName(&rayJob)
		pg := &volcanoschedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podGroupName,
				Namespace: rayJob.Namespace,
			},
			Spec: volcanoschedulingv1beta1.PodGroupSpec{
				MinMember: 3,
				MinResources: &corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("768m"),
					corev1.ResourceMemory: resource.MustParse("768Mi"),
				},
			},
		}
		err := fakeCli.Create(ctx, pg)
		require.NoError(err)

		// CleanupOnCompletion should update PodGroup with empty resources
		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.True(didUpdate)

		// Verify PodGroup was updated with empty resources
		var retrievedPg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: podGroupName}, &retrievedPg)
		require.NoError(err)
		a.Equal(int32(0), retrievedPg.Spec.MinMember)
		a.True(retrievedPg.Spec.MinResources.Cpu().IsZero())
		a.True(retrievedPg.Spec.MinResources.Memory().IsZero())
		a.Empty(retrievedPg.Spec.SubGroupPolicy)
	})

	t.Run("RayJob - RayCluster exists, recalculates PodGroup from live spec", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		rayJob.Status.RayClusterName = "raycluster-sample"

		rayCluster := rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "raycluster-sample",
				Namespace: "default",
			},
			Spec: *rayJob.Spec.RayClusterSpec,
		}

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rayCluster).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		// Pre-create a PodGroup with stale values (e.g. minMember=10)
		podGroupName := getAppPodGroupName(&rayJob)
		pg := &volcanoschedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podGroupName,
				Namespace: rayJob.Namespace,
			},
			Spec: volcanoschedulingv1beta1.PodGroupSpec{
				MinMember: 10,
				MinResources: &corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("10"),
					corev1.ResourceMemory: resource.MustParse("10Gi"),
				},
			},
		}
		err := fakeCli.Create(ctx, pg)
		require.NoError(err)

		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.True(didUpdate)

		// Verify PodGroup was updated with recalculated values from live cluster spec
		// RayCluster has 1 head + 2 workers = 3 minMember (+ 1 for head = 3 total from calculatePodGroupParams)
		var retrievedPg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: podGroupName}, &retrievedPg)
		require.NoError(err)
		// 1 head + 2 workers (desired, not min replicas)
		a.Equal(int32(3), retrievedPg.Spec.MinMember)
		// 256m * 3 (requests, not limits)
		a.Equal("768m", retrievedPg.Spec.MinResources.Cpu().String())
		// 256Mi * 3 (requests, not limits)
		a.Equal("768Mi", retrievedPg.Spec.MinResources.Memory().String())
		require.Len(retrievedPg.Spec.SubGroupPolicy, 2)
		a.Equal("small-group", retrievedPg.Spec.SubGroupPolicy[1].Name)
		a.Equal(int32(2), ptr.Deref(retrievedPg.Spec.SubGroupPolicy[1].MinSubGroups, int32(0)))
	})

	t.Run("RayJob - RayCluster exists, recalculates PodGroup with the submitter container", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		rayJob.Status.RayClusterName = "raycluster-sample"
		rayJob.Spec.SubmissionMode = rayv1.SidecarMode
		rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers = append(rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.Containers, corev1.Container{
			Name: "submitter",
			Resources: corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("200m"),
					corev1.ResourceMemory: resource.MustParse("200Mi"),
				},
			},
		})

		rayCluster := rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "raycluster-sample",
				Namespace: "default",
			},
			Spec: *rayJob.Spec.RayClusterSpec,
		}

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rayCluster).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		// Pre-create a PodGroup with stale values (e.g. minMember=10)
		podGroupName := getAppPodGroupName(&rayJob)
		pg := &volcanoschedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podGroupName,
				Namespace: rayJob.Namespace,
			},
			Spec: volcanoschedulingv1beta1.PodGroupSpec{
				MinMember: 10,
				MinResources: &corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("10"),
					corev1.ResourceMemory: resource.MustParse("10Gi"),
				},
			},
		}
		err := fakeCli.Create(ctx, pg)
		require.NoError(err)

		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.True(didUpdate)

		// Verify PodGroup was updated with recalculated values from live cluster spec
		// RayCluster has 1 head + 2 workers = 3 minMember (+ 1 for head = 3 total from calculatePodGroupParams)
		var retrievedPg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: podGroupName}, &retrievedPg)
		require.NoError(err)
		// 1 head + 2 workers (desired, not min replicas)
		a.Equal(int32(3), retrievedPg.Spec.MinMember)
		// 256m * 3 + 200Mi(requests, not limits)
		a.Equal("968m", retrievedPg.Spec.MinResources.Cpu().String())
		// 256Mi * 3 + 200Mi (requests, not limits)
		a.Equal("968Mi", retrievedPg.Spec.MinResources.Memory().String())
	})

	t.Run("RayJob - RayCluster exists, recalculates PodGroup from live spec with worker suspended", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		rayJob.Status.RayClusterName = "raycluster-sample"

		rayCluster := rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "raycluster-sample",
				Namespace: "default",
			},
			Spec: *rayJob.Spec.RayClusterSpec,
		}
		for i := range rayCluster.Spec.WorkerGroupSpecs {
			rayCluster.Spec.WorkerGroupSpecs[i].Suspend = new(true)
		}

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rayCluster).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		// Pre-create a PodGroup with stale values (e.g. minMember=10)
		podGroupName := getAppPodGroupName(&rayJob)
		pg := &volcanoschedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podGroupName,
				Namespace: rayJob.Namespace,
			},
			Spec: volcanoschedulingv1beta1.PodGroupSpec{
				MinMember: 3,
				MinResources: &corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("768m"),
					corev1.ResourceMemory: resource.MustParse("768Mi"),
				},
			},
		}
		err := fakeCli.Create(ctx, pg)
		require.NoError(err)

		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.True(didUpdate)

		// Verify PodGroup was updated with recalculated values from live cluster spec
		// RayCluster has 1 head = 1 minMember
		var retrievedPg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: podGroupName}, &retrievedPg)
		require.NoError(err)
		// 1 head
		a.Equal(int32(1), retrievedPg.Spec.MinMember)
		// 256m * 1 (requests, not limits)
		a.Equal("256m", retrievedPg.Spec.MinResources.Cpu().String())
		// 256Mi * 1 (requests, not limits)
		a.Equal("256Mi", retrievedPg.Spec.MinResources.Memory().String())
		require.Len(retrievedPg.Spec.SubGroupPolicy, 1)
		a.Equal(utils.RayNodeHeadGroupLabelValue, retrievedPg.Spec.SubGroupPolicy[0].Name)
	})

	t.Run("RayJob - RayCluster being deleted, updates PodGroup with empty resources", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		rayJob.Status.RayClusterName = "raycluster-sample"

		rayCluster := rayv1.RayCluster{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "raycluster-sample",
				Namespace: "default",
			},
			Spec: *rayJob.Spec.RayClusterSpec,
		}
		// Add a finalizer so that Delete sets DeletionTimestamp without removing the object
		rayCluster.Finalizers = []string{"test-finalizer"}

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).WithObjects(&rayCluster).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		// Delete the RayCluster to set DeletionTimestamp (finalizer prevents actual removal)
		err := fakeCli.Delete(ctx, &rayCluster)
		require.NoError(err)

		// Pre-create a PodGroup with non-zero values
		podGroupName := getAppPodGroupName(&rayJob)
		pg := &volcanoschedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podGroupName,
				Namespace: rayJob.Namespace,
			},
			Spec: volcanoschedulingv1beta1.PodGroupSpec{
				MinMember: 3,
				MinResources: &corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("768m"),
					corev1.ResourceMemory: resource.MustParse("768Mi"),
				},
			},
		}
		err = fakeCli.Create(ctx, pg)
		require.NoError(err)

		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.True(didUpdate)

		// Verify PodGroup was updated with empty resources (RayCluster is being deleted)
		var retrievedPg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: podGroupName}, &retrievedPg)
		require.NoError(err)
		a.Equal(int32(0), retrievedPg.Spec.MinMember)
		a.True(retrievedPg.Spec.MinResources.Cpu().IsZero())
		a.True(retrievedPg.Spec.MinResources.Memory().IsZero())
	})

	t.Run("RayJob - The bool return of CleanupOnCompletion should reflect the update if calling multiple times ", func(t *testing.T) {
		a := assert.New(t)
		require := require.New(t)

		rayJob := createTestRayJob(1)
		rayJob.Status.RayClusterName = "nonexistent-cluster"

		scheme := runtime.NewScheme()
		a.NoError(rayv1.AddToScheme(scheme))
		a.NoError(volcanoschedulingv1beta1.AddToScheme(scheme))
		fakeCli := fake.NewClientBuilder().WithScheme(scheme).Build()
		scheduler := &VolcanoBatchScheduler{cli: fakeCli}

		ctx := context.Background()

		// Pre-create a PodGroup with non-zero values
		podGroupName := getAppPodGroupName(&rayJob)
		pg := &volcanoschedulingv1beta1.PodGroup{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podGroupName,
				Namespace: rayJob.Namespace,
			},
			Spec: volcanoschedulingv1beta1.PodGroupSpec{
				MinMember: 3,
				MinResources: &corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("768m"),
					corev1.ResourceMemory: resource.MustParse("768Mi"),
				},
			},
		}
		err := fakeCli.Create(ctx, pg)
		require.NoError(err)

		// Verify PodGroup exists
		var retrievedPg volcanoschedulingv1beta1.PodGroup
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &retrievedPg)
		require.NoError(err)
		a.Equal(int32(3), retrievedPg.Spec.MinMember)

		// first call should succeed
		didUpdate, err := scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.True(didUpdate)

		// PodGroup should still exist with same values
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &retrievedPg)
		require.NoError(err)
		a.Equal(int32(0), retrievedPg.Spec.MinMember)
		a.True(retrievedPg.Spec.MinResources.Cpu().IsZero())
		a.True(retrievedPg.Spec.MinResources.Memory().IsZero())

		// second call should succeed without changes.
		didUpdate, err = scheduler.CleanupOnCompletion(ctx, &rayJob)
		require.NoError(err)
		a.False(didUpdate)

		// PodGroup should still exist with same values
		err = fakeCli.Get(ctx, client.ObjectKey{Namespace: rayJob.Namespace, Name: getAppPodGroupName(&rayJob)}, &retrievedPg)
		require.NoError(err)
		a.Equal(int32(0), retrievedPg.Spec.MinMember)
		a.True(retrievedPg.Spec.MinResources.Cpu().IsZero())
		a.True(retrievedPg.Spec.MinResources.Memory().IsZero())
	})
}
