package v1alpha3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestAddMetadataToChildResourceSetsDefaultSchedulerName(t *testing.T) {
	scheduler := &KubernetesWASV1Alpha3Scheduler{}
	rayCluster := newTestRayCluster(newWorkerGroup())

	pod := &corev1.Pod{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, pod, "head")
	require.Equal(t, corev1.DefaultSchedulerName, pod.Spec.SchedulerName)

	template := &corev1.PodTemplateSpec{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, template, "worker-group")
	require.Equal(t, corev1.DefaultSchedulerName, template.Spec.SchedulerName)
}

func TestName(t *testing.T) {
	scheduler := &KubernetesWASV1Alpha3Scheduler{}
	require.Equal(t, "kubernetes-was-v1alpha3", scheduler.Name())
}

func TestDoBatchSchedulingOnSubmissionCreatesWorkloadAndPodGroups(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	scheduler, fakeClient := newTestScheduler(t)

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.NoError(t, err)

	workload := &schedulingv1alpha3.Workload{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, workload)
	require.NoError(t, err)
	require.Len(t, workload.Spec.PodGroupTemplates, 1)
	assert.Equal(t, "cluster", workload.Spec.PodGroupTemplates[0].Name)
	require.NotNil(t, workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang)
	// MinCount = 1 head + 3 worker replicas.
	assert.Equal(t, int32(4), workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount)

	clusterPodGroup := &schedulingv1alpha3.PodGroup{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, clusterPodGroup)
	require.NoError(t, err)
	require.NotNil(t, clusterPodGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(4), clusterPodGroup.Spec.SchedulingPolicy.Gang.MinCount)
	require.NotNil(t, clusterPodGroup.Spec.WorkloadRef)
	assert.Equal(t, "test-cluster", clusterPodGroup.Spec.WorkloadRef.WorkloadName)
	assert.Equal(t, "cluster", clusterPodGroup.Spec.WorkloadRef.TemplateName)
}

func TestDoBatchSchedulingOnSubmissionGangsFloorWhenAutoscalingEnabled(t *testing.T) {
	ctx := context.Background()
	// minReplicas (2) is deliberately different from replicas (5) so the test can tell
	// the floor apart from the desired size.
	rayCluster := newTestRayCluster(newAutoscalingWorkerGroup("workers", 2, 5))
	enableAutoscaling := true
	rayCluster.Spec.EnableInTreeAutoscaling = &enableAutoscaling
	scheduler, fakeClient := newTestScheduler(t)

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.NoError(t, err)

	workload := &schedulingv1alpha3.Workload{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, workload))
	require.Len(t, workload.Spec.PodGroupTemplates, 1)
	require.NotNil(t, workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang)
	// Floor MinCount = 1 head + 2 minReplicas.
	assert.Equal(t, int32(3), workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount)

	clusterPodGroup := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, clusterPodGroup))
	require.NotNil(t, clusterPodGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(3), clusterPodGroup.Spec.SchedulingPolicy.Gang.MinCount)

	*rayCluster.Spec.WorkerGroupSpecs[0].MinReplicas = 3
	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster))

	require.NoError(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(workload), workload))
	require.Len(t, workload.Spec.PodGroupTemplates, 1)
	require.NotNil(t, workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang)
	assert.Equal(t, int32(4), workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount)

	require.NoError(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(clusterPodGroup), clusterPodGroup))
	require.NotNil(t, clusterPodGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(4), clusterPodGroup.Spec.SchedulingPolicy.Gang.MinCount)
}

func TestDoBatchSchedulingOnSubmissionSkipsAndCleansUpWithoutGangLabel(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	delete(rayCluster.Labels, utils.RayGangSchedulingEnabled)
	existingWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:      "test-cluster-cluster",
		Namespace: rayCluster.Namespace,
		Labels:    map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	scheduler, fakeClient := newTestScheduler(t, existingWorkload, existingPodGroup)

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "waiting for PodGroup default/test-cluster-cluster")
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, &schedulingv1alpha3.Workload{}))
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, &schedulingv1alpha3.PodGroup{})
	assert.True(t, apierrors.IsNotFound(err))

	err = scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.NoError(t, err)

	err = fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, &schedulingv1alpha3.Workload{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestDoBatchSchedulingOnSubmissionAllowsManyWorkerGroups(t *testing.T) {
	ctx := context.Background()
	// The single whole-cluster PodGroup uses only one of the 8 template slots, so
	// there is no longer a cap on the number of worker groups.
	workerGroupCount := schedulingv1alpha3.WorkloadMaxPodGroupTemplates + 2
	rayCluster := newTestRayCluster(newWorkerGroups(workerGroupCount)...)
	scheduler, fakeClient := newTestScheduler(t)

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.NoError(t, err)

	workload := &schedulingv1alpha3.Workload{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, workload)
	require.NoError(t, err)
	require.Len(t, workload.Spec.PodGroupTemplates, 1)
	assert.Equal(t, "cluster", workload.Spec.PodGroupTemplates[0].Name)
	require.NotNil(t, workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang)
	// MinCount = 1 head + one replica per worker group.
	assert.Equal(t, int32(1+workerGroupCount), workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount)

	clusterPodGroup := &schedulingv1alpha3.PodGroup{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, clusterPodGroup)
	require.NoError(t, err)
}

func TestAddMetadataToChildResourceSetsSchedulingGroup(t *testing.T) {
	scheduler := &KubernetesWASV1Alpha3Scheduler{}
	rayCluster := newTestRayCluster(newWorkerGroup())

	// Both head and worker pods reference the single whole-cluster PodGroup.
	headPod := &corev1.Pod{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, headPod, utils.RayNodeHeadGroupLabelValue)
	assertPodGroupMembership(t, headPod)
	assert.Equal(t, corev1.DefaultSchedulerName, headPod.Spec.SchedulerName)

	workerPod := &corev1.Pod{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, workerPod, "workers")
	assertPodGroupMembership(t, workerPod)
}

func TestAddMetadataToChildResourceSetsTemplateSchedulingGroup(t *testing.T) {
	scheduler := &KubernetesWASV1Alpha3Scheduler{}
	rayCluster := newTestRayCluster(newWorkerGroup())

	template := &corev1.PodTemplateSpec{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, template, "workers")

	assertPodGroupMembership(t, template)
	assert.Equal(t, corev1.DefaultSchedulerName, template.Spec.SchedulerName)
}

func TestAddMetadataToChildResourceSetsSchedulingGroupWhenAutoscalingEnabled(t *testing.T) {
	scheduler := &KubernetesWASV1Alpha3Scheduler{}
	rayCluster := newTestRayCluster(newWorkerGroup())
	enableAutoscaling := true
	rayCluster.Spec.EnableInTreeAutoscaling = &enableAutoscaling

	pod := &corev1.Pod{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, pod, "workers")

	// Autoscaling clusters are gang scheduled at the floor, so their pods still join
	// the whole-cluster PodGroup and get the default scheduler name.
	assertPodGroupMembership(t, pod)
	assert.Equal(t, corev1.DefaultSchedulerName, pod.Spec.SchedulerName)
}

func TestCleanupOnCompletionDeletesSchedulingResourcesInDependencyOrder(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	existingWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:       "test-cluster-cluster",
		Namespace:  rayCluster.Namespace,
		Finalizers: []string{podGroupProtectionFinalizer},
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	scheduler, fakeClient := newTestScheduler(t, existingWorkload, existingPodGroup)

	didCleanup, err := scheduler.CleanupOnCompletion(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "waiting for PodGroup default/test-cluster-cluster")
	assert.True(t, didCleanup)

	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, &schedulingv1alpha3.PodGroup{})
	assert.True(t, apierrors.IsNotFound(err))
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, &schedulingv1alpha3.Workload{}))

	didCleanup, err = scheduler.CleanupOnCompletion(ctx, rayCluster)
	require.NoError(t, err)
	assert.True(t, didCleanup)

	err = fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, &schedulingv1alpha3.Workload{})
	assert.True(t, apierrors.IsNotFound(err))
}

func TestCleanupOnCompletionSkipsForeignPodGroupAndDeletesOwnedWorkload(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	foreignRayCluster := newTestRayCluster(newWorkerGroup())
	foreignRayCluster.Name = "foreign-cluster"
	foreignRayCluster.UID = types.UID("foreign-cluster-uid")
	ownedWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	foreignPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:       clusterPodGroupName(rayCluster.Name),
		Namespace:  rayCluster.Namespace,
		Finalizers: []string{podGroupProtectionFinalizer},
	}}
	setRayClusterControllerReference(rayCluster, ownedWorkload)
	setRayClusterControllerReference(foreignRayCluster, foreignPodGroup)
	scheduler, fakeClient := newTestScheduler(t, ownedWorkload, foreignPodGroup)

	didCleanup, err := scheduler.CleanupOnCompletion(ctx, rayCluster)
	require.NoError(t, err)
	assert.True(t, didCleanup)

	// The owned Workload is deleted; the same-named foreign PodGroup is left untouched.
	err = fakeClient.Get(ctx, types.NamespacedName{Name: ownedWorkload.Name, Namespace: ownedWorkload.Namespace}, &schedulingv1alpha3.Workload{})
	assert.True(t, apierrors.IsNotFound(err))
	podGroup := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: foreignPodGroup.Name, Namespace: foreignPodGroup.Namespace}, podGroup))
	assert.Contains(t, podGroup.Finalizers, podGroupProtectionFinalizer)
}

func TestCleanupOnCompletionSkipsForeignWorkloadAndDeletesOwnedPodGroup(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	foreignRayCluster := newTestRayCluster(newWorkerGroup())
	foreignRayCluster.Name = "foreign-cluster"
	foreignRayCluster.UID = types.UID("foreign-cluster-uid")
	foreignWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	ownedPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:       clusterPodGroupName(rayCluster.Name),
		Namespace:  rayCluster.Namespace,
		Finalizers: []string{podGroupProtectionFinalizer},
	}}
	setRayClusterControllerReference(foreignRayCluster, foreignWorkload)
	setRayClusterControllerReference(rayCluster, ownedPodGroup)
	scheduler, fakeClient := newTestScheduler(t, foreignWorkload, ownedPodGroup)

	// The owned PodGroup is deleted first, so cleanup reports it is waiting for the
	// deletion to finish; the same-named foreign Workload is left untouched.
	didCleanup, err := scheduler.CleanupOnCompletion(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "waiting for PodGroup")
	assert.True(t, didCleanup)

	err = fakeClient.Get(ctx, types.NamespacedName{Name: ownedPodGroup.Name, Namespace: ownedPodGroup.Namespace}, &schedulingv1alpha3.PodGroup{})
	assert.True(t, apierrors.IsNotFound(err))
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: foreignWorkload.Name, Namespace: foreignWorkload.Namespace}, &schedulingv1alpha3.Workload{}))
}

func TestCleanupOnCompletionWaitsForPodGroupsBeforeDeletingWorkload(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	existingWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:       "test-cluster-cluster",
		Namespace:  rayCluster.Namespace,
		Labels:     map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
		Finalizers: []string{podGroupProtectionFinalizer, "example.com/retain"},
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	scheduler, fakeClient := newTestScheduler(t, existingWorkload, existingPodGroup)

	didCleanup, err := scheduler.CleanupOnCompletion(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "waiting for PodGroup default/test-cluster-cluster")
	assert.True(t, didCleanup)

	// Only the explicitly approved protection finalizer is removed. The unrelated
	// finalizer keeps the PodGroup terminating, and cleanup must retain the Workload.
	podGroup := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: existingPodGroup.Name, Namespace: existingPodGroup.Namespace}, podGroup))
	assert.NotContains(t, podGroup.Finalizers, podGroupProtectionFinalizer)
	assert.Contains(t, podGroup.Finalizers, "example.com/retain")
	assert.NotNil(t, podGroup.DeletionTimestamp)
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: existingWorkload.Name, Namespace: existingWorkload.Namespace}, &schedulingv1alpha3.Workload{}))
}

func TestCleanupOnCompletionNotFoundIsNoop(t *testing.T) {
	ctx := context.Background()
	scheduler, _ := newTestScheduler(t)

	didCleanup, err := scheduler.CleanupOnCompletion(ctx, newTestRayCluster(newWorkerGroup()))

	require.NoError(t, err)
	assert.False(t, didCleanup)
}

func TestSyncSchedulingResourcesRejectsForeignSameNameWorkload(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	foreignRayCluster := newTestRayCluster(newWorkerGroup())
	foreignRayCluster.Name = "foreign-cluster"
	foreignRayCluster.UID = types.UID("foreign-cluster-uid")
	desiredPolicy := buildClusterSchedulingPolicy(rayCluster)
	foreignWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: clusterPodGroupTemplateName, SchedulingPolicy: desiredPolicy},
		}},
	}
	setRayClusterControllerReference(foreignRayCluster, foreignWorkload)
	scheduler, fakeClient := newTestScheduler(t, foreignWorkload)

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Workload default/test-cluster already exists and is not owned by this RayCluster")

	// We do not adopt a same-named foreign Workload, and synchronization must not
	// proceed to create the PodGroup.
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: foreignWorkload.Name, Namespace: foreignWorkload.Namespace}, &schedulingv1alpha3.Workload{}))
	getErr := fakeClient.Get(ctx, types.NamespacedName{Name: clusterPodGroupName(rayCluster.Name), Namespace: rayCluster.Namespace}, &schedulingv1alpha3.PodGroup{})
	assert.True(t, apierrors.IsNotFound(getErr))
}

func TestSyncSchedulingResourcesRejectsForeignSameNamePodGroup(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	foreignRayCluster := newTestRayCluster(newWorkerGroup())
	foreignRayCluster.Name = "foreign-cluster"
	foreignRayCluster.UID = types.UID("foreign-cluster-uid")
	desiredPolicy := buildClusterSchedulingPolicy(rayCluster)
	existingWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: clusterPodGroupTemplateName, SchedulingPolicy: desiredPolicy},
		}},
	}
	foreignPodGroup := &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterPodGroupName(rayCluster.Name),
			Namespace: rayCluster.Namespace,
			Labels:    map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
		},
		Spec: schedulingv1alpha3.PodGroupSpec{SchedulingPolicy: desiredPolicy},
	}
	setRayClusterControllerReference(rayCluster, existingWorkload)
	setRayClusterControllerReference(foreignRayCluster, foreignPodGroup)
	scheduler, fakeClient := newTestScheduler(t, existingWorkload, foreignPodGroup)

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PodGroup default/test-cluster-cluster already exists and is not owned by this RayCluster")
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: foreignPodGroup.Name, Namespace: foreignPodGroup.Namespace}, &schedulingv1alpha3.PodGroup{}))
}

func TestBuildClusterSchedulingPolicy(t *testing.T) {
	one := int32(1)
	suspended := true

	tests := []struct {
		name         string
		cluster      *rayv1.RayCluster
		wantMinCount int32
	}{
		{
			name:         "head only",
			cluster:      newTestRayCluster(),
			wantMinCount: 1,
		},
		{
			name:         "single worker group counts head plus replicas",
			cluster:      newTestRayCluster(newWorkerGroupWithReplicas("workers", 3)),
			wantMinCount: 4,
		},
		{
			name:         "multiple worker groups sum replicas",
			cluster:      newTestRayCluster(newWorkerGroupWithReplicas("group-a", 1), newWorkerGroupWithReplicas("group-b", 2)),
			wantMinCount: 4,
		},
		{
			name:         "multi-host replicas multiply by num of hosts",
			cluster:      newTestRayCluster(workerGroupWithNumOfHosts("workers", 3, 2)),
			wantMinCount: 7,
		},
		{
			name:         "autoscaling gangs at floor of head plus minReplicas",
			cluster:      withAutoscaling(newTestRayCluster(newAutoscalingWorkerGroup("workers", 2, 5))),
			wantMinCount: 3,
		},
		{
			name: "suspended worker group contributes zero",
			cluster: newTestRayCluster(rayv1.WorkerGroupSpec{
				GroupName:   "workers",
				NumOfHosts:  1,
				Replicas:    &one,
				MinReplicas: &one,
				MaxReplicas: &one,
				Suspend:     &suspended,
			}),
			wantMinCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			policy := buildClusterSchedulingPolicy(tt.cluster)
			require.NotNil(t, policy.Gang)
			assert.Nil(t, policy.Basic)
			assert.Equal(t, tt.wantMinCount, policy.Gang.MinCount)
		})
	}
}

func TestSyncSchedulingResourcesPatchesStaleResourcesInPlace(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroupWithReplicas("workers", 5))
	existingWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace, UID: types.UID("stale-workload-uid")},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: "cluster", SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: 4}}},
		}},
	}
	existingPodGroup := &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-cluster",
			Namespace: rayCluster.Namespace,
			UID:       types.UID("stale-podgroup-uid"),
			Labels:    map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
		},
		Spec: schedulingv1alpha3.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: 4}},
		},
	}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	scheduler, fakeClient := newTestScheduler(t, existingWorkload, existingPodGroup)

	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster))

	workload := &schedulingv1alpha3.Workload{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, workload))
	require.Len(t, workload.Spec.PodGroupTemplates, 1)
	require.NotNil(t, workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang)
	// MinCount = 1 head + 5 worker replicas; patched in place so the UID is preserved.
	assert.Equal(t, int32(6), workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount)
	assert.Equal(t, existingWorkload.UID, workload.UID)

	podGroup := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, podGroup))
	require.NotNil(t, podGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(6), podGroup.Spec.SchedulingPolicy.Gang.MinCount)
	assert.Equal(t, existingPodGroup.UID, podGroup.UID)
}

func TestSyncSchedulingResourcesRejectsConcurrentUpdates(t *testing.T) {
	for _, kind := range []string{"Workload", "PodGroup"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			rayCluster := newTestRayCluster(newWorkerGroup())
			scheduler, _ := newTestScheduler(t)
			workload, podGroup, err := scheduler.buildSchedulingResources(rayCluster)
			require.NoError(t, err)

			var existing client.Object
			var minCount func() int32
			var syncResource func() error
			if kind == "Workload" {
				live := workload.DeepCopy()
				existing = live
				live.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount = 3
				minCount = func() int32 { return live.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount }
				syncResource = func() error { return scheduler.syncWorkload(ctx, rayCluster, workload) }
			} else {
				live := podGroup.DeepCopy()
				existing = live
				live.Spec.SchedulingPolicy.Gang.MinCount = 3
				minCount = func() int32 { return live.Spec.SchedulingPolicy.Gang.MinCount }
				syncResource = func() error { return scheduler.syncPodGroup(ctx, rayCluster, podGroup) }
			}
			updated := false
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(newTestScheme(t)).
				WithObjects(existing).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(ctx context.Context, cli client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						if !updated {
							// Change the live resource after the scheduler's read, before its patch.
							require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(existing), existing))
							existing.SetAnnotations(map[string]string{"concurrent-update": "true"})
							require.NoError(t, cli.Update(ctx, existing))
							updated = true
						}
						return cli.Patch(ctx, obj, patch, opts...)
					},
				}).Build()
			scheduler.cli = fakeClient

			require.True(t, apierrors.IsConflict(syncResource()))
			require.NoError(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(existing), existing))
			assert.Equal(t, int32(3), minCount())

			require.NoError(t, syncResource())
			require.NoError(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(existing), existing))
			assert.Equal(t, "true", existing.GetAnnotations()["concurrent-update"])
			assert.Equal(t, int32(4), minCount())
		})
	}
}

// TestSyncSchedulingResourcesPatchesStalePodGroupInPlace also locks in that the patch never
// deletes the PodGroup and preserves its protection finalizer.
func TestSyncSchedulingResourcesPatchesStalePodGroupInPlace(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup()) // 3 replicas -> desired MinCount 4

	// The Workload matches the desired spec (not stale), but the PodGroup drifted
	// to an old MinCount. The stale PodGroup must be patched in place.
	existingWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: "cluster", SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: 4}}},
		}},
	}
	existingPodGroup := &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-cluster-cluster",
			Namespace:  rayCluster.Namespace,
			UID:        types.UID("stale-podgroup-uid"),
			Labels:     map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
			Finalizers: []string{podGroupProtectionFinalizer},
		},
		Spec: schedulingv1alpha3.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: 3}},
		},
	}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	scheduler, fakeClient := newTestScheduler(t, existingWorkload, existingPodGroup)

	// A rescale mutates gang.minCount in place. The PodGroup must not be deleted, and
	// its UID and protection finalizer must survive.
	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster))

	podGroup := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, podGroup))
	require.NotNil(t, podGroup.Spec.SchedulingPolicy.Gang)
	// MinCount = 1 head + 3 worker replicas; patched in place so the UID is preserved.
	assert.Equal(t, int32(4), podGroup.Spec.SchedulingPolicy.Gang.MinCount)
	assert.Equal(t, existingPodGroup.UID, podGroup.UID)
	assert.Nil(t, podGroup.DeletionTimestamp)
	assert.Contains(t, podGroup.Finalizers, podGroupProtectionFinalizer)
}

func TestDoBatchSchedulingOnSubmissionIsIdempotentWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())
	scheduler, fakeClient := newTestScheduler(t)

	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster))

	workloadAfterFirst := &schedulingv1alpha3.Workload{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, workloadAfterFirst))
	podGroupAfterFirst := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, podGroupAfterFirst))

	// A second reconcile with an unchanged spec must be a no-op: the existing
	// Workload is not stale and the existing PodGroup already exists, so neither
	// resource is deleted or recreated.
	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster))

	workloadAfterSecond := &schedulingv1alpha3.Workload{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, workloadAfterSecond))
	require.Len(t, workloadAfterSecond.Spec.PodGroupTemplates, 1)
	assert.Equal(t, workloadAfterFirst.UID, workloadAfterSecond.UID)
	assert.Equal(t, workloadAfterFirst.ResourceVersion, workloadAfterSecond.ResourceVersion, "Workload should not be recreated on an unchanged reconcile")

	podGroupAfterSecond := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, podGroupAfterSecond))
	assert.Equal(t, podGroupAfterFirst.UID, podGroupAfterSecond.UID)
	assert.Equal(t, podGroupAfterFirst.ResourceVersion, podGroupAfterSecond.ResourceVersion, "PodGroup should not be recreated on an unchanged reconcile")
}

func TestSyncSchedulingResourcesLeavesTerminatingPodGroupToKubernetes(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())

	// A non-stale Workload already exists so reconciliation proceeds to the PodGroup.
	// Build its template from the cluster spec so the Workload stays non-stale even
	// if the test helpers' replica counts change.
	desiredPolicy := buildClusterSchedulingPolicy(rayCluster)
	existingWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: clusterPodGroupTemplateName, SchedulingPolicy: desiredPolicy},
		}},
	}
	// The PodGroup is mid-deletion (e.g. deleted out of band) with a stale minCount.
	deletionTime := metav1.NewTime(time.Now())
	existingPodGroup := &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-cluster-cluster",
			Namespace:         rayCluster.Namespace,
			Labels:            map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
			Finalizers:        []string{podGroupProtectionFinalizer},
			DeletionTimestamp: &deletionTime,
		},
		Spec: schedulingv1alpha3.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: desiredPolicy.Gang.MinCount - 1},
			},
		},
	}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	scheduler, fakeClient := newTestScheduler(t, existingWorkload, existingPodGroup)

	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster))

	podGroup := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: existingPodGroup.Name, Namespace: existingPodGroup.Namespace}, podGroup))
	assert.Contains(t, podGroup.Finalizers, podGroupProtectionFinalizer)
	assert.NotNil(t, podGroup.DeletionTimestamp)
	require.NotNil(t, podGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, desiredPolicy.Gang.MinCount, podGroup.Spec.SchedulingPolicy.Gang.MinCount)
}

func TestSyncSchedulingResourcesRetriesWhenWorkloadBeingDeleted(t *testing.T) {
	ctx := context.Background()
	rayCluster := newTestRayCluster(newWorkerGroup())

	// A non-stale Workload exists but is mid-deletion. The scheduler must not proceed
	// to create a PodGroup against a Workload that is still being deleted.
	desiredPolicy := buildClusterSchedulingPolicy(rayCluster)
	deletionTime := metav1.NewTime(time.Now())
	existingWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:              rayCluster.Name,
			Namespace:         rayCluster.Namespace,
			Finalizers:        []string{podGroupProtectionFinalizer},
			DeletionTimestamp: &deletionTime,
		},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: clusterPodGroupTemplateName, SchedulingPolicy: desiredPolicy},
		}},
	}
	setRayClusterControllerReference(rayCluster, existingWorkload)
	scheduler, fakeClient := newTestScheduler(t, existingWorkload)

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is being deleted")

	// No PodGroup should have been created while the Workload is terminating.
	podGroup := &schedulingv1alpha3.PodGroup{}
	getErr := fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, podGroup)
	assert.True(t, apierrors.IsNotFound(getErr))
}

func TestSchedulingV1alpha3Available(t *testing.T) {
	tests := []struct {
		name        string
		handler     http.HandlerFunc
		wantErr     bool
		errContains string
	}{
		{
			name: "API available returns resource list",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/apis/scheduling.k8s.io/v1alpha3" {
					writer.Header().Set("Content-Type", "application/json")
					resourceList := metav1.APIResourceList{
						GroupVersion: "scheduling.k8s.io/v1alpha3",
						APIResources: []metav1.APIResource{
							{Name: "workloads", Kind: "Workload", Namespaced: true},
							{Name: "podgroups", Kind: "PodGroup", Namespaced: true},
						},
					}
					assert.NoError(t, json.NewEncoder(writer).Encode(resourceList))
					return
				}
				http.NotFound(writer, request)
			},
		},
		{
			name: "API available returns empty resource list",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/apis/scheduling.k8s.io/v1alpha3" {
					writer.Header().Set("Content-Type", "application/json")
					assert.NoError(t, json.NewEncoder(writer).Encode(metav1.APIResourceList{GroupVersion: "scheduling.k8s.io/v1alpha3"}))
					return
				}
				http.NotFound(writer, request)
			},
		},
		{
			name: "API not available returns 404",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				http.NotFound(writer, request)
			},
			wantErr:     true,
			errContains: "scheduling.k8s.io/v1alpha3 API is not available",
		},
		{
			name: "API not available returns server error",
			handler: func(writer http.ResponseWriter, _ *http.Request) {
				http.Error(writer, "internal server error", http.StatusInternalServerError)
			},
			wantErr:     true,
			errContains: "scheduling.k8s.io/v1alpha3 API is not available",
		},
		{
			name: "different group version does not satisfy v1alpha3",
			handler: func(writer http.ResponseWriter, request *http.Request) {
				if request.URL.Path == "/apis/scheduling.k8s.io/v1" {
					writer.Header().Set("Content-Type", "application/json")
					assert.NoError(t, json.NewEncoder(writer).Encode(metav1.APIResourceList{GroupVersion: "scheduling.k8s.io/v1"}))
					return
				}
				http.NotFound(writer, request)
			},
			wantErr:     true,
			errContains: "scheduling.k8s.io/v1alpha3 API is not available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			err := schedulingV1alpha3Available(&rest.Config{Host: server.URL})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContains)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSchedulingV1alpha3AvailableAllowsNilConfig(t *testing.T) {
	require.NoError(t, schedulingV1alpha3Available(nil))
}

func TestSchedulingV1alpha3AvailableUnreachableServer(t *testing.T) {
	err := schedulingV1alpha3Available(&rest.Config{Host: "http://127.0.0.1:1"})
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "scheduling.k8s.io/v1alpha3 API is not available") || strings.Contains(err.Error(), "connection refused"))
}

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	require.NoError(t, schedulingv1alpha3.AddToScheme(scheme))
	return scheme
}

// newTestScheduler builds a scheduler backed by a fake client seeded with objects, returning
// both so tests can assert against the client.
func newTestScheduler(t *testing.T, objects ...client.Object) (*KubernetesWASV1Alpha3Scheduler, client.Client) {
	t.Helper()
	fakeClient := clientFake.NewClientBuilder().WithScheme(newTestScheme(t)).WithObjects(objects...).Build()
	return &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}, fakeClient
}

// assertPodGroupMembership asserts that a pod or pod template carries the whole-cluster
// scheduling group.
func assertPodGroupMembership(t *testing.T, obj metav1.Object) {
	t.Helper()
	var schedulingGroup *corev1.PodSchedulingGroup
	switch o := obj.(type) {
	case *corev1.Pod:
		schedulingGroup = o.Spec.SchedulingGroup
	case *corev1.PodTemplateSpec:
		schedulingGroup = o.Spec.SchedulingGroup
	default:
		t.Fatalf("unsupported object type %T", obj)
	}
	require.NotNil(t, schedulingGroup)
	require.NotNil(t, schedulingGroup.PodGroupName)
	assert.Equal(t, "test-cluster-cluster", *schedulingGroup.PodGroupName)
}

func TestSchedulingSkippedWhenGangSchedulingDisabled(t *testing.T) {
	rayCluster := newTestRayCluster(newWorkerGroup())
	require.Empty(t, schedulingSkipReason(rayCluster))

	delete(rayCluster.Labels, utils.RayGangSchedulingEnabled)
	require.Equal(t, skipReasonGangSchedulingDisabled, schedulingSkipReason(rayCluster))

	rayCluster.Labels[utils.RayGangSchedulingEnabled] = "false"
	require.Equal(t, skipReasonGangSchedulingDisabled, schedulingSkipReason(rayCluster))

	rayCluster.Labels[utils.RayGangSchedulingEnabled] = "False"
	require.Equal(t, skipReasonGangSchedulingDisabled, schedulingSkipReason(rayCluster))

	rayCluster.Labels[utils.RayGangSchedulingEnabled] = "foo"
	require.Equal(t, skipReasonGangSchedulingDisabled, schedulingSkipReason(rayCluster))

	rayCluster.Labels[utils.RayGangSchedulingEnabled] = "True"
	require.Empty(t, schedulingSkipReason(rayCluster))
}

func newTestRayCluster(workerGroups ...rayv1.WorkerGroupSpec) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			UID:       types.UID("test-cluster-uid"),
			Labels:    map[string]string{utils.RayGangSchedulingEnabled: "true"},
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec:    rayv1.HeadGroupSpec{Template: corev1.PodTemplateSpec{}},
			WorkerGroupSpecs: workerGroups,
		},
	}
}

func setRayClusterControllerReference(rayCluster *rayv1.RayCluster, objects ...metav1.Object) {
	ownerReference := *metav1.NewControllerRef(rayCluster, rayv1.GroupVersion.WithKind("RayCluster"))
	for _, object := range objects {
		if object.GetUID() == "" {
			object.SetUID(types.UID(object.GetName() + "-uid"))
		}
		object.SetOwnerReferences([]metav1.OwnerReference{ownerReference})
	}
}

func newWorkerGroup() rayv1.WorkerGroupSpec {
	return newWorkerGroupWithReplicas("workers", 3)
}

func newWorkerGroupWithReplicas(groupName string, replicas int32) rayv1.WorkerGroupSpec {
	return rayv1.WorkerGroupSpec{
		GroupName:   groupName,
		NumOfHosts:  1,
		Replicas:    &replicas,
		MinReplicas: &replicas,
		MaxReplicas: &replicas,
		Template:    corev1.PodTemplateSpec{},
	}
}

func workerGroupWithNumOfHosts(groupName string, replicas int32, numOfHosts int32) rayv1.WorkerGroupSpec {
	workerGroup := newWorkerGroupWithReplicas(groupName, replicas)
	workerGroup.NumOfHosts = numOfHosts
	return workerGroup
}

func newAutoscalingWorkerGroup(groupName string, minReplicas, replicas int32) rayv1.WorkerGroupSpec {
	workerGroup := newWorkerGroupWithReplicas(groupName, replicas)
	maxReplicas := replicas
	workerGroup.MinReplicas = &minReplicas
	workerGroup.MaxReplicas = &maxReplicas
	return workerGroup
}

func withAutoscaling(rayCluster *rayv1.RayCluster) *rayv1.RayCluster {
	enable := true
	rayCluster.Spec.EnableInTreeAutoscaling = &enable
	return rayCluster
}

func newWorkerGroups(count int) []rayv1.WorkerGroupSpec {
	workerGroups := make([]rayv1.WorkerGroupSpec, 0, count)
	for index := range count {
		workerGroups = append(workerGroups, newWorkerGroupWithReplicas(fmt.Sprintf("group-%d", index), 1))
	}
	return workerGroups
}
