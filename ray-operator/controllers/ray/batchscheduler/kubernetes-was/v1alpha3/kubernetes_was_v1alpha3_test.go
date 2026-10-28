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
	scheme := newTestScheme(t)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}
	rayCluster := newTestRayCluster(newWorkerGroup())

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

func TestDoBatchSchedulingOnSubmissionSkipsAndCleansUpWhenAutoscalingEnabled(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	rayCluster := newTestRayCluster(newWorkerGroup())
	existingWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:      "test-cluster-cluster",
		Namespace: rayCluster.Namespace,
		Labels:    map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload, existingPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}
	enableAutoscaling := true
	rayCluster.Spec.EnableInTreeAutoscaling = &enableAutoscaling

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

func TestDoBatchSchedulingOnSubmissionSkipsAndCleansUpWithoutGangLabel(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	rayCluster := newTestRayCluster(newWorkerGroup())
	delete(rayCluster.Labels, utils.RayGangSchedulingEnabled)
	existingWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:      "test-cluster-cluster",
		Namespace: rayCluster.Namespace,
		Labels:    map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload, existingPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

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
	scheme := newTestScheme(t)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}
	// The single whole-cluster PodGroup uses only one of the 8 template slots, so
	// there is no longer a cap on the number of worker groups.
	workerGroupCount := schedulingv1alpha3.WorkloadMaxPodGroupTemplates + 2
	rayCluster := newTestRayCluster(newWorkerGroups(workerGroupCount)...)

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
	require.NotNil(t, headPod.Spec.SchedulingGroup)
	require.NotNil(t, headPod.Spec.SchedulingGroup.PodGroupName)
	assert.Equal(t, "test-cluster-cluster", *headPod.Spec.SchedulingGroup.PodGroupName)
	assert.Equal(t, corev1.DefaultSchedulerName, headPod.Spec.SchedulerName)

	workerPod := &corev1.Pod{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, workerPod, "workers")
	require.NotNil(t, workerPod.Spec.SchedulingGroup)
	require.NotNil(t, workerPod.Spec.SchedulingGroup.PodGroupName)
	assert.Equal(t, "test-cluster-cluster", *workerPod.Spec.SchedulingGroup.PodGroupName)
}

func TestAddMetadataToChildResourceSetsTemplateSchedulingGroup(t *testing.T) {
	scheduler := &KubernetesWASV1Alpha3Scheduler{}
	rayCluster := newTestRayCluster(newWorkerGroup())

	template := &corev1.PodTemplateSpec{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, template, "workers")

	require.NotNil(t, template.Spec.SchedulingGroup)
	require.NotNil(t, template.Spec.SchedulingGroup.PodGroupName)
	assert.Equal(t, "test-cluster-cluster", *template.Spec.SchedulingGroup.PodGroupName)
	assert.Equal(t, corev1.DefaultSchedulerName, template.Spec.SchedulerName)
}

func TestAddMetadataToChildResourceSkipsSchedulingGroupWhenAutoscalingEnabled(t *testing.T) {
	scheduler := &KubernetesWASV1Alpha3Scheduler{}
	rayCluster := newTestRayCluster(newWorkerGroup())
	enableAutoscaling := true
	rayCluster.Spec.EnableInTreeAutoscaling = &enableAutoscaling

	pod := &corev1.Pod{}
	scheduler.AddMetadataToChildResource(context.Background(), rayCluster, pod, "workers")

	// Skipped clusters are left untouched: no scheduling group and no forced scheduler name.
	assert.Nil(t, pod.Spec.SchedulingGroup)
	assert.Empty(t, pod.Spec.SchedulerName)
}

func TestCleanupOnCompletionDeletesSchedulingResourcesInDependencyOrder(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	rayCluster := newTestRayCluster(newWorkerGroup())
	existingWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:       "test-cluster-cluster",
		Namespace:  rayCluster.Namespace,
		Finalizers: []string{podGroupProtectionFinalizer},
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload, existingPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

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
	scheme := newTestScheme(t)
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
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(ownedWorkload, foreignPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

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
	scheme := newTestScheme(t)
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
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(foreignWorkload, ownedPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

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
	scheme := newTestScheme(t)
	rayCluster := newTestRayCluster(newWorkerGroup())
	existingWorkload := &schedulingv1alpha3.Workload{ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace}}
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:       "test-cluster-cluster",
		Namespace:  rayCluster.Namespace,
		Labels:     map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
		Finalizers: []string{podGroupProtectionFinalizer, "example.com/retain"},
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload, existingPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

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
	scheme := newTestScheme(t)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

	didCleanup, err := scheduler.CleanupOnCompletion(ctx, newTestRayCluster(newWorkerGroup()))

	require.NoError(t, err)
	assert.False(t, didCleanup)
}

func TestSyncSchedulingResourcesRejectsForeignSameNameWorkload(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
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
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(foreignWorkload).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

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
	scheme := newTestScheme(t)
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
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload, foreignPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

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

func TestSyncSchedulingResourcesReplacesStaleResourcesAcrossReconciles(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	rayCluster := newTestRayCluster(newWorkerGroupWithReplicas("workers", 5))
	existingWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: "cluster", SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: 4}}},
		}},
	}
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:      "test-cluster-cluster",
		Namespace: rayCluster.Namespace,
		Labels:    map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload, existingPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleted PodGroup default/test-cluster-cluster before replacing stale Workload")
	// Replacement teardown is dependency ordered: the Workload remains until its
	// runtime PodGroup has been removed.
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, &schedulingv1alpha3.Workload{}))

	err = scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleted stale Workload")

	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster))

	workload := &schedulingv1alpha3.Workload{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: rayCluster.Name, Namespace: rayCluster.Namespace}, workload)
	require.NoError(t, err)
	require.Len(t, workload.Spec.PodGroupTemplates, 1)
	require.NotNil(t, workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang)
	// MinCount = 1 head + 5 worker replicas.
	assert.Equal(t, int32(6), workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount)

	podGroup := &schedulingv1alpha3.PodGroup{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, podGroup)
	require.NoError(t, err)
	require.NotNil(t, podGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, int32(6), podGroup.Spec.SchedulingPolicy.Gang.MinCount)
}

func TestSyncSchedulingResourcesRecreatesStalePodGroup(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	rayCluster := newTestRayCluster(newWorkerGroup()) // 3 replicas -> desired MinCount 4

	// The Workload matches the desired spec (not stale), but the PodGroup drifted
	// to an old MinCount. The stale PodGroup must be deleted and recreated.
	existingWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: "cluster", SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: 4}}},
		}},
	}
	existingPodGroup := &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster-cluster",
			Namespace: rayCluster.Namespace,
			Labels:    map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
		},
		Spec: schedulingv1alpha3.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: 3}},
		},
	}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload, existingPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleted stale PodGroup")
	require.NoError(t, scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster))

	podGroup := &schedulingv1alpha3.PodGroup{}
	err = fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, podGroup)
	require.NoError(t, err)
	require.NotNil(t, podGroup.Spec.SchedulingPolicy.Gang)
	// MinCount = 1 head + 3 worker replicas.
	assert.Equal(t, int32(4), podGroup.Spec.SchedulingPolicy.Gang.MinCount)
}

func TestSyncPodGroupDeleteUsesUIDPreconditionAndDefersRecreation(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	rayCluster := newTestRayCluster(newWorkerGroup())
	desiredPolicy := buildClusterSchedulingPolicy(rayCluster)
	existingWorkload := &schedulingv1alpha3.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: rayCluster.Name, Namespace: rayCluster.Namespace},
		Spec: schedulingv1alpha3.WorkloadSpec{PodGroupTemplates: []schedulingv1alpha3.PodGroupTemplate{
			{Name: clusterPodGroupTemplateName, SchedulingPolicy: desiredPolicy},
		}},
	}
	existingPodGroup := &schedulingv1alpha3.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:       clusterPodGroupName(rayCluster.Name),
			Namespace:  rayCluster.Namespace,
			UID:        types.UID("stale-podgroup-uid"),
			Labels:     map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
			Finalizers: []string{podGroupProtectionFinalizer, "example.com/retain"},
		},
		Spec: schedulingv1alpha3.PodGroupSpec{
			SchedulingPolicy: schedulingv1alpha3.PodGroupSchedulingPolicy{
				Gang: &schedulingv1alpha3.GangSchedulingPolicy{MinCount: desiredPolicy.Gang.MinCount - 1},
			},
		},
	}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	deleteCalled := false
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingWorkload, existingPodGroup).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(_ context.Context, _ client.WithWatch, object client.Object, options ...client.DeleteOption) error {
				podGroup, ok := object.(*schedulingv1alpha3.PodGroup)
				require.True(t, ok)
				deleteOptions := (&client.DeleteOptions{}).ApplyOptions(options)
				require.NotNil(t, deleteOptions.Preconditions)
				require.NotNil(t, deleteOptions.Preconditions.UID)
				assert.Equal(t, podGroup.UID, *deleteOptions.Preconditions.UID)
				deleteCalled = true
				// Simulate an API server that accepted Delete but has not removed the
				// object yet. Reconciliation must not create its replacement now.
				return nil
			},
		}).
		Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleted stale PodGroup")
	assert.True(t, deleteCalled)

	podGroup := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: existingPodGroup.Name, Namespace: existingPodGroup.Namespace}, podGroup))
	require.NotNil(t, podGroup.Spec.SchedulingPolicy.Gang)
	assert.Equal(t, desiredPolicy.Gang.MinCount-1, podGroup.Spec.SchedulingPolicy.Gang.MinCount)
	assert.NotContains(t, podGroup.Finalizers, podGroupProtectionFinalizer)
	assert.Contains(t, podGroup.Finalizers, "example.com/retain")
}

func TestDoBatchSchedulingOnSubmissionIsIdempotentWhenUnchanged(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}
	rayCluster := newTestRayCluster(newWorkerGroup())

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

func TestSyncSchedulingResourcesRemovesProtectionFinalizerWhenPodGroupBeingDeleted(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
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
	// The PodGroup is mid-deletion with the protection finalizer still present.
	deletionTime := metav1.NewTime(time.Now())
	existingPodGroup := &schedulingv1alpha3.PodGroup{ObjectMeta: metav1.ObjectMeta{
		Name:              "test-cluster-cluster",
		Namespace:         rayCluster.Namespace,
		Labels:            map[string]string{utils.RayClusterLabelKey: rayCluster.Name},
		Finalizers:        []string{podGroupProtectionFinalizer, "example.com/retain"},
		DeletionTimestamp: &deletionTime,
	}}
	setRayClusterControllerReference(rayCluster, existingWorkload, existingPodGroup)
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload, existingPodGroup).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is being deleted")

	podGroup := &schedulingv1alpha3.PodGroup{}
	require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: existingPodGroup.Name, Namespace: existingPodGroup.Namespace}, podGroup))
	assert.NotContains(t, podGroup.Finalizers, podGroupProtectionFinalizer)
	assert.Contains(t, podGroup.Finalizers, "example.com/retain")
	assert.NotNil(t, podGroup.DeletionTimestamp)
}

func TestSyncSchedulingResourcesRetriesWhenWorkloadBeingDeleted(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
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
	fakeClient := clientFake.NewClientBuilder().WithScheme(scheme).WithObjects(existingWorkload).Build()
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: fakeClient}

	err := scheduler.DoBatchSchedulingOnSubmission(ctx, rayCluster)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is being deleted")

	// No PodGroup should have been created while the Workload is terminating.
	podGroup := &schedulingv1alpha3.PodGroup{}
	getErr := fakeClient.Get(ctx, types.NamespacedName{Name: "test-cluster-cluster", Namespace: rayCluster.Namespace}, podGroup)
	assert.True(t, apierrors.IsNotFound(getErr))
}

func TestIsWorkloadStale(t *testing.T) {
	baseCluster := newTestRayCluster(newWorkerGroupWithReplicas("workers", 3))
	scheduler := &KubernetesWASV1Alpha3Scheduler{cli: clientFake.NewClientBuilder().WithScheme(newTestScheme(t)).Build()}
	baseWorkload, _, err := scheduler.buildSchedulingResources(baseCluster)
	require.NoError(t, err)

	tests := []struct {
		name      string
		workload  *schedulingv1alpha3.Workload
		cluster   *rayv1.RayCluster
		wantStale bool
	}{
		{name: "no change", workload: baseWorkload.DeepCopy(), cluster: baseCluster, wantStale: false},
		{name: "worker group added", workload: baseWorkload.DeepCopy(), cluster: newTestRayCluster(newWorkerGroupWithReplicas("workers", 3), newWorkerGroupWithReplicas("gpu", 1)), wantStale: true},
		{name: "worker group removed", workload: baseWorkload.DeepCopy(), cluster: newTestRayCluster(), wantStale: true},
		{name: "worker group renamed with same total replicas is not stale", workload: baseWorkload.DeepCopy(), cluster: newTestRayCluster(newWorkerGroupWithReplicas("renamed", 3)), wantStale: false},
		{name: "replica count changed", workload: baseWorkload.DeepCopy(), cluster: newTestRayCluster(newWorkerGroupWithReplicas("workers", 5)), wantStale: true},
		{name: "num hosts changed", workload: baseWorkload.DeepCopy(), cluster: newTestRayCluster(workerGroupWithNumOfHosts("workers", 3, 2)), wantStale: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			desired, _, err := scheduler.buildSchedulingResources(tt.cluster)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStale, isWorkloadStale(tt.workload, desired))
		})
	}
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

func newWorkerGroups(count int) []rayv1.WorkerGroupSpec {
	workerGroups := make([]rayv1.WorkerGroupSpec, 0, count)
	for index := range count {
		workerGroups = append(workerGroups, newWorkerGroupWithReplicas(fmt.Sprintf("group-%d", index), 1))
	}
	return workerGroups
}
