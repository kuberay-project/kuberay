package ray

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func newRetentionTestJob(retention int32) *rayv1.RayJob {
	return &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: "retained-job", Namespace: "default", UID: "retained-job"},
		Spec: rayv1.RayJobSpec{
			Entrypoint:               "echo hello",
			ShutdownAfterJobFinishes: true,
			TTLSecondsAfterFinished:  300,
			RayClusterSpec: &rayv1.RayClusterSpec{HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "ray-head", Image: "rayproject/ray:latest"}},
				}},
			}},
			DeletionStrategy: &rayv1.DeletionStrategy{DeletionRules: []rayv1.DeletionRule{
				{Policy: rayv1.DeleteSelf, Condition: rayv1.DeletionCondition{JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusComplete), TTLSeconds: retention}},
				{Policy: rayv1.DeleteSelf, Condition: rayv1.DeletionCondition{JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed), TTLSeconds: retention}},
			}},
		},
	}
}

func newRetentionTestReconciler(t *testing.T, job *rayv1.RayJob, objects ...client.Object) *RayJobReconciler {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(s))
	require.NoError(t, corev1.AddToScheme(s))
	require.NoError(t, batchv1.AddToScheme(s))
	objects = append(objects, job)
	c := clientFake.NewClientBuilder().WithScheme(s).WithObjects(objects...).WithStatusSubresource(job).Build()
	return &RayJobReconciler{Client: c, Scheme: s, Recorder: events.NewFakeRecorder(100)}
}

func TestRayJobIndependentRetention(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, true)
	// Explicit deletion rules continue to take precedence over the legacy environment variable.
	t.Setenv(utils.DELETE_RAYJOB_CR_AFTER_JOB_FINISHES, "true")
	for _, tc := range []struct {
		name       string
		status     rayv1.JobStatus
		deployment rayv1.JobDeploymentStatus
		retention  int32
	}{
		{"successful normal job", rayv1.JobStatusSucceeded, rayv1.JobDeploymentStatusComplete, 2592000},
		{"failed normal job", rayv1.JobStatusFailed, rayv1.JobDeploymentStatusFailed, 2592000},
		{"successful test job", rayv1.JobStatusSucceeded, rayv1.JobDeploymentStatusComplete, 604800},
		{"failed test job", rayv1.JobStatusFailed, rayv1.JobDeploymentStatusFailed, 604800},
		{"deployment failed before submission", rayv1.JobStatusNew, rayv1.JobDeploymentStatusFailed, 604800},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			job := newRetentionTestJob(tc.retention)
			job.Status = rayv1.RayJobStatus{
				JobStatus: tc.status, JobDeploymentStatus: tc.deployment,
				RayClusterName: "retained-cluster", EndTime: &metav1.Time{Time: time.Now().Add(-time.Minute)},
			}
			cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: job.Status.RayClusterName, Namespace: job.Namespace}}
			r := newRetentionTestReconciler(t, job, cluster)
			request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}
			originalSpec := job.DeepCopy().Spec

			result, err := r.Reconcile(ctx, request)
			require.NoError(t, err)
			require.InDelta(t, 242, result.RequeueAfter.Seconds(), 5, "schedule cluster cleanup before CR retention")
			require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(cluster), &rayv1.RayCluster{}))
			require.NoError(t, r.Get(ctx, request.NamespacedName, job))
			require.Equal(t, originalSpec, job.Spec)

			// Advance past cluster cleanup without waiting in real time.
			job.Status.EndTime = &metav1.Time{Time: time.Now().Add(-301 * time.Second)}
			require.NoError(t, r.Status().Update(ctx, job))
			result, err = r.Reconcile(ctx, request)
			require.NoError(t, err)
			require.True(t, apierrors.IsNotFound(r.Get(ctx, client.ObjectKeyFromObject(cluster), &rayv1.RayCluster{})))
			require.NoError(t, r.Get(ctx, request.NamespacedName, job), "RayJob remains after cluster deletion")
			require.InDelta(t, float64(tc.retention-301+2), result.RequeueAfter.Seconds(), 5)

			// A new reconciler must recover the remaining deadline from persisted state.
			r = &RayJobReconciler{Client: r.Client, Scheme: r.Scheme, Recorder: events.NewFakeRecorder(100)}
			result, err = r.Reconcile(ctx, request)
			require.NoError(t, err)
			require.InDelta(t, float64(tc.retention-301+2), result.RequeueAfter.Seconds(), 5)
			require.NoError(t, r.Get(ctx, request.NamespacedName, job))
			require.Equal(t, originalSpec, job.Spec)

			job.Status.EndTime = &metav1.Time{Time: time.Now().Add(-time.Duration(tc.retention+1) * time.Second)}
			require.NoError(t, r.Status().Update(ctx, job))
			result, err = r.Reconcile(ctx, request)
			require.NoError(t, err)
			require.Zero(t, result.RequeueAfter)
			require.True(t, apierrors.IsNotFound(r.Get(ctx, request.NamespacedName, &rayv1.RayJob{})))
		})
	}
}

func TestRayJobShutdownWithoutMatchingRetentionRule(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, true)
	job := newRetentionTestJob(604800)
	job.Spec.DeletionStrategy.DeletionRules = job.Spec.DeletionStrategy.DeletionRules[:1] // Complete only.
	job.Status = rayv1.RayJobStatus{
		JobDeploymentStatus: rayv1.JobDeploymentStatusFailed, RayClusterName: "failed-cluster",
		EndTime: &metav1.Time{Time: time.Now().Add(-301 * time.Second)},
	}
	cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: job.Status.RayClusterName, Namespace: job.Namespace}}
	r := newRetentionTestReconciler(t, job, cluster)
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}
	for range 2 {
		result, err := r.Reconcile(context.Background(), request)
		require.NoError(t, err)
		require.Zero(t, result.RequeueAfter)
		require.True(t, apierrors.IsNotFound(r.Get(context.Background(), client.ObjectKeyFromObject(cluster), &rayv1.RayCluster{})))
		require.NoError(t, r.Get(context.Background(), request.NamespacedName, &rayv1.RayJob{}))
	}
}

func TestRayJobRetentionOverdue(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, true)
	for _, tc := range []struct {
		name      string
		retention int32
	}{
		{"equal cleanup deadlines", 300},
		{"controller resumes after both deadlines", 604800},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := newRetentionTestJob(tc.retention)
			job.Status = rayv1.RayJobStatus{
				JobStatus: rayv1.JobStatusSucceeded, JobDeploymentStatus: rayv1.JobDeploymentStatusComplete,
				RayClusterName: "overdue-cluster",
				EndTime:        &metav1.Time{Time: time.Now().Add(-time.Duration(tc.retention+1) * time.Second)},
			}
			cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: job.Status.RayClusterName, Namespace: job.Namespace}}
			r := newRetentionTestReconciler(t, job, cluster)
			request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}
			result, err := r.Reconcile(context.Background(), request)
			require.NoError(t, err)
			require.Zero(t, result.RequeueAfter)
			require.True(t, apierrors.IsNotFound(r.Get(context.Background(), request.NamespacedName, &rayv1.RayJob{})))
		})
	}
}

func TestRayJobRetentionSuspendResume(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, true)
	ctx := context.Background()
	job := newRetentionTestJob(604800)
	job.Spec.Suspend = true
	r := newRetentionTestReconciler(t, job)
	request := reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)}
	step := func(expected rayv1.JobDeploymentStatus) {
		t.Helper()
		_, err := r.Reconcile(ctx, request)
		require.NoError(t, err)
		require.NoError(t, r.Get(ctx, request.NamespacedName, job))
		require.Equal(t, expected, job.Status.JobDeploymentStatus)
	}
	step(rayv1.JobDeploymentStatusSuspended)
	require.Empty(t, job.Status.RayClusterName)
	job.Spec.Suspend = false
	require.NoError(t, r.Update(ctx, job))
	step(rayv1.JobDeploymentStatusNew)
	step(rayv1.JobDeploymentStatusInitializing)
	step(rayv1.JobDeploymentStatusInitializing)
	clusterKey := client.ObjectKey{Namespace: job.Namespace, Name: job.Status.RayClusterName}
	require.NoError(t, r.Get(ctx, clusterKey, &rayv1.RayCluster{}))

	job.Spec.Suspend = true
	require.NoError(t, r.Update(ctx, job))
	step(rayv1.JobDeploymentStatusSuspending)
	step(rayv1.JobDeploymentStatusSuspending) // Request cluster deletion.
	step(rayv1.JobDeploymentStatusSuspended)  // Observe completed deletion.
	require.True(t, apierrors.IsNotFound(r.Get(ctx, clusterKey, &rayv1.RayCluster{})))
	require.Nil(t, job.Status.EndTime, "suspension must not start the retention clock")

	job.Spec.Suspend = false
	require.NoError(t, r.Update(ctx, job))
	step(rayv1.JobDeploymentStatusNew)
	step(rayv1.JobDeploymentStatusInitializing)
	step(rayv1.JobDeploymentStatusInitializing)
	require.NoError(t, r.Get(ctx, client.ObjectKey{Namespace: job.Namespace, Name: job.Status.RayClusterName}, &rayv1.RayCluster{}))
}
