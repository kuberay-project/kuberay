package ray

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

type deadlineDashboard struct {
	dashboardclient.RayDashboardClientInterface
	info      *utiltypes.RayJobInfo
	infoErr   error
	stopErr   error
	getIDs    []string
	stopIDs   []string
	submitted int
}

func (d *deadlineDashboard) GetJobInfo(_ context.Context, id string) (*utiltypes.RayJobInfo, error) {
	d.getIDs = append(d.getIDs, id)
	return d.info, d.infoErr
}

func (d *deadlineDashboard) StopJob(_ context.Context, id string) error {
	d.stopIDs = append(d.stopIDs, id)
	return d.stopErr
}

func (d *deadlineDashboard) SubmitJob(_ context.Context, job *rayv1.RayJob) (string, error) {
	d.submitted++
	return job.Status.JobId, nil
}

func newDeadlineRayJob() *rayv1.RayJob {
	return &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{Name: "deadline-job", Namespace: "default"},
		Spec: rayv1.RayJobSpec{
			Entrypoint: "sleep 600", SubmissionMode: rayv1.HTTPMode,
			ActiveDeadlineSeconds: new(int32(1)), BackoffLimit: new(int32(2)),
			RayClusterSpec: &rayv1.RayClusterSpec{HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "ray-head", Image: "rayproject/ray"}},
				}},
			}},
		},
		Status: rayv1.RayJobStatus{
			JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			JobStatus:           rayv1.JobStatusRunning, JobId: "deadline-submission-id",
			RayClusterName: "deadline-cluster", DashboardURL: "dashboard.invalid:8265",
			StartTime: &metav1.Time{Time: time.Now().Add(-time.Minute).UTC().Truncate(time.Second)},
		},
	}
}

type deadlineFixture struct {
	client        client.Client
	scheme        *runtime.Scheme
	key           types.NamespacedName
	dashboard     *deadlineDashboard
	dashboardErr  error
	dashboardInit int
}

func newDeadlineFixture(t *testing.T, job *rayv1.RayJob) *deadlineFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: job.Status.RayClusterName, Namespace: job.Namespace},
		Status:     rayv1.RayClusterStatus{State: rayv1.Ready},
	}
	if job.Spec.RayClusterSpec != nil {
		cluster.Spec = *job.Spec.RayClusterSpec.DeepCopy()
	}
	return &deadlineFixture{
		client: clientFake.NewClientBuilder().WithScheme(scheme).
			WithStatusSubresource(job, cluster).WithObjects(job, cluster).Build(),
		scheme: scheme, key: client.ObjectKeyFromObject(job),
		dashboard: &deadlineDashboard{info: &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusRunning}},
	}
}

func (f *deadlineFixture) reconcile(t *testing.T) (reconcile.Result, *rayv1.RayJob, error) {
	t.Helper()
	// A fresh reconciler on every pass proves cancellation does not depend on controller memory.
	r := &RayJobReconciler{
		Client: f.client, Scheme: f.scheme, Recorder: events.NewFakeRecorder(10),
		dashboardClientFunc: func(*rayv1.RayCluster, string) (dashboardclient.RayDashboardClientInterface, error) {
			f.dashboardInit++
			return f.dashboard, f.dashboardErr
		},
	}
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: f.key})
	job := &rayv1.RayJob{}
	require.NoError(t, f.client.Get(context.Background(), f.key, job))
	return result, job, err
}

func assertDeadlineFailureUnchanged(t *testing.T, before, after *rayv1.RayJob) {
	t.Helper()
	require.Equal(t, rayv1.JobDeploymentStatusFailed, after.Status.JobDeploymentStatus)
	require.Equal(t, rayv1.DeadlineExceeded, after.Status.Reason)
	require.Equal(t, before.Status.Message, after.Status.Message)
	require.Equal(t, before.Status.Failed, after.Status.Failed)
	require.Equal(t, before.Status.Succeeded, after.Status.Succeeded)
	require.Equal(t, before.Status.EndTime, after.Status.EndTime)
	require.Equal(t, before.Status.JobId, after.Status.JobId)
	require.Equal(t, before.Status.RayClusterName, after.Status.RayClusterName)
}

func TestRayJobDeadlineCancellation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		shared   bool
		terminal rayv1.JobStatus
	}{
		{name: "owned_stopped", terminal: rayv1.JobStatusStopped},
		{name: "shared_stopped", shared: true, terminal: rayv1.JobStatusStopped},
		{name: "already_succeeded", terminal: rayv1.JobStatusSucceeded},
		{name: "already_failed", terminal: rayv1.JobStatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := newDeadlineRayJob()
			if tc.shared {
				job.Spec.RayClusterSpec = nil
				job.Spec.ClusterSelector = map[string]string{utils.RayJobClusterSelectorKey: job.Status.RayClusterName}
				job.Spec.BackoffLimit = new(int32(0))
				job.Spec.ShutdownAfterJobFinishes = true
				job.Status.JobStatus = rayv1.JobStatusPending
			}
			f := newDeadlineFixture(t, job)
			result, expired, err := f.reconcile(t)
			require.NoError(t, err)
			require.Equal(t, RayJobDefaultRequeueDuration, result.RequeueAfter)
			require.Equal(t, rayv1.JobDeploymentStatusFailed, expired.Status.JobDeploymentStatus)
			require.Equal(t, rayv1.DeadlineExceeded, expired.Status.Reason)
			require.EqualValues(t, 1, *expired.Status.Failed)
			require.EqualValues(t, 0, *expired.Status.Succeeded)
			require.NotNil(t, expired.Status.EndTime)
			// API timestamps have second precision: simulate a later reconciliation without sleeping.
			expired.Status.EndTime = &metav1.Time{Time: time.Now().Add(-time.Minute).UTC().Truncate(time.Second)}
			require.NoError(t, f.client.Status().Update(context.Background(), expired))

			// An accepted stop is not proof of termination: keep polling both active Ray states.
			var activeStates []rayv1.JobStatus
			if tc.terminal == rayv1.JobStatusStopped {
				activeStates = []rayv1.JobStatus{rayv1.JobStatusRunning, rayv1.JobStatusRunning, rayv1.JobStatusPending}
			}
			for i, status := range activeStates {
				f.dashboard.info = &utiltypes.RayJobInfo{JobStatus: status}
				result, observed, err := f.reconcile(t)
				require.NoError(t, err)
				require.Equal(t, RayJobDefaultRequeueDuration, result.RequeueAfter)
				assertDeadlineFailureUnchanged(t, expired, observed)
				require.False(t, rayv1.IsJobTerminal(observed.Status.JobStatus))
				require.Len(t, f.dashboard.getIDs, i+1)
				require.Len(t, f.dashboard.stopIDs, i+1)
				require.Equal(t, job.Status.JobId, f.dashboard.getIDs[i])
				require.Equal(t, job.Status.JobId, f.dashboard.stopIDs[i])
			}

			runtimeStart := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
			runtimeEnd := runtimeStart.Add(30 * time.Second)
			f.dashboard.info = &utiltypes.RayJobInfo{
				JobStatus: tc.terminal, Message: "Ray terminal result",
				StartTime: uint64(runtimeStart.UnixMilli()), EndTime: uint64(runtimeEnd.UnixMilli()),
			}
			_, terminal, err := f.reconcile(t)
			require.NoError(t, err)
			require.Equal(t, tc.terminal, terminal.Status.JobStatus)
			require.NotNil(t, terminal.Status.RayJobStatusInfo.StartTime)
			require.NotNil(t, terminal.Status.RayJobStatusInfo.EndTime)
			require.True(t, runtimeStart.Equal(terminal.Status.RayJobStatusInfo.StartTime.Time))
			require.True(t, runtimeEnd.Equal(terminal.Status.RayJobStatusInfo.EndTime.Time))
			assertDeadlineFailureUnchanged(t, expired, terminal)
			require.Len(t, f.dashboard.getIDs, len(activeStates)+1)
			require.Len(t, f.dashboard.stopIDs, len(activeStates), "do not stop a job already observed as terminal")
			result, stable, err := f.reconcile(t)
			require.NoError(t, err)
			require.Zero(t, result.RequeueAfter)
			require.Equal(t, terminal.Status, stable.Status)
			require.Len(t, f.dashboard.getIDs, len(activeStates)+1, "terminal observation ends cancellation polling")
			require.Zero(t, f.dashboard.submitted, "deadline failures must never resubmit, even with backoff remaining")
			require.NoError(t, f.client.Get(context.Background(), types.NamespacedName{
				Name: job.Status.RayClusterName, Namespace: job.Namespace,
			}, &rayv1.RayCluster{}), "retained owned and shared clusters must survive")
		})
	}
}

func TestRayJobDeadlineCancellationUncertainStatus(t *testing.T) {
	for _, tc := range []struct {
		name         string
		info         *utiltypes.RayJobInfo
		infoErr      error
		stopErr      error
		dashboardErr error
		wantGets     int
		wantStops    int
	}{
		{name: "dashboard_client_error", dashboardErr: errors.New("dashboard unavailable")},
		{name: "get_error", infoErr: errors.New("connection reset"), wantGets: 1},
		{name: "nil_info", wantGets: 1},
		{name: "empty_status", info: &utiltypes.RayJobInfo{}, wantGets: 1},
		{name: "unknown_status", info: &utiltypes.RayJobInfo{JobStatus: "UNKNOWN"}, wantGets: 1},
		{name: "stop_error", info: &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusRunning}, stopErr: errors.New("stop unavailable"), wantGets: 1, wantStops: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := newDeadlineRayJob()
			f := newDeadlineFixture(t, job)
			_, expired, err := f.reconcile(t)
			require.NoError(t, err)
			f.dashboard.info, f.dashboard.infoErr, f.dashboard.stopErr = tc.info, tc.infoErr, tc.stopErr
			f.dashboardErr = tc.dashboardErr
			result, observed, err := f.reconcile(t)
			require.NoError(t, err)
			require.Equal(t, RayJobDefaultRequeueDuration, result.RequeueAfter, "uncertain termination must be retried")
			assertDeadlineFailureUnchanged(t, expired, observed)
			require.Equal(t, expired.Status.JobStatus, observed.Status.JobStatus)
			require.Equal(t, 1, f.dashboardInit)
			require.Len(t, f.dashboard.getIDs, tc.wantGets)
			require.Len(t, f.dashboard.stopIDs, tc.wantStops)
			require.Zero(t, f.dashboard.submitted)

			// Recover on the next pass without resetting the deployment's failed status.
			f.dashboardErr, f.dashboard.infoErr, f.dashboard.stopErr = nil, nil, nil
			f.dashboard.info = &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusStopped}
			_, observed, err = f.reconcile(t)
			require.NoError(t, err)
			require.Equal(t, rayv1.JobStatusStopped, observed.Status.JobStatus)
			assertDeadlineFailureUnchanged(t, expired, observed)
			require.Zero(t, f.dashboard.submitted)
		})
	}
}

func TestRayJobDeadlineCancellationMissingRecord(t *testing.T) {
	t.Setenv(utils.DELETE_RAYJOB_CR_AFTER_JOB_FINISHES, "false")
	for _, tc := range []struct {
		name     string
		shared   bool
		shutdown bool
		ttl      int32
	}{
		{name: "retained_owned"},
		{name: "shared", shared: true, shutdown: true},
		{name: "owned_cleanup_due", shutdown: true},
		{name: "owned_long_ttl", shutdown: true, ttl: 3600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := newDeadlineRayJob()
			job.Spec.ShutdownAfterJobFinishes, job.Spec.TTLSecondsAfterFinished = tc.shutdown, tc.ttl
			if tc.shared {
				job.Spec.RayClusterSpec = nil
				job.Spec.ClusterSelector = map[string]string{utils.RayJobClusterSelectorKey: job.Status.RayClusterName}
				job.Spec.BackoffLimit = new(int32(0))
			}
			f := newDeadlineFixture(t, job)
			_, expired, err := f.reconcile(t)
			require.NoError(t, err)
			var dashboardStatus atomic.Int32
			dashboardStatus.Store(http.StatusNotFound)
			var gets, stops, unexpected atomic.Int32
			jobPath := dashboardclient.JobPath + job.Status.JobId
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodGet && r.URL.Path == jobPath:
					gets.Add(1)
					if status := int(dashboardStatus.Load()); status != http.StatusOK {
						http.Error(w, "dashboard request failed", status)
						return
					}
					_, _ = io.WriteString(w, `{"status":"RUNNING"}`)
				case r.Method == http.MethodPost && r.URL.Path == jobPath+"/stop":
					stops.Add(1)
					_, _ = io.WriteString(w, `{"stopped":true}`)
				default:
					unexpected.Add(1)
					http.Error(w, "unexpected dashboard request", http.StatusInternalServerError)
				}
			}))
			t.Cleanup(server.Close)
			dashboard := &dashboardclient.RayDashboardClient{}
			dashboard.InitClient(server.Client(), server.URL, "")
			reconcileHTTP := func() reconcile.Result {
				r := &RayJobReconciler{
					Client: f.client, Scheme: f.scheme, Recorder: events.NewFakeRecorder(10),
					dashboardClientFunc: func(*rayv1.RayCluster, string) (dashboardclient.RayDashboardClientInterface, error) {
						return dashboard, nil
					},
				}
				result, reconcileErr := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: f.key})
				require.NoError(t, reconcileErr)
				return result
			}
			observed := &rayv1.RayJob{}
			if !tc.shutdown {
				// HTTP 400 is not the dashboard client's special not-found error and must still retry.
				dashboardStatus.Store(http.StatusBadRequest)
				require.Equal(t, RayJobDefaultRequeueDuration, reconcileHTTP().RequeueAfter)
				require.NoError(t, f.client.Get(context.Background(), f.key, observed))
				require.Equal(t, expired.Status, observed.Status)
				require.EqualValues(t, 1, gets.Load())
				require.Zero(t, stops.Load())
				require.Zero(t, unexpected.Load())
				dashboardStatus.Store(http.StatusNotFound)
				gets.Store(0)
			}
			result := reconcileHTTP()
			require.NoError(t, f.client.Get(context.Background(), f.key, observed))
			require.Equal(t, expired.Status, observed.Status, "absence must not invent a Ray outcome or change accounting")
			require.Equal(t, expired.ResourceVersion, observed.ResourceVersion)
			require.EqualValues(t, 1, gets.Load())
			require.Zero(t, stops.Load())
			require.Zero(t, unexpected.Load(), "missing records must not be resubmitted")
			if tc.ttl > 0 {
				require.Greater(t, result.RequeueAfter, 50*time.Minute, "only the cleanup TTL should schedule reconciliation")
				require.LessOrEqual(t, result.RequeueAfter, time.Hour+2*time.Second)
			} else {
				require.Zero(t, result.RequeueAfter, "a missing Ray record must not self-poll indefinitely")
			}
			clusterErr := f.client.Get(context.Background(), types.NamespacedName{
				Name: job.Status.RayClusterName, Namespace: job.Namespace,
			}, &rayv1.RayCluster{})
			if tc.shutdown && !tc.shared && tc.ttl == 0 {
				require.True(t, apierrors.IsNotFound(clusterErr), "missing Ray records must not block due cleanup")
				return
			}
			require.NoError(t, clusterErr)

			// Absence is not a durable cancellation marker: an external event can discover the job again.
			dashboardStatus.Store(http.StatusOK)
			result = reconcileHTTP()
			require.Equal(t, RayJobDefaultRequeueDuration, result.RequeueAfter)
			require.EqualValues(t, 2, gets.Load())
			require.EqualValues(t, 1, stops.Load())
			require.Zero(t, unexpected.Load())
			require.NoError(t, f.client.Get(context.Background(), f.key, observed))
			require.Equal(t, expired.Status, observed.Status)
		})
	}
}

func TestRayJobDeadlineCancellationStatusConflict(t *testing.T) {
	job := newDeadlineRayJob()
	f := newDeadlineFixture(t, job)
	_, expired, err := f.reconcile(t)
	require.NoError(t, err)
	_, _, err = f.reconcile(t)
	require.NoError(t, err)
	require.Equal(t, []string{job.Status.JobId}, f.dashboard.stopIDs)
	f.dashboard.info = &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusStopped}
	fakeClient, ok := f.client.(client.WithWatch)
	require.True(t, ok)
	statusWrites := 0
	f.client = interceptor.NewClient(fakeClient, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subresource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if subresource == "status" {
				statusWrites++
				if statusWrites == 1 {
					return apierrors.NewConflict(schema.GroupResource{Group: rayv1.GroupVersion.Group, Resource: "rayjobs"}, obj.GetName(), errors.New("concurrent update"))
				}
			}
			return c.SubResource(subresource).Update(ctx, obj, opts...)
		},
	})
	result, conflicted, err := f.reconcile(t)
	require.True(t, apierrors.IsConflict(err))
	require.Equal(t, RayJobDefaultRequeueDuration, result.RequeueAfter)
	require.Equal(t, rayv1.JobStatusRunning, conflicted.Status.JobStatus)
	require.EqualValues(t, 1, *conflicted.Status.Failed)
	assertDeadlineFailureUnchanged(t, expired, conflicted)

	_, terminal, err := f.reconcile(t)
	require.NoError(t, err)
	require.Equal(t, rayv1.JobStatusStopped, terminal.Status.JobStatus)
	assertDeadlineFailureUnchanged(t, expired, terminal)
	require.Equal(t, 2, statusWrites)
	require.Len(t, f.dashboard.getIDs, 3)
	require.Equal(t, []string{job.Status.JobId}, f.dashboard.stopIDs)
	require.Zero(t, f.dashboard.submitted)
}

func TestRayJobDeadlineCancellationMissingCluster(t *testing.T) {
	job := newDeadlineRayJob()
	f := newDeadlineFixture(t, job)
	_, expired, err := f.reconcile(t)
	require.NoError(t, err)
	clusterKey := types.NamespacedName{Name: job.Status.RayClusterName, Namespace: job.Namespace}
	require.NoError(t, f.client.Delete(context.Background(), &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{
		Name: clusterKey.Name, Namespace: clusterKey.Namespace,
	}}))
	_, observed, err := f.reconcile(t)
	require.NoError(t, err)
	assertDeadlineFailureUnchanged(t, expired, observed)
	require.Equal(t, expired.Status.JobStatus, observed.Status.JobStatus, "cluster absence is not a Ray terminal observation")
	require.True(t, apierrors.IsNotFound(f.client.Get(context.Background(), clusterKey, &rayv1.RayCluster{})))
	require.Zero(t, f.dashboardInit, "a missing cluster must not be recreated or contacted")
	require.Zero(t, f.dashboard.submitted)
}

func TestRayJobDeadlineCancellationDoesNotBlockCleanup(t *testing.T) {
	t.Setenv(utils.DELETE_RAYJOB_CR_AFTER_JOB_FINISHES, "false")
	for _, tc := range []struct {
		name          string
		ttl           int32
		deletionRules bool
		dashboardErr  error
		infoErr       error
		stopErr       error
	}{
		{name: "client_error", dashboardErr: errors.New("dashboard unavailable")},
		{name: "get_error", infoErr: errors.New("dashboard unavailable")},
		{name: "stop_error", stopErr: errors.New("stop unavailable")},
		{name: "long_ttl", ttl: 3600},
		{name: "rules_long_ttl", ttl: 3600, deletionRules: true},
		{name: "rules_dashboard_error", deletionRules: true, dashboardErr: errors.New("dashboard unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, tc.deletionRules)
			job := newDeadlineRayJob()
			job.Spec.ShutdownAfterJobFinishes = true
			job.Spec.TTLSecondsAfterFinished = tc.ttl
			if tc.deletionRules {
				job.Spec.ShutdownAfterJobFinishes = false
				job.Spec.TTLSecondsAfterFinished = 0
				job.Spec.DeletionStrategy = &rayv1.DeletionStrategy{DeletionRules: []rayv1.DeletionRule{{
					Policy: rayv1.DeleteCluster,
					Condition: rayv1.DeletionCondition{
						JobDeploymentStatus: new(rayv1.JobDeploymentStatusFailed), TTLSeconds: tc.ttl,
					},
				}}}
			}
			f := newDeadlineFixture(t, job)
			_, expired, err := f.reconcile(t)
			require.NoError(t, err)
			expired.Status.EndTime = &metav1.Time{Time: time.Now().Add(-time.Minute).UTC().Truncate(time.Second)}
			require.NoError(t, f.client.Status().Update(context.Background(), expired))
			f.dashboardErr, f.dashboard.infoErr, f.dashboard.stopErr = tc.dashboardErr, tc.infoErr, tc.stopErr
			result, observed, err := f.reconcile(t)
			require.NoError(t, err)
			assertDeadlineFailureUnchanged(t, expired, observed)
			require.Equal(t, expired.Status.JobStatus, observed.Status.JobStatus)
			require.Equal(t, 1, f.dashboardInit)
			clusterErr := f.client.Get(context.Background(), types.NamespacedName{
				Name: job.Status.RayClusterName, Namespace: job.Namespace,
			}, &rayv1.RayCluster{})
			if tc.ttl == 0 {
				require.True(t, apierrors.IsNotFound(clusterErr), "dashboard errors must not prevent due cleanup")
			} else {
				require.NoError(t, clusterErr)
				require.Equal(t, RayJobDefaultRequeueDuration, result.RequeueAfter, "cleanup TTL must not delay cancellation polling")
				require.Equal(t, []string{job.Status.JobId}, f.dashboard.stopIDs)
			}
			require.Zero(t, f.dashboard.submitted)
		})
	}
}

func TestRayJobDeadlineCancellationScope(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reason rayv1.JobFailedReason
		status rayv1.JobStatus
	}{
		{name: "other_failure", reason: rayv1.AppFailed, status: rayv1.JobStatusRunning},
		{name: "never_observed", reason: rayv1.DeadlineExceeded, status: rayv1.JobStatusNew},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := newDeadlineRayJob()
			job.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusFailed
			job.Status.Reason, job.Status.JobStatus = tc.reason, tc.status
			job.Status.EndTime = &metav1.Time{Time: time.Now().Add(-time.Minute).UTC().Truncate(time.Second)}
			job.Status.Failed, job.Status.Succeeded = new(int32(1)), new(int32(0))
			f := newDeadlineFixture(t, job)
			before := &rayv1.RayJob{}
			require.NoError(t, f.client.Get(context.Background(), f.key, before))
			result, observed, err := f.reconcile(t)
			require.NoError(t, err)
			require.Zero(t, result.RequeueAfter)
			require.Equal(t, before.Status, observed.Status)
			require.Zero(t, f.dashboardInit)
		})
	}
}
