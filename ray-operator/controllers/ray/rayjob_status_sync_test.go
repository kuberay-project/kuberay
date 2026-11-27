package ray

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	"github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/scheme"
)

func TestUpdateRayJobStatusDetails(t *testing.T) {
	now := metav1.NewTime(time.Now().Truncate(time.Second))
	tests := []struct {
		name           string
		mutate         func(*rayv1.RayJob)
		expectedWrites int
	}{
		{name: "unchanged", mutate: func(_ *rayv1.RayJob) {}, expectedWrites: 0},
		{name: "message", mutate: func(job *rayv1.RayJob) { job.Status.Message = "waiting for workers" }, expectedWrites: 1},
		{name: "reason", mutate: func(job *rayv1.RayJob) { job.Status.Reason = rayv1.AppFailed }, expectedWrites: 1},
		{name: "start time", mutate: func(job *rayv1.RayJob) { job.Status.RayJobStatusInfo.StartTime = &now }, expectedWrites: 1},
		{name: "end time", mutate: func(job *rayv1.RayJob) { job.Status.RayJobStatusInfo.EndTime = &now }, expectedWrites: 1},
		{name: "clear start time", mutate: func(job *rayv1.RayJob) { job.Status.RayJobStatusInfo.StartTime = nil }, expectedWrites: 1},
		{name: "clear end time", mutate: func(job *rayv1.RayJob) { job.Status.RayJobStatusInfo.EndTime = nil }, expectedWrites: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"}, Status: rayv1.RayJobStatus{JobDeploymentStatus: rayv1.JobDeploymentStatusRunning, JobStatus: rayv1.JobStatusRunning}}
			if tc.name == "clear start time" {
				job.Status.RayJobStatusInfo.StartTime = &now
			}
			if tc.name == "clear end time" {
				job.Status.RayJobStatusInfo.EndTime = &now
			}
			writes := 0
			cli := clientfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(job).WithStatusSubresource(job).
				WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					writes++
					return c.SubResource(name).Update(ctx, obj, opts...)
				}}).Build()
			ctx := context.Background()
			require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(job), job))
			old := job.DeepCopy()
			tc.mutate(job)
			r := &RayJobReconciler{Client: cli}
			require.NoError(t, r.updateRayJobStatus(ctx, old, job))
			stored := &rayv1.RayJob{}
			require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(job), stored))
			assert.Equal(t, job.Status, stored.Status)
			assert.Equal(t, tc.expectedWrites, writes)
			require.NoError(t, r.updateRayJobStatus(ctx, stored.DeepCopy(), stored))
			assert.Equal(t, tc.expectedWrites, writes)
		})
	}
}

func TestUpdateRayJobStatusTimestampRoundTrip(t *testing.T) {
	for _, field := range []string{"start", "end"} {
		for _, tc := range []struct {
			name           string
			offset         time.Duration
			expectedWrites int
		}{
			{name: "same second"},
			{name: "different milliseconds", offset: 100 * time.Millisecond},
			{name: "different second", offset: time.Second, expectedWrites: 1},
		} {
			t.Run(field+"/"+tc.name, func(t *testing.T) {
				raw := metav1.NewTime(time.Now().Truncate(time.Second).Add(123 * time.Millisecond))
				job := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"}, Status: rayv1.RayJobStatus{JobDeploymentStatus: rayv1.JobDeploymentStatusRunning, JobStatus: rayv1.JobStatusRunning}}
				if field == "start" {
					job.Status.RayJobStatusInfo.StartTime = &raw
				} else {
					job.Status.RayJobStatusInfo.EndTime = &raw
				}
				// Reproduce the timestamp precision returned by the API server.
				data, err := json.Marshal(job)
				require.NoError(t, err)
				job = &rayv1.RayJob{}
				require.NoError(t, json.Unmarshal(data, job))
				writes := 0
				cli := clientfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(job).WithStatusSubresource(job).
					WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
						writes++
						return c.SubResource(name).Update(ctx, obj, opts...)
					}}).Build()
				require.NoError(t, cli.Get(t.Context(), client.ObjectKeyFromObject(job), job))
				old := job.DeepCopy()
				current := metav1.NewTime(raw.Add(tc.offset))
				if field == "start" {
					job.Status.RayJobStatusInfo.StartTime = &current
				} else {
					job.Status.RayJobStatusInfo.EndTime = &current
				}
				r := &RayJobReconciler{Client: cli}
				require.NoError(t, r.updateRayJobStatus(t.Context(), old, job))
				assert.Equal(t, tc.expectedWrites, writes)
			})
		}
	}
}

func TestUpdateRayJobStatusPreservesTerminalEndTime(t *testing.T) {
	for _, status := range []rayv1.JobDeploymentStatus{rayv1.JobDeploymentStatusComplete, rayv1.JobDeploymentStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			end := metav1.NewTime(time.Now().Add(-time.Hour).Truncate(time.Second))
			job := &rayv1.RayJob{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"}, Status: rayv1.RayJobStatus{JobDeploymentStatus: status, EndTime: &end}}
			cli := clientfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(job).WithStatusSubresource(job).Build()
			ctx := context.Background()
			require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(job), job))
			old := job.DeepCopy()
			job.Status.Message = "updated diagnostic"
			r := &RayJobReconciler{Client: cli}
			require.NoError(t, r.updateRayJobStatus(ctx, old, job))
			stored := &rayv1.RayJob{}
			require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(job), stored))
			assert.Equal(t, job.Status.Message, stored.Status.Message)
			assert.Equal(t, &end, stored.Status.EndTime)
		})
	}
}

func TestReconcilePersistsRayClusterStatusBeforeDashboard(t *testing.T) {
	tests := []struct {
		name           string
		clientError    error
		jobInfoError   error
		statusError    error
		expectedError  string
		expectedWrites int
	}{
		{name: "dashboard client unavailable", clientError: errors.New("dashboard unavailable"), expectedError: "dashboard unavailable", expectedWrites: 1},
		{name: "job info unavailable", jobInfoError: errors.New("connection refused"), expectedWrites: 2},
		{name: "job not found", jobInfoError: apierrors.NewBadRequest("job not found"), expectedWrites: 1},
		{name: "job info changes after cluster status", expectedWrites: 2},
		{name: "cluster status write fails", statusError: errors.New("status update failed"), expectedError: "status update failed", expectedWrites: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			job := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "default"},
				Spec:       rayv1.RayJobSpec{SubmissionMode: rayv1.InteractiveMode, ClusterSelector: map[string]string{utils.RayJobClusterSelectorKey: "cluster"}},
				Status:     rayv1.RayJobStatus{JobDeploymentStatus: rayv1.JobDeploymentStatusRunning, JobStatus: rayv1.JobStatusRunning, RayClusterName: "cluster", DashboardURL: "http://dashboard:8265", JobId: "job-id"},
			}
			cluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "default"}, Status: rayv1.RayClusterStatus{ReadyWorkerReplicas: 2, Conditions: []metav1.Condition{{Type: string(rayv1.HeadPodReady), Status: metav1.ConditionTrue, Reason: rayv1.HeadPodRunningAndReady}}}}
			writes := 0
			cli := clientfake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(job, cluster).WithStatusSubresource(job, cluster).
				WithInterceptorFuncs(interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, name string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					writes++
					if tc.statusError != nil {
						return tc.statusError
					}
					return c.SubResource(name).Update(ctx, obj, opts...)
				}}).Build()
			dashboard := &utils.FakeRayDashboardClient{}
			getJobInfo := func(context.Context, string) (*utiltypes.RayJobInfo, error) {
				return &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusRunning, Message: "running tasks"}, tc.jobInfoError
			}
			dashboard.GetJobInfoMock.Store(&getJobInfo)
			dashboardCalls := 0
			r := &RayJobReconciler{Client: cli, Scheme: scheme.Scheme, Recorder: events.NewFakeRecorder(10), dashboardClientFunc: func(_ *rayv1.RayCluster, _ string) (dashboardclient.RayDashboardClientInterface, error) {
				dashboardCalls++
				return dashboard, tc.clientError
			}}
			ctx := context.Background()
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)})
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(job), job))
			require.NoError(t, cli.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
			if tc.statusError != nil {
				assert.Zero(t, dashboardCalls)
			} else {
				assert.Equal(t, cluster.Status, job.Status.RayClusterStatus)
				assert.Equal(t, 1, dashboardCalls)
				assert.Equal(t, rayv1.JobDeploymentStatusRunning, job.Status.JobDeploymentStatus)
				if job.Status.Failed != nil {
					assert.Zero(t, *job.Status.Failed)
				}
				assert.Nil(t, job.Status.EndTime)
			}
			assert.Equal(t, tc.expectedWrites, writes)
			if tc.expectedError == "" {
				_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)})
				require.NoError(t, err)
				assert.Equal(t, tc.expectedWrites, writes)
			}
		})
	}
}
