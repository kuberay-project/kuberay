package metrics

import (
	"context"
	"net/http"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestMetricRayJobInfo(t *testing.T) {
	tests := []struct {
		name            string
		rayJobs         []rayv1.RayJob
		expectedMetrics []string
	}{
		{
			name: "two jobs and delete one later",
			rayJobs: []rayv1.RayJob{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-job-1",
						Namespace: "ns1",
						UID:       types.UID("ray-job-1-uid"),
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-job-2",
						Namespace: "ns2",
						UID:       types.UID("ray-job-2-uid"),
					},
				},
			},
			expectedMetrics: []string{
				`kuberay_job_info{name="ray-job-1",namespace="ns1",uid="ray-job-1-uid"} 1`,
				`kuberay_job_info{name="ray-job-2",namespace="ns2",uid="ray-job-2-uid"} 1`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sScheme := runtime.NewScheme()
			require.NoError(t, rayv1.AddToScheme(k8sScheme))

			objs := make([]client.Object, len(tc.rayJobs))
			for i := range tc.rayJobs {
				objs[i] = &tc.rayJobs[i]
			}
			client := fake.NewClientBuilder().WithScheme(k8sScheme).WithObjects(objs...).Build()
			manager := NewRayJobMetricsManager(context.Background(), client)
			reg := prometheus.NewRegistry()
			reg.MustRegister(manager)

			body, statusCode := support.GetMetricsResponseAndCode(t, reg)

			assert.Equal(t, http.StatusOK, statusCode)
			for _, label := range tc.expectedMetrics {
				assert.Contains(t, body, label)
			}

			if len(tc.rayJobs) > 0 {
				err := client.Delete(t.Context(), &tc.rayJobs[0])
				require.NoError(t, err)
			}

			body2, statusCode := support.GetMetricsResponseAndCode(t, reg)

			assert.Equal(t, http.StatusOK, statusCode)
			assert.NotContains(t, body2, tc.expectedMetrics[0])
			for _, label := range tc.expectedMetrics[1:] {
				assert.Contains(t, body2, label)
			}
		})
	}
}

func TestDeleteRayJobMetrics(t *testing.T) {
	k8sScheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(k8sScheme))
	client := fake.NewClientBuilder().WithScheme(k8sScheme).Build()
	manager := NewRayJobMetricsManager(context.Background(), client)
	reg := prometheus.NewRegistry()
	reg.MustRegister(manager)

	// Test case 1: Delete specific job metrics
	// Manually add some metrics
	manager.ObserveRayJobExecutionDuration("job1", "ns1", "uid1", rayv1.JobDeploymentStatusComplete, 0, 10.5)
	manager.ObserveRayJobExecutionDuration("job2", "ns2", "uid2", rayv1.JobDeploymentStatusFailed, 1, 20.3)
	manager.ObserveRayJobExecutionDuration("job3", "ns1", "uid3", rayv1.JobDeploymentStatusRunning, 0, 5.7)

	// Test deleting metrics for job1 in ns1
	manager.DeleteRayJobMetrics("job1", "ns1")

	// Verify metrics
	body, statusCode := support.GetMetricsResponseAndCode(t, reg)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotContains(t, body, `kuberay_job_execution_duration_seconds{job_deployment_status="Complete",name="job1",namespace="ns1",retry_count="0",uid="uid1"}`)
	assert.Contains(t, body, `kuberay_job_execution_duration_seconds{job_deployment_status="Failed",name="job2",namespace="ns2",retry_count="1",uid="uid2"}`)
	assert.Contains(t, body, `kuberay_job_execution_duration_seconds{job_deployment_status="Running",name="job3",namespace="ns1",retry_count="0",uid="uid3"}`)

	// Test case 2: Delete with empty name
	manager.DeleteRayJobMetrics("", "ns1")

	// Verify metrics again
	body2, statusCode := support.GetMetricsResponseAndCode(t, reg)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotContains(t, body2, `kuberay_job_execution_duration_seconds{job_deployment_status="Complete",name="job1",namespace="ns1",retry_count="0",uid="uid1"}`)
	assert.Contains(t, body2, `kuberay_job_execution_duration_seconds{job_deployment_status="Failed",name="job2",namespace="ns2",retry_count="1",uid="uid2"}`)
	assert.Contains(t, body2, `kuberay_job_execution_duration_seconds{job_deployment_status="Running",name="job3",namespace="ns1",retry_count="0",uid="uid3"}`)

	// Test case 3: Delete with empty name and namespace
	manager.DeleteRayJobMetrics("", "")

	// Verify no metrics were deleted
	body3, statusCode := support.GetMetricsResponseAndCode(t, reg)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotContains(t, body3, `kuberay_job_execution_duration_seconds{job_deployment_status="Complete",name="job1",namespace="ns1",retry_count="0",uid="uid1"}`)
	assert.Contains(t, body3, `kuberay_job_execution_duration_seconds{job_deployment_status="Failed",name="job2",namespace="ns2",retry_count="1",uid="uid2"}`)
	assert.Contains(t, body3, `kuberay_job_execution_duration_seconds{job_deployment_status="Running",name="job3",namespace="ns1",retry_count="0",uid="uid3"}`)

	// Test case 4: Delete with false name and namespace
	manager.DeleteRayJobMetrics("ns2", "job2")

	// Verify no metrics were deleted
	body4, statusCode := support.GetMetricsResponseAndCode(t, reg)

	assert.Equal(t, http.StatusOK, statusCode)
	assert.NotContains(t, body4, `kuberay_job_execution_duration_seconds{job_deployment_status="Complete",name="job1",namespace="ns1",retry_count="0",uid="uid1"}`)
	assert.Contains(t, body4, `kuberay_job_execution_duration_seconds{job_deployment_status="Failed",name="job2",namespace="ns2",retry_count="1",uid="uid2"}`)
	assert.Contains(t, body4, `kuberay_job_execution_duration_seconds{job_deployment_status="Running",name="job3",namespace="ns1",retry_count="0",uid="uid3"}`)
}

func TestMetricRayJobDeploymentStatus(t *testing.T) {
	tests := []struct {
		name            string
		rayJobs         []rayv1.RayJob
		expectedMetrics []string
	}{
		{
			name: "two jobs with different deployment statuses",
			rayJobs: []rayv1.RayJob{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-job-1",
						Namespace: "ns1",
						UID:       types.UID("ray-job-1-uid"),
					},
					Status: rayv1.RayJobStatus{
						JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "ray-job-2",
						Namespace: "ns2",
						UID:       types.UID("ray-job-2-uid"),
					},
					Status: rayv1.RayJobStatus{
						JobDeploymentStatus: rayv1.JobDeploymentStatusFailed,
					},
				},
			},
			expectedMetrics: []string{
				`kuberay_job_deployment_status{deployment_status="Running",name="ray-job-1",namespace="ns1",uid="ray-job-1-uid"} 1`,
				`kuberay_job_deployment_status{deployment_status="Failed",name="ray-job-2",namespace="ns2",uid="ray-job-2-uid"} 1`,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			k8sScheme := runtime.NewScheme()
			require.NoError(t, rayv1.AddToScheme(k8sScheme))

			objs := make([]client.Object, len(tc.rayJobs))
			for i := range tc.rayJobs {
				objs[i] = &tc.rayJobs[i]
			}
			client := fake.NewClientBuilder().WithScheme(k8sScheme).WithObjects(objs...).Build()
			manager := NewRayJobMetricsManager(context.Background(), client)
			reg := prometheus.NewRegistry()
			reg.MustRegister(manager)

			body, statusCode := support.GetMetricsResponseAndCode(t, reg)

			assert.Equal(t, http.StatusOK, statusCode)
			for _, label := range tc.expectedMetrics {
				assert.Contains(t, body, label)
			}

			if len(tc.rayJobs) > 0 {
				err := client.Delete(context.Background(), &tc.rayJobs[0])
				require.NoError(t, err)
			}

			body2, statusCode := support.GetMetricsResponseAndCode(t, reg)

			assert.Equal(t, http.StatusOK, statusCode)

			assert.NotContains(t, body2, tc.expectedMetrics[0])
			for _, label := range tc.expectedMetrics[1:] {
				assert.Contains(t, body2, label)
			}
		})
	}
}
