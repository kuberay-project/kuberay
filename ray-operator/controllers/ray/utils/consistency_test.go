package utils

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestInconsistentRayClusterStatus(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)

	// Mock data
	timeNow := metav1.Now()
	oldStatus := rayv1.RayClusterStatus{
		State:                   rayv1.Ready,
		ReadyWorkerReplicas:     1,
		AvailableWorkerReplicas: 1,
		DesiredWorkerReplicas:   1,
		MinWorkerReplicas:       1,
		MaxWorkerReplicas:       10,
		LastUpdateTime:          &timeNow,
		Endpoints: map[string]string{
			ClientPortName:    strconv.Itoa(DefaultClientPort),
			DashboardPortName: strconv.Itoa(DefaultDashboardPort),
			GcsServerPortName: strconv.Itoa(DefaultGcsServerPort),
			MetricsPortName:   strconv.Itoa(DefaultMetricsPort),
		},
		Head: rayv1.HeadInfo{
			PodIP:     "10.244.0.6",
			ServiceIP: "10.96.140.249",
		},
		ObservedGeneration: 1,
		Reason:             "test reason",
	}

	// `inconsistentRayClusterStatus` is used to check whether the old and new RayClusterStatus are inconsistent
	// by comparing different fields. If the only differences between the old and new status are the `LastUpdateTime`
	// and `ObservedGeneration` fields, the status update will not be triggered.
	testCases := []struct {
		modifyStatus func(*rayv1.RayClusterStatus)
		name         string
		expectResult bool
	}{
		{
			name: "State is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.State = rayv1.Suspended
			},
			expectResult: true,
		},
		{
			name: "Reason is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.Reason = "new reason"
			},
			expectResult: true,
		},
		{
			name: "ReadyWorkerReplicas is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.ReadyWorkerReplicas = oldStatus.ReadyWorkerReplicas + 1
			},
			expectResult: true,
		},
		{
			name: "AvailableWorkerReplicas is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.AvailableWorkerReplicas = oldStatus.AvailableWorkerReplicas + 1
			},
			expectResult: true,
		},
		{
			name: "DesiredWorkerReplicas is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.DesiredWorkerReplicas = oldStatus.DesiredWorkerReplicas + 1
			},
			expectResult: true,
		},
		{
			name: "MinWorkerReplicas is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.MinWorkerReplicas = oldStatus.MinWorkerReplicas + 1
			},
			expectResult: true,
		},
		{
			name: "MaxWorkerReplicas is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.MaxWorkerReplicas = oldStatus.MaxWorkerReplicas + 1
			},
			expectResult: true,
		},
		{
			name: "Endpoints is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.Endpoints["fakeEndpoint"] = "10009"
			},
			expectResult: true,
		},
		{
			name: "Head.PodIP is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.Head.PodIP = "test head pod ip"
			},
			expectResult: true,
		},
		{
			name: "RayClusterReplicaFailure is updated, expect result to be true",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				meta.SetStatusCondition(&newStatus.Conditions, metav1.Condition{Type: string(rayv1.RayClusterReplicaFailure), Status: metav1.ConditionTrue})
			},
			expectResult: true,
		},
		{
			name: "LastUpdateTime is updated, expect result to be false",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.LastUpdateTime = &metav1.Time{Time: timeNow.Add(time.Hour)}
			},
			expectResult: false,
		},
		{
			name: "ObservedGeneration is updated, expect result to be false",
			modifyStatus: func(newStatus *rayv1.RayClusterStatus) {
				newStatus.ObservedGeneration = oldStatus.ObservedGeneration + 1
			},
			expectResult: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			newStatus := oldStatus.DeepCopy()
			testCase.modifyStatus(newStatus)
			result := InconsistentRayClusterStatus(oldStatus, *newStatus)
			assert.Equal(t, testCase.expectResult, result)
		})
	}
}

func TestInconsistentRayServiceStatuses(t *testing.T) {
	oldStatus := rayv1.RayServiceStatuses{
		ActiveServiceStatus: rayv1.RayServiceStatus{
			RayClusterName: "new-cluster",
			Applications: map[string]rayv1.AppStatus{
				DefaultServeAppName: {
					Status:  rayv1.ApplicationStatusEnum.RUNNING,
					Message: "OK",
					Deployments: map[string]rayv1.ServeDeploymentStatus{
						"serve-1": {
							Status:  rayv1.DeploymentStatusEnum.UNHEALTHY,
							Message: "error",
						},
					},
				},
			},
		},
		PendingServiceStatus: rayv1.RayServiceStatus{
			RayClusterName: "old-cluster",
			Applications: map[string]rayv1.AppStatus{
				DefaultServeAppName: {
					Status:  rayv1.ApplicationStatusEnum.NOT_STARTED,
					Message: "application not started yet",
					Deployments: map[string]rayv1.ServeDeploymentStatus{
						"serve-1": {
							Status:  rayv1.DeploymentStatusEnum.HEALTHY,
							Message: "Serve is healthy",
						},
					},
				},
			},
		},
		ServiceStatus: rayv1.NotRunning,
	}

	// Test 1: Update ServiceStatus only.
	newStatus := oldStatus.DeepCopy()
	newStatus.ServiceStatus = rayv1.Running
	assert.True(t, InconsistentRayServiceStatuses(oldStatus, *newStatus))

	// Test 2: Test RayServiceStatus
	newStatus = oldStatus.DeepCopy()
	assert.False(t, InconsistentRayServiceStatuses(oldStatus, *newStatus))
}
