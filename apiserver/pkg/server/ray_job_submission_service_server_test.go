package server

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"

	"github.com/ray-project/kuberay/apiserver/pkg/client"
	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	fakeclientset "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/fake"
)

func TestGetRayClusterURL(t *testing.T) {
	ctx := context.Background()

	const namespace = "test-namespace"
	const clusterName = "test-raycluster"

	validCluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName,
			Namespace: namespace,
			Labels: map[string]string{
				util.KubernetesManagedByLabelKey: util.ComponentName,
			},
		},
		Status: rayv1.RayClusterStatus{
			State: rayv1.Ready,
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "test",
								Image: "test",
							},
						},
					},
				},
			},
		},
	}

	tests := []struct {
		rayCluster          *rayv1.RayCluster
		rayEvent            *eventsv1.Event
		name                string
		expectedURL         string
		expectedErrorString string
	}{
		{
			name:                "Get URL from a valid cluster",
			rayCluster:          &validCluster,
			expectedURL:         clusterName + "-head-svc." + namespace + ".svc.cluster.local:8265",
			expectedErrorString: "",
		},
		{
			name: "Get URL from a cluster with missing name",
			rayCluster: func() *rayv1.RayCluster {
				newCluster := validCluster.DeepCopy()
				newCluster.ObjectMeta.Name = ""
				return newCluster
			}(),
			expectedURL:         "",
			expectedErrorString: "Cluster name is empty. Please specify a valid value.",
		},
		{
			name: "Get URL from a cluster with missing namespace",
			rayCluster: func() *rayv1.RayCluster {
				newCluster := validCluster.DeepCopy()
				newCluster.ObjectMeta.Namespace = ""
				return newCluster
			}(),
			expectedURL:         "",
			expectedErrorString: "Namespace is empty. Please specify a valid value.",
		},
		{
			name: "Get URL from a cluster without ready state",
			rayCluster: func() *rayv1.RayCluster {
				newCluster := validCluster.DeepCopy()
				newCluster.Status.State = rayv1.Suspended
				return newCluster
			}(),
			expectedURL:         "",
			expectedErrorString: "cluster is not ready",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			getClusterRequest := &api.GetClusterRequest{
				Name:      tc.rayCluster.Name,
				Namespace: tc.rayCluster.Namespace,
			}

			expectedEvent := &eventsv1.Event{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "ray-event-1",
					Namespace: tc.rayCluster.Namespace,
				},
			}

			// mock controller
			ctrl := gomock.NewController(t)

			mockClientManager := manager.NewMockClientManagerInterface(ctrl)

			// Mock ray cluster client
			mockClusterClient := client.NewMockClusterClientInterface(ctrl)
			// create fake ray cluster
			fakeClient := fakeclientset.NewSimpleClientset(tc.rayCluster)
			fakeRayCluster := fakeClient.RayV1().RayClusters(tc.rayCluster.Namespace)
			mockClusterClient.EXPECT().RayClusterClient(tc.rayCluster.Namespace).Return(fakeRayCluster).MinTimes(1).MaxTimes(2)
			mockClientManager.EXPECT().ClusterClient().Return(mockClusterClient).MinTimes(1).MaxTimes(2)

			// Mock events client
			mockKubeClient := client.NewMockKubernetesClientInterface(ctrl)
			// create fake events
			fakeClientset := kubernetesfake.NewClientset(expectedEvent)
			fakeEvents := fakeClientset.EventsV1().Events(tc.rayCluster.Namespace)
			mockKubeClient.EXPECT().EventsClient(tc.rayCluster.Namespace).Return(fakeEvents).MaxTimes(1)
			mockClientManager.EXPECT().KubernetesClient().Return(mockKubeClient).MaxTimes(1)

			resourceManager := manager.NewResourceManager(mockClientManager)

			rayJobSubmissionService := NewRayJobSubmissionServiceServer(
				&ClusterServer{
					resourceManager: resourceManager,
					options:         &ClusterServerOptions{},
				}, &RayJobSubmissionServiceServerOptions{},
			)

			url, err := rayJobSubmissionService.getRayClusterURL(ctx, getClusterRequest)

			if tc.expectedErrorString == "" {
				require.NoError(t, err, "No error expected")
			} else {
				require.ErrorContains(t, err, tc.expectedErrorString)
			}

			if tc.expectedURL == "" {
				assert.Empty(t, url, "Expect empty URL")
			} else {
				assert.Equal(t, tc.expectedURL, *url)
			}
		})
	}
}

func TestConvertNodeInfo(t *testing.T) {
	entrypoint := "entrypoint"
	jobID := "ID1"
	submissionID := "subID1"
	message := "some message"
	errorType := "SomeError"

	unixStart := time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC).Unix()
	unixEnd := time.Date(2025, 5, 3, 0, 0, 0, 0, time.UTC).Unix()
	// Prevent overflow when converting negative int64 to uint64
	require.GreaterOrEqual(t, unixStart, int64(0), "start time is negative, invalid Unix timestamp: start=%d", unixStart)
	require.GreaterOrEqual(t, unixEnd, int64(0), "end time is negative, invalid Unix timestamp: end=%d", unixEnd)

	startTime := uint64(unixStart) //nolint:gosec // we've already checked the timestamp is non-negative
	endTime := uint64(unixEnd)     //nolint:gosec // we've already checked the timestamp is non-negative

	metadata := map[string]string{
		"foo": "boo",
	}
	runtimeEnv := utiltypes.RuntimeEnvType{
		"working_dir": "/tmp/workdir",
		"pip":         []string{"numpy", "pandas"},
	}
	expectedRuntimeEnv := map[string]string{
		"working_dir": "/tmp/workdir",
		"pip":         "[numpy pandas]",
	}

	rayJobInfo := utiltypes.RayJobInfo{
		Entrypoint:   entrypoint,
		JobId:        jobID,
		SubmissionId: submissionID,
		JobStatus:    rayv1.JobStatusRunning,
		Message:      message,
		StartTime:    startTime,
		EndTime:      endTime,
		ErrorType:    &errorType,
		Metadata:     metadata,
		RuntimeEnv:   runtimeEnv,
	}

	expected := &api.JobSubmissionInfo{
		Entrypoint:   entrypoint,
		JobId:        jobID,
		SubmissionId: submissionID,
		Status:       string(rayv1.JobStatusRunning),
		Message:      message,
		StartTime:    startTime,
		EndTime:      endTime,
		ErrorType:    errorType,
		Metadata:     metadata,
		RuntimeEnv:   expectedRuntimeEnv,
	}

	result := convertNodeInfo(&rayJobInfo)

	assert.Equal(t, expected, result)
}
