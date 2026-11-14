package ray

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/metrics/mocks"
	utils "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	"github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/scheme"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// fakeBatchScheduler implements schedulerinterface.BatchScheduler for testing.
type fakeBatchScheduler struct {
	schedulerinterface.DefaultBatchScheduler
	cleanupCalled    bool
	cleanupObject    metav1.Object
	cleanupDidUpdate bool
	cleanupErr       error
}

func (f *fakeBatchScheduler) CleanupOnCompletion(_ context.Context, object metav1.Object) (bool, error) {
	f.cleanupCalled = true
	f.cleanupObject = object
	return f.cleanupDidUpdate, f.cleanupErr
}

func TestCreateRayJobSubmitterIfNeed(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = batchv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-raycluster",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Image: "rayproject/ray",
							},
						},
					},
				},
			},
		},
	}

	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
	}

	k8sJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
	}

	// Test 1: Return the existing k8s job if it already exists
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(k8sJob, rayCluster, rayJob).Build()
	ctx := context.TODO()

	rayJobReconciler := &RayJobReconciler{
		Client:   fakeClient,
		Scheme:   newScheme,
		Recorder: &events.FakeRecorder{},
	}

	err := rayJobReconciler.createK8sJobIfNeed(ctx, rayJob, rayCluster)
	require.NoError(t, err)

	// Test 2: Create a new k8s job if it does not already exist
	fakeClient = clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(rayCluster, rayJob).Build()
	rayJobReconciler.Client = fakeClient

	err = rayJobReconciler.createK8sJobIfNeed(ctx, rayJob, rayCluster)
	require.NoError(t, err)

	err = fakeClient.Get(ctx, types.NamespacedName{
		Namespace: k8sJob.Namespace,
		Name:      k8sJob.Name,
	}, k8sJob, nil)
	require.NoError(t, err)

	assert.Equal(t, k8sJob.Labels[utils.RayOriginatedFromCRNameLabelKey], rayJob.Name)
	assert.Equal(t, k8sJob.Labels[utils.RayOriginatedFromCRDLabelKey], utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD))
}

func TestGetSubmitterTemplate(t *testing.T) {
	// RayJob instance with user-provided submitter pod template.
	rayJobInstanceWithTemplate := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			Entrypoint: "echo no quote 'single quote' \"double quote\"",
			SubmitterPodTemplate: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Command: []string{"user-command"},
						},
					},
				},
			},
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "test-url",
			JobId:        "test-job-id",
		},
	}

	// RayJob instance without user-provided submitter pod template.
	// In this case we should use the image of the Ray Head, so specify the image so we can test it.
	rayJobInstanceWithoutTemplate := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			Entrypoint: "echo no quote 'single quote' \"double quote\"",
			RayClusterSpec: &rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Image: "rayproject/ray:custom-version",
								},
							},
						},
					},
				},
			},
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "test-url",
			JobId:        "test-job-id",
		},
	}
	rayClusterInstance := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Image: "rayproject/ray:custom-version",
							},
						},
					},
				},
			},
		},
	}

	// Test 1: User provided template with command
	submitterTemplate, err := getSubmitterTemplate(rayJobInstanceWithTemplate, rayClusterInstance)
	require.NoError(t, err)
	assert.Equal(t, "user-command", submitterTemplate.Spec.Containers[utils.RayContainerIndex].Command[0])

	// Test 2: User provided template without command
	rayJobInstanceWithTemplate.Spec.SubmitterPodTemplate.Spec.Containers[utils.RayContainerIndex].Command = []string{}
	submitterTemplate, err = getSubmitterTemplate(rayJobInstanceWithTemplate, rayClusterInstance)
	require.NoError(t, err)
	assert.Equal(t, []string{"/bin/bash", "-ce", "--"}, submitterTemplate.Spec.Containers[utils.RayContainerIndex].Command)
	expectedK8sJobModeArgs := []string{
		"until " + fmt.Sprintf(utils.K8sJobDashboardHealthCommand, utils.RayDashboardGCSHealthPath, utils.RayDashboardGCSHealthCheckTimeoutSeconds, "http://test-url") +
			" >/dev/null 2>&1 ; do echo \"Waiting for Ray Dashboard GCS to become healthy at http://test-url ...\" ; sleep 2 ; done ; " +
			"if ! ray job status --address http://test-url test-job-id >/dev/null 2>&1 ; then ray job submit --address http://test-url --no-wait --submission-id test-job-id -- echo no quote 'single quote' \"double quote\" ; fi ; ray job logs --address http://test-url --follow test-job-id",
	}
	assert.Equal(t, expectedK8sJobModeArgs, submitterTemplate.Spec.Containers[utils.RayContainerIndex].Args)

	// Test 3: User did not provide template, should use the image of the Ray Head
	submitterTemplate, err = getSubmitterTemplate(rayJobInstanceWithoutTemplate, rayClusterInstance)
	require.NoError(t, err)
	assert.Equal(t, []string{"/bin/bash", "-ce", "--"}, submitterTemplate.Spec.Containers[utils.RayContainerIndex].Command)
	assert.Equal(t, expectedK8sJobModeArgs, submitterTemplate.Spec.Containers[utils.RayContainerIndex].Args)
	assert.Equal(t, "rayproject/ray:custom-version", submitterTemplate.Spec.Containers[utils.RayContainerIndex].Image)

	// Test 4: Check default PYTHONUNBUFFERED setting
	submitterTemplate, err = getSubmitterTemplate(rayJobInstanceWithoutTemplate, rayClusterInstance)
	require.NoError(t, err)

	envVar, found := utils.EnvVarByName(PythonUnbufferedEnvVarName, submitterTemplate.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, found)
	assert.Equal(t, "1", envVar.Value)

	// Test 5: Check default RAY_DASHBOARD_ADDRESS env var
	submitterTemplate, err = getSubmitterTemplate(rayJobInstanceWithTemplate, rayClusterInstance)
	require.NoError(t, err)

	envVar, found = utils.EnvVarByName(utils.RAY_DASHBOARD_ADDRESS, submitterTemplate.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, found)
	assert.Equal(t, "test-url", envVar.Value)

	// Test 6: Check default RAY_JOB_SUBMISSION_ID env var
	envVar, found = utils.EnvVarByName(utils.RAY_JOB_SUBMISSION_ID, submitterTemplate.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, found)
	assert.Equal(t, "test-job-id", envVar.Value)
}

func TestGetSubmitterContainerWithFeatureGate(t *testing.T) {
	// Enable the SidecarSubmitterRestart feature gate for this test
	features.SetFeatureGateDuringTest(t, features.SidecarSubmitterRestart, true)

	rayJobInstance := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			Entrypoint:     "echo test",
			SubmissionMode: rayv1.SidecarMode,
			RayClusterSpec: &rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Image: "rayproject/ray:test",
									Ports: []corev1.ContainerPort{
										{
											Name:          utils.DashboardPortName,
											ContainerPort: utils.DefaultDashboardPort,
										},
									},
								},
							},
						},
					},
				},
			},
		},
		Status: rayv1.RayJobStatus{
			DashboardURL: "http://127.0.0.1:8265",
			JobId:        "test-job-id",
		},
	}

	rayClusterInstance := &rayv1.RayCluster{
		Spec: *rayJobInstance.Spec.RayClusterSpec,
	}

	container, err := getSubmitterContainer(rayJobInstance, rayClusterInstance)
	require.NoError(t, err)

	// Verify restart policy is set to OnFailure for the submitter container
	require.NotNil(t, container.RestartPolicy)
	assert.Equal(t, corev1.ContainerRestartPolicyOnFailure, *container.RestartPolicy)
}

func TestCheckIsRestartCountExceeded(t *testing.T) {
	tests := []struct {
		name                 string
		restartCount         int32
		existCode            int32
		hasTermState         bool
		hasLastTermState     bool
		expectedShouldUpdate bool
	}{
		{
			name:                 "restart count below limit of 2",
			restartCount:         1,
			existCode:            1,
			hasTermState:         true,
			hasLastTermState:     true,
			expectedShouldUpdate: false,
		},
		{
			name:                 "restart count at limit of 2",
			restartCount:         2,
			existCode:            1,
			hasTermState:         true,
			hasLastTermState:     true,
			expectedShouldUpdate: true,
		},
		{
			name:                 "successful exit should not trigger failure",
			restartCount:         3,
			existCode:            0,
			hasTermState:         true,
			hasLastTermState:     true,
			expectedShouldUpdate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			containerStatus := corev1.ContainerStatus{
				Name:         utils.SubmitterContainerName,
				RestartCount: tt.restartCount,
			}

			if tt.hasTermState {
				containerStatus.State = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: tt.existCode},
				}
			}

			if tt.hasLastTermState {
				containerStatus.LastTerminationState = corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{ExitCode: 1},
				}
			}

			headPod := &corev1.Pod{
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{containerStatus},
				},
			}

			shouldUpdate, _ := checkIsRestartCountExceeded(headPod, 2)
			assert.Equal(t, tt.expectedShouldUpdate, shouldUpdate)
		})
	}
}

func TestUpdateStatusToSuspendingIfNeeded(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	tests := []struct {
		name                 string
		status               rayv1.JobDeploymentStatus
		suspend              bool
		expectedShouldUpdate bool
	}{
		// When Autoscaler is enabled, the random Pod deletion is controleld by the feature flag `ENABLE_RANDOM_POD_DELETE`.
		{
			name:                 "Suspend is false",
			suspend:              false,
			status:               rayv1.JobDeploymentStatusInitializing,
			expectedShouldUpdate: false,
		},
		{
			name:                 "Suspend is true, but the status is not allowed to transition to suspending",
			suspend:              true,
			status:               rayv1.JobDeploymentStatusComplete,
			expectedShouldUpdate: false,
		},
		{
			name:                 "Suspend is true, and the status is allowed to transition to suspending",
			suspend:              true,
			status:               rayv1.JobDeploymentStatusInitializing,
			expectedShouldUpdate: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			name := "test-rayjob"
			namespace := "default"
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      name,
					Namespace: namespace,
				},
				Spec: rayv1.RayJobSpec{
					Suspend: tc.suspend,
				},
				Status: rayv1.RayJobStatus{
					JobDeploymentStatus: tc.status,
				},
			}

			ctx := context.Background()
			shouldUpdate := updateStatusToSuspendingIfNeeded(ctx, rayJob)
			assert.Equal(t, tc.expectedShouldUpdate, shouldUpdate)

			if tc.expectedShouldUpdate {
				assert.Equal(t, rayv1.JobDeploymentStatusSuspending, rayJob.Status.JobDeploymentStatus)
			} else {
				assert.Equal(t, tc.status, rayJob.Status.JobDeploymentStatus)
			}
		})
	}
}

func TestUpdateRayJobStatus(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)

	rayJobTemplate := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
		Status: rayv1.RayJobStatus{
			JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			JobStatus:           rayv1.JobStatusRunning,
			Message:             "old message",
		},
	}
	newMessage := "new message"

	tests := []struct {
		name                         string
		isJobDeploymentStatusChanged bool
	}{
		{
			name:                         "JobDeploymentStatus is not changed",
			isJobDeploymentStatusChanged: false,
		},
		{
			name:                         "JobDeploymentStatus is changed",
			isJobDeploymentStatusChanged: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldRayJob := rayJobTemplate.DeepCopy()

			// Initialize a fake client with newScheme and runtimeObjects.
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(newScheme).
				WithRuntimeObjects(oldRayJob).
				WithStatusSubresource(oldRayJob).Build()
			ctx := context.Background()

			newRayJob := &rayv1.RayJob{}
			err := fakeClient.Get(ctx, types.NamespacedName{Namespace: oldRayJob.Namespace, Name: oldRayJob.Name}, newRayJob)
			require.NoError(t, err)

			// Update the status
			newRayJob.Status.Message = newMessage
			if tc.isJobDeploymentStatusChanged {
				newRayJob.Status.JobDeploymentStatus = rayv1.JobDeploymentStatusSuspending
			}

			// Initialize a new RayClusterReconciler.
			testRayJobReconciler := &RayJobReconciler{
				Client:   fakeClient,
				Recorder: &events.FakeRecorder{},
				Scheme:   newScheme,
			}

			err = testRayJobReconciler.updateRayJobStatus(ctx, oldRayJob, newRayJob)
			require.NoError(t, err)

			err = fakeClient.Get(ctx, types.NamespacedName{Namespace: newRayJob.Namespace, Name: newRayJob.Name}, newRayJob)
			require.NoError(t, err)
			assert.Equal(t, newRayJob.Status.Message == newMessage, tc.isJobDeploymentStatusChanged)
		})
	}
}

func TestUpdateRayJobStatusPersistsJobStatusCheckFailureStartTime(t *testing.T) {
	newScheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(newScheme))

	oldRayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
		Status: rayv1.RayJobStatus{
			JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			JobStatus:           rayv1.JobStatusRunning,
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(newScheme).
		WithRuntimeObjects(oldRayJob).
		WithStatusSubresource(oldRayJob).Build()
	ctx := context.Background()

	newRayJob := &rayv1.RayJob{}
	err := fakeClient.Get(ctx, types.NamespacedName{Namespace: oldRayJob.Namespace, Name: oldRayJob.Name}, newRayJob)
	require.NoError(t, err)

	startTime := metav1.NewTime(time.Now())
	newRayJob.Status.JobStatusCheckFailureStartTime = &startTime

	testRayJobReconciler := &RayJobReconciler{
		Client:   fakeClient,
		Recorder: &events.FakeRecorder{},
		Scheme:   newScheme,
	}

	err = testRayJobReconciler.updateRayJobStatus(ctx, oldRayJob, newRayJob)
	require.NoError(t, err)

	err = fakeClient.Get(ctx, types.NamespacedName{Namespace: newRayJob.Namespace, Name: newRayJob.Name}, newRayJob)
	require.NoError(t, err)
	require.NotNil(t, newRayJob.Status.JobStatusCheckFailureStartTime)
	assert.WithinDuration(t, startTime.Time, newRayJob.Status.JobStatusCheckFailureStartTime.Time, time.Second)
}

func TestUpdateRayJobStatusClearsJobStatusCheckFailureStartTime(t *testing.T) {
	newScheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(newScheme))

	startTime := metav1.NewTime(time.Now())
	oldRayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
		Status: rayv1.RayJobStatus{
			JobDeploymentStatus:            rayv1.JobDeploymentStatusRunning,
			JobStatus:                      rayv1.JobStatusRunning,
			JobStatusCheckFailureStartTime: &startTime,
		},
	}

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(newScheme).
		WithRuntimeObjects(oldRayJob).
		WithStatusSubresource(oldRayJob).Build()
	ctx := context.Background()

	newRayJob := &rayv1.RayJob{}
	err := fakeClient.Get(ctx, types.NamespacedName{Namespace: oldRayJob.Namespace, Name: oldRayJob.Name}, newRayJob)
	require.NoError(t, err)
	newRayJob.Status.JobStatusCheckFailureStartTime = nil

	testRayJobReconciler := &RayJobReconciler{
		Client:   fakeClient,
		Recorder: &events.FakeRecorder{},
		Scheme:   newScheme,
	}

	err = testRayJobReconciler.updateRayJobStatus(ctx, oldRayJob, newRayJob)
	require.NoError(t, err)

	err = fakeClient.Get(ctx, types.NamespacedName{Namespace: newRayJob.Namespace, Name: newRayJob.Name}, newRayJob)
	require.NoError(t, err)
	assert.Nil(t, newRayJob.Status.JobStatusCheckFailureStartTime)
}

func TestFailedToCreateRayJobSubmitterEvent(t *testing.T) {
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
	}

	submitterTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-submit-pod",
			Namespace: "default",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "ray-submit",
					Image: "rayproject/ray:latest",
				},
			},
		},
	}

	fakeClient := clientFake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
			return errors.New("random")
		},
	}).WithScheme(scheme.Scheme).Build()

	recorder := events.NewFakeRecorder(100)

	reconciler := &RayJobReconciler{
		Client:   fakeClient,
		Recorder: recorder,
		Scheme:   scheme.Scheme,
	}

	err := reconciler.createNewK8sJob(context.Background(), rayJob, submitterTemplate)

	require.Error(t, err, "Expected error due to simulated job creation failure")

	var foundFailureEvent bool
	events := []string{}
	for len(recorder.Events) > 0 {
		event := <-recorder.Events
		if strings.Contains(event, "Failed to create new Kubernetes Job") {
			foundFailureEvent = true
			break
		}
		events = append(events, event)
	}

	assert.Truef(t, foundFailureEvent, "Expected event to be generated for job creation failure, got events: %s", strings.Join(events, "\n"))
}

// TestCreateNewK8sJob_PropagatesLabelsToSubmitterPodTemplate verifies that createNewK8sJob
// propagates KubeRay labels to the submitter pod template so that NetworkPolicies can
// select submitter pods by KubeRay-specific labels.
func TestCreateNewK8sJob_PropagatesLabelsToSubmitterPodTemplate(t *testing.T) {
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
			UID:       "test-uid",
		},
	}

	submitterTemplate := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{
				utils.RayOriginatedFromCRNameLabelKey: rayJob.Name,
				utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD),
				utils.KubernetesCreatedByLabelKey:     utils.ComponentName,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "ray-submit", Image: "rayproject/ray:latest"},
			},
		},
	}

	var createdJob *batchv1.Job
	fakeClient := clientFake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, obj client.Object, _ ...client.CreateOption) error {
			if job, ok := obj.(*batchv1.Job); ok {
				createdJob = job
			}
			return nil
		},
	}).WithScheme(scheme.Scheme).Build()

	reconciler := &RayJobReconciler{
		Client:   fakeClient,
		Recorder: events.NewFakeRecorder(10),
		Scheme:   scheme.Scheme,
	}

	err := reconciler.createNewK8sJob(context.Background(), rayJob, submitterTemplate)
	require.NoError(t, err)
	require.NotNil(t, createdJob)

	podLabels := createdJob.Spec.Template.Labels
	assert.Equal(t, "test-rayjob", podLabels[utils.RayOriginatedFromCRNameLabelKey],
		"Pod template should have ray.io/originated-from-cr-name label")
	assert.Equal(t, utils.RayOriginatedFromCRDLabelValue(utils.RayJobCRD), podLabels[utils.RayOriginatedFromCRDLabelKey],
		"Pod template should have ray.io/originated-from-crd label")
	assert.Equal(t, utils.ComponentName, podLabels[utils.KubernetesCreatedByLabelKey],
		"Pod template should have app.kubernetes.io/created-by label")
}

func TestFailedCreateRayClusterEvent(t *testing.T) {
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
		Spec: rayv1.RayJobSpec{
			RayClusterSpec: &rayv1.RayClusterSpec{},
		},
	}

	fakeClient := clientFake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Create: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.CreateOption) error {
			return errors.New("random")
		},
	}).WithScheme(scheme.Scheme).Build()

	recorder := events.NewFakeRecorder(100)

	reconciler := &RayJobReconciler{
		Client:   fakeClient,
		Recorder: recorder,
		Scheme:   scheme.Scheme,
	}

	_, err := reconciler.getOrCreateRayClusterInstance(context.Background(), rayJob)

	require.Error(t, err, "Expected error due to cluster creation failure")

	var foundFailureEvent bool
	events := []string{}
	for len(recorder.Events) > 0 {
		event := <-recorder.Events
		if strings.Contains(event, "Failed to create RayCluster") {
			foundFailureEvent = true
			break
		}
		events = append(events, event)
	}

	assert.Truef(t, foundFailureEvent, "Expected event to be generated for cluster creation failure, got events: %s", strings.Join(events, "\n"))
}

func TestFailedDeleteRayJobSubmitterEvent(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = batchv1.AddToScheme(newScheme)

	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
	}
	submitter := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
	}

	fakeClient := clientFake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
			return errors.New("random")
		},
	}).WithScheme(newScheme).WithRuntimeObjects(submitter).Build()

	recorder := events.NewFakeRecorder(100)

	reconciler := &RayJobReconciler{
		Client:   fakeClient,
		Recorder: recorder,
		Scheme:   scheme.Scheme,
	}

	_, err := reconciler.deleteSubmitterJob(context.Background(), rayJob)

	require.Error(t, err, "Expected error due to job deletion failure")

	var foundFailureEvent bool
	events := []string{}
	for len(recorder.Events) > 0 {
		event := <-recorder.Events
		if strings.Contains(event, "Failed to delete submitter K8s Job") {
			foundFailureEvent = true
			break
		}
		events = append(events, event)
	}

	assert.Truef(t, foundFailureEvent, "Expected event to be generated for cluster deletion failure, got events: %s", strings.Join(events, "\n"))
}

func TestFailedDeleteRayClusterEvent(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)

	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-raycluster",
			Namespace: "default",
		},
	}

	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayjob",
			Namespace: "default",
		},
		Status: rayv1.RayJobStatus{
			RayClusterName: "test-raycluster",
		},
	}

	fakeClient := clientFake.NewClientBuilder().WithInterceptorFuncs(interceptor.Funcs{
		Delete: func(_ context.Context, _ client.WithWatch, _ client.Object, _ ...client.DeleteOption) error {
			return errors.New("random")
		},
	}).WithScheme(newScheme).WithRuntimeObjects(rayCluster).Build()

	recorder := events.NewFakeRecorder(100)

	reconciler := &RayJobReconciler{
		Client:   fakeClient,
		Recorder: recorder,
		Scheme:   scheme.Scheme,
	}

	_, err := reconciler.deleteClusterResources(context.Background(), rayJob)

	require.Error(t, err, "Expected error due to cluster deletion failure")

	var foundFailureEvent bool
	events := []string{}
	for len(recorder.Events) > 0 {
		event := <-recorder.Events
		if strings.Contains(event, "Failed to delete cluster") {
			foundFailureEvent = true
			break
		}
		events = append(events, event)
	}

	assert.Truef(t, foundFailureEvent, "Expected event to be generated for cluster deletion failure, got events: %s", strings.Join(events, "\n"))
}

func TestEmitRayJobExecutionDuration(t *testing.T) {
	rayJobName := "test-job"
	rayJobNamespace := "default"
	rayJobUID := types.UID("test-job-uid")
	mockTime := time.Now().Add(-60 * time.Second)

	tests := []struct {
		name                        string
		originalRayJobStatus        rayv1.RayJobStatus
		rayJobStatus                rayv1.RayJobStatus
		expectMetricsCall           bool
		expectedJobDeploymentStatus rayv1.JobDeploymentStatus
		expectedRetryCount          int
		expectedDuration            float64
	}{
		{
			name: "non-terminal to complete state should emit metrics",
			originalRayJobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			},
			rayJobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusComplete,
				StartTime:           &metav1.Time{Time: mockTime},
			},
			expectMetricsCall:           true,
			expectedJobDeploymentStatus: rayv1.JobDeploymentStatusComplete,
			expectedRetryCount:          0,
			expectedDuration:            60.0,
		},
		{
			name: "non-terminal to failed state should emit metrics",
			originalRayJobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			},
			rayJobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusFailed,
				StartTime:           &metav1.Time{Time: mockTime},
			},
			expectMetricsCall:           true,
			expectedJobDeploymentStatus: rayv1.JobDeploymentStatusFailed,
			expectedRetryCount:          0,
			expectedDuration:            60.0,
		},
		{
			name: "non-terminal to retrying state should emit metrics",
			originalRayJobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
				Failed:              ptr.To[int32](2),
			},
			rayJobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusRetrying,
				StartTime:           &metav1.Time{Time: mockTime},
			},
			expectMetricsCall:           true,
			expectedJobDeploymentStatus: rayv1.JobDeploymentStatusRetrying,
			expectedRetryCount:          2,
			expectedDuration:            60.0,
		},
		{
			name: "non-terminal to non-terminal state should not emit metrics",
			originalRayJobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusInitializing,
			},
			rayJobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			},
			expectMetricsCall: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockObserver := mocks.NewMockRayJobMetricsObserver(ctrl)
			if tt.expectMetricsCall {
				mockObserver.EXPECT().
					ObserveRayJobExecutionDuration(
						rayJobName,
						rayJobNamespace,
						rayJobUID,
						tt.expectedJobDeploymentStatus,
						tt.expectedRetryCount,
						mock.MatchedBy(func(d float64) bool {
							// Allow some wiggle room in timing
							return math.Abs(d-tt.expectedDuration) < 1.0
						}),
					).Times(1)
			}

			emitRayJobExecutionDuration(mockObserver, rayJobName, rayJobNamespace, rayJobUID, tt.originalRayJobStatus, tt.rayJobStatus)
		})
	}
}

func TestGetSubmitterTemplate_WithEnableK8sTokenAuth(t *testing.T) {
	rayJob := &rayv1.RayJob{
		Spec: rayv1.RayJobSpec{
			SubmissionMode: rayv1.K8sJobMode,
		},
	}
	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-raycluster",
		},
		Spec: rayv1.RayClusterSpec{
			AuthOptions: &rayv1.AuthOptions{
				Mode:               rayv1.AuthModeToken,
				EnableK8sTokenAuth: new(true),
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Image: "rayproject/ray",
							},
						},
					},
				},
			},
		},
	}

	template, err := getSubmitterTemplate(rayJob, rayCluster)
	require.NoError(t, err)

	// Check volume
	foundVolume := false
	for _, v := range template.Spec.Volumes {
		if v.Name == utils.RayTokenVolumeName && v.Projected != nil {
			foundVolume = true
			break
		}
	}
	assert.True(t, foundVolume, "Submitter Pod should have the ray-token volume")

	// Check volume mount
	foundVolumeMount := false
	for _, vm := range template.Spec.Containers[utils.RayContainerIndex].VolumeMounts {
		if vm.Name == utils.RayTokenVolumeName && vm.MountPath == utils.RayTokenMountPath {
			foundVolumeMount = true
			break
		}
	}
	assert.True(t, foundVolumeMount, "Submitter container should have the ray-token volume mount")
}

func TestBatchSchedulerOnCompletionCalledWhenRayJobComplete(t *testing.T) {
	tests := []struct {
		name                string
		jobDeploymentStatus rayv1.JobDeploymentStatus
		cleanupDidUpdate    bool
		cleanupErr          error
		expectCleanupCalled bool
		expectCleanupEvent  string
	}{
		{
			name:                "Complete status - cleanup performed successfully",
			jobDeploymentStatus: rayv1.JobDeploymentStatusComplete,
			cleanupDidUpdate:    true,
			cleanupErr:          nil,
			expectCleanupCalled: true,
			expectCleanupEvent:  string(utils.BatchSchedulerCleanedUp),
		},
		{
			name:                "Complete status - cleanup no-op (no resources to clean)",
			jobDeploymentStatus: rayv1.JobDeploymentStatusComplete,
			cleanupDidUpdate:    false,
			cleanupErr:          nil,
			expectCleanupCalled: true,
			expectCleanupEvent:  "",
		},
		{
			name:                "Complete status - cleanup returns error",
			jobDeploymentStatus: rayv1.JobDeploymentStatusComplete,
			cleanupDidUpdate:    false,
			cleanupErr:          errors.New("cleanup failed"),
			expectCleanupCalled: true,
			expectCleanupEvent:  string(utils.FailedToCleanupBatchScheduler),
		},
		{
			name:                "Failed status - cleanup performed successfully",
			jobDeploymentStatus: rayv1.JobDeploymentStatusFailed,
			cleanupDidUpdate:    true,
			cleanupErr:          nil,
			expectCleanupCalled: true,
			expectCleanupEvent:  string(utils.BatchSchedulerCleanedUp),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			_ = rayv1.AddToScheme(newScheme)

			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					Entrypoint: "echo hello",
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "ray-head",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
					},
				},
				Status: rayv1.RayJobStatus{
					JobDeploymentStatus: tc.jobDeploymentStatus,
					JobStatus:           rayv1.JobStatusSucceeded,
					RayClusterName:      "test-raycluster",
				},
			}

			fakeClient := clientFake.NewClientBuilder().
				WithScheme(newScheme).
				WithRuntimeObjects(rayJob).
				WithStatusSubresource(rayJob).
				Build()

			fakeScheduler := &fakeBatchScheduler{
				cleanupDidUpdate: tc.cleanupDidUpdate,
				cleanupErr:       tc.cleanupErr,
			}
			schedulerManager := batchscheduler.NewSchedulerManagerForTest(fakeScheduler)

			recorder := events.NewFakeRecorder(100)

			reconciler := &RayJobReconciler{
				Client:   fakeClient,
				Recorder: recorder,
				Scheme:   newScheme,
				options: RayJobReconcilerOptions{
					BatchSchedulerManager: schedulerManager,
				},
			}

			ctx := context.Background()
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      rayJob.Name,
					Namespace: rayJob.Namespace,
				},
			})
			// The Reconcile should not return an error for terminal states
			require.NoError(t, err)

			// Verify CleanupOnCompletion was called
			assert.True(t, fakeScheduler.cleanupCalled, "CleanupOnCompletion should have been called when RayJob is in %s status", tc.jobDeploymentStatus)
			assert.Equal(t, rayJob.Name, fakeScheduler.cleanupObject.GetName(), "CleanupOnCompletion should receive the correct RayJob object")
			assert.Equal(t, rayJob.Namespace, fakeScheduler.cleanupObject.GetNamespace(), "CleanupOnCompletion should receive the correct RayJob namespace")

			// Verify the expected event was emitted
			if tc.expectCleanupEvent != "" {
				var foundEvent bool
				for len(recorder.Events) > 0 {
					event := <-recorder.Events
					if strings.Contains(event, tc.expectCleanupEvent) {
						foundEvent = true
						break
					}
				}
				assert.True(t, foundEvent, "Expected event %q to be emitted", tc.expectCleanupEvent)
			}
		})
	}
}

func TestGetJobStatusCheckTimeoutSeconds(t *testing.T) {
	t.Setenv(utils.RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS, "120")
	assert.Equal(t, 120, getJobStatusCheckTimeoutSeconds())

	t.Setenv(utils.RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS, "invalid")
	assert.Equal(t, utils.DEFAULT_RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS, getJobStatusCheckTimeoutSeconds())

	os.Unsetenv(utils.RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS)
	assert.Equal(t, utils.DEFAULT_RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS, getJobStatusCheckTimeoutSeconds())
}

func TestRecordJobStatusCheckFailure(t *testing.T) {
	ctx := context.Background()
	rayJob := &rayv1.RayJob{
		Status: rayv1.RayJobStatus{JobId: "test-job"},
	}
	testErr := errors.New("connection refused")

	needsPersist := recordJobStatusCheckFailure(ctx, rayJob, testErr)
	assert.True(t, needsPersist)
	assert.NotNil(t, rayJob.Status.JobStatusCheckFailureStartTime)

	needsPersist = recordJobStatusCheckFailure(ctx, rayJob, testErr)
	assert.False(t, needsPersist)
}

func TestCheckJobStatusCheckTimeoutAndUpdateStatusIfNeeded(t *testing.T) {
	ctx := context.Background()
	rayJob := &rayv1.RayJob{
		Status: rayv1.RayJobStatus{JobId: "test-job"},
	}

	assert.False(t, checkJobStatusCheckTimeoutAndUpdateStatusIfNeeded(ctx, rayJob))

	rayJob.Status.JobStatusCheckFailureStartTime = &metav1.Time{Time: time.Now()}
	assert.False(t, checkJobStatusCheckTimeoutAndUpdateStatusIfNeeded(ctx, rayJob))

	t.Setenv(utils.RAYJOB_STATUS_CHECK_TIMEOUT_SECONDS, "1")
	rayJob.Status.JobStatusCheckFailureStartTime = &metav1.Time{Time: time.Now().Add(-2 * time.Second)}
	assert.True(t, checkJobStatusCheckTimeoutAndUpdateStatusIfNeeded(ctx, rayJob))
	assert.Equal(t, rayv1.JobDeploymentStatusFailed, rayJob.Status.JobDeploymentStatus)
	assert.Equal(t, rayv1.JobStatusCheckTimeoutExceeded, rayJob.Status.Reason)
	assert.Contains(t, rayJob.Status.Message, "exceeded timeout of 1s")
}

func TestCheckSubmitterFinishedTimeoutAndUpdateStatusIfNeeded(t *testing.T) {
	ctx := context.Background()
	justFinished := time.Now()
	expired := time.Now().Add(-DefaultSubmitterFinishedTimeout - time.Second)

	const driverNode = "3bbbda71075d7929d056881d8921592b969874dd65ea3ee2d2067ca8"

	tests := []struct {
		finishedAt *time.Time
		jobInfo    *utiltypes.RayJobInfo
		name       string
		jobStatus  rayv1.JobStatus
		aliveNode  string
		wantDeploy rayv1.JobDeploymentStatus
		wantReason rayv1.JobFailedReason
		wantUpdate bool
		aliveErr   bool
	}{
		{
			name:       "submitter still running",
			jobStatus:  rayv1.JobStatusRunning,
			finishedAt: nil,
			jobInfo:    &utiltypes.RayJobInfo{DriverNodeID: driverNode},
			aliveNode:  driverNode,
		},
		{
			name:       "grace period not yet elapsed",
			jobStatus:  rayv1.JobStatusNew,
			finishedAt: &justFinished,
			jobInfo:    &utiltypes.RayJobInfo{DriverNodeID: driverNode},
			aliveNode:  driverNode,
		},
		{
			name:       "job RUNNING on a live driver node is left alone",
			jobStatus:  rayv1.JobStatusRunning,
			finishedAt: &expired,
			jobInfo:    &utiltypes.RayJobInfo{DriverNodeID: driverNode},
			aliveNode:  driverNode,
		},
		{
			// Ray assigns driver_node_id only once it schedules the supervisor, so a PENDING job has
			// none to check. These keep the existing timeout behavior rather than guessing.
			name:       "job PENDING has no driver node yet and times out",
			jobStatus:  rayv1.JobStatusPending,
			finishedAt: &expired,
			jobInfo:    &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusPending},
			wantUpdate: true,
			wantDeploy: rayv1.JobDeploymentStatusFailed,
			wantReason: rayv1.JobDeploymentStatusTransitionGracePeriodExceeded,
		},
		{
			name:       "job RUNNING but the driver node is gone times out",
			jobStatus:  rayv1.JobStatusRunning,
			finishedAt: &expired,
			jobInfo:    &utiltypes.RayJobInfo{DriverNodeID: driverNode},
			aliveNode:  "some-other-node",
			wantUpdate: true,
			wantDeploy: rayv1.JobDeploymentStatusFailed,
			wantReason: rayv1.JobDeploymentStatusTransitionGracePeriodExceeded,
		},
		{
			name:       "job RUNNING with no driver node reported times out",
			jobStatus:  rayv1.JobStatusRunning,
			finishedAt: &expired,
			jobInfo:    &utiltypes.RayJobInfo{},
			wantUpdate: true,
			wantDeploy: rayv1.JobDeploymentStatusFailed,
			wantReason: rayv1.JobDeploymentStatusTransitionGracePeriodExceeded,
		},
		{
			name:       "job never observed times out",
			jobStatus:  rayv1.JobStatusNew,
			finishedAt: &expired,
			jobInfo:    &utiltypes.RayJobInfo{DriverNodeID: driverNode},
			aliveNode:  driverNode,
			wantUpdate: true,
			wantDeploy: rayv1.JobDeploymentStatusFailed,
			wantReason: rayv1.JobDeploymentStatusTransitionGracePeriodExceeded,
		},
		{
			// The dashboard is the fresher source: a job that has already succeeded must not be
			// overwritten with a grace-period failure just because the CR has not caught up.
			name:       "fresh job info is terminal, CR is stale",
			jobStatus:  rayv1.JobStatusRunning,
			finishedAt: &expired,
			jobInfo:    &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusSucceeded, DriverNodeID: driverNode},
			aliveNode:  "some-other-node",
		},
		{
			// A dashboard error is inconclusive, not proof of a dead driver, and must not fail the
			// job or return early and skip the reconcile's status update.
			name:       "node liveness errors leave the job alone",
			jobStatus:  rayv1.JobStatusRunning,
			finishedAt: &expired,
			jobInfo:    &utiltypes.RayJobInfo{JobStatus: rayv1.JobStatusRunning, DriverNodeID: driverNode},
			aliveErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &nodeLivenessDashboardClient{aliveNode: tc.aliveNode, err: tc.aliveErr}
			r := &RayJobReconciler{}
			rayJob := &rayv1.RayJob{Status: rayv1.RayJobStatus{JobStatus: tc.jobStatus}}

			got := r.checkSubmitterFinishedTimeoutAndUpdateStatusIfNeeded(ctx, rayJob, client, tc.jobInfo, tc.finishedAt)
			assert.Equal(t, tc.wantUpdate, got)
			if !tc.wantUpdate {
				assert.Equal(t, tc.jobStatus, rayJob.Status.JobStatus, "job status must not be overwritten")
				assert.Empty(t, rayJob.Status.Reason)
				return
			}
			assert.Equal(t, tc.wantDeploy, rayJob.Status.JobDeploymentStatus)
			assert.Equal(t, tc.wantReason, rayJob.Status.Reason)
		})
	}
}

// nodeLivenessDashboardClient answers IsNodeAlive for exactly one node.
type nodeLivenessDashboardClient struct {
	*utils.FakeRayDashboardClient
	aliveNode string
	err       bool
}

func (c *nodeLivenessDashboardClient) IsNodeAlive(_ context.Context, nodeID string) (bool, error) {
	if c.err {
		return false, errors.New("dashboard unreachable")
	}
	return nodeID != "" && nodeID == c.aliveNode, nil
}

// TestBatchSchedulerCleanupCalledWhenRayJobSuspending verifies that batch scheduler resources are
// cleaned up when a RayJob is suspended/retrying. Unlike the terminal (Complete/Failed) path, the
// suspend path must requeue on cleanup failure rather than swallow the error, otherwise the PodGroup
// would leak once the status transitions to Suspended (which is no longer requeued).
func TestBatchSchedulerCleanupCalledWhenRayJobSuspendingOrRetrying(t *testing.T) {
	tests := []struct {
		name                string
		jobDeploymentStatus rayv1.JobDeploymentStatus
		expectedStatus      rayv1.JobDeploymentStatus
		cleanupErr          error
		cleanupDidUpdate    bool
		expectReconcileErr  bool
		expectCleanupEvent  string
	}{
		{
			name:                "Suspending - cleanup succeeds, transitions to Suspended",
			jobDeploymentStatus: rayv1.JobDeploymentStatusSuspending,
			expectedStatus:      rayv1.JobDeploymentStatusSuspended,
			cleanupDidUpdate:    true,
			expectCleanupEvent:  string(utils.BatchSchedulerCleanedUp),
		},
		{
			name:                "Retrying - cleanup succeeds, transitions to New",
			jobDeploymentStatus: rayv1.JobDeploymentStatusRetrying,
			expectedStatus:      rayv1.JobDeploymentStatusNew,
			cleanupDidUpdate:    true,
			expectCleanupEvent:  string(utils.BatchSchedulerCleanedUp),
		},
		{
			name:                "Suspending - cleanup fails, requeues and stays Suspending",
			jobDeploymentStatus: rayv1.JobDeploymentStatusSuspending,
			expectedStatus:      rayv1.JobDeploymentStatusSuspending,
			cleanupErr:          errors.New("cleanup failed"),
			expectReconcileErr:  true,
			expectCleanupEvent:  string(utils.FailedToCleanupBatchScheduler),
		},
		{
			name:                "Retrying - cleanup fails, requeues and stays Retrying",
			jobDeploymentStatus: rayv1.JobDeploymentStatusRetrying,
			expectedStatus:      rayv1.JobDeploymentStatusRetrying,
			cleanupErr:          errors.New("cleanup failed"),
			expectReconcileErr:  true,
			expectCleanupEvent:  string(utils.FailedToCleanupBatchScheduler),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			_ = rayv1.AddToScheme(newScheme)
			_ = batchv1.AddToScheme(newScheme)

			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-rayjob",
					Namespace: "default",
				},
				Spec: rayv1.RayJobSpec{
					Entrypoint: "echo hello",
					RayClusterSpec: &rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "ray-head",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
					},
				},
				Status: rayv1.RayJobStatus{
					JobDeploymentStatus: tc.jobDeploymentStatus,
					// The RayCluster and submitter Job are intentionally absent from the fake client,
					// so deleteClusterResources/deleteSubmitterJob treat them as already deleted and
					// the reconcile proceeds to the batch scheduler cleanup.
					RayClusterName: "test-raycluster",
				},
			}

			fakeClient := clientFake.NewClientBuilder().
				WithScheme(newScheme).
				WithRuntimeObjects(rayJob).
				WithStatusSubresource(rayJob).
				Build()

			fakeScheduler := &fakeBatchScheduler{
				cleanupDidUpdate: tc.cleanupDidUpdate,
				cleanupErr:       tc.cleanupErr,
			}
			schedulerManager := batchscheduler.NewSchedulerManagerForTest(fakeScheduler)

			recorder := events.NewFakeRecorder(100)

			reconciler := &RayJobReconciler{
				Client:   fakeClient,
				Recorder: recorder,
				Scheme:   newScheme,
				options: RayJobReconcilerOptions{
					BatchSchedulerManager: schedulerManager,
				},
			}

			ctx := context.Background()
			_, err := reconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Name:      rayJob.Name,
					Namespace: rayJob.Namespace,
				},
			})
			if tc.expectReconcileErr {
				require.Error(t, err, "Reconcile should requeue with an error when cleanup fails during suspend/retry")
			} else {
				require.NoError(t, err)
			}

			require.True(t, fakeScheduler.cleanupCalled, "CleanupOnCompletion should have been called when RayJob is in %s status", tc.jobDeploymentStatus)
			assert.Equal(t, rayJob.Name, fakeScheduler.cleanupObject.GetName(), "CleanupOnCompletion should receive the correct RayJob object")

			updated := &rayv1.RayJob{}
			require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: rayJob.Name, Namespace: rayJob.Namespace}, updated))
			assert.Equal(t, tc.expectedStatus, updated.Status.JobDeploymentStatus)

			if tc.expectCleanupEvent != "" {
				var foundEvent bool
				for len(recorder.Events) > 0 {
					event := <-recorder.Events
					if strings.Contains(event, tc.expectCleanupEvent) {
						foundEvent = true
						break
					}
				}
				assert.True(t, foundEvent, "Expected event %q to be emitted", tc.expectCleanupEvent)
			}
		})
	}
}

// TestCheckSubmitterAndUpdateStatusIfNeeded_K8sJobMode covers a submitter Kubernetes Job failing
// underneath a job that is still running: an infrastructure-killed submitter exhausts its backoff
// and fails the Job, but the driver runs on the cluster and carries on (#2314).
func TestCheckSubmitterAndUpdateStatusIfNeeded_SidecarMode(t *testing.T) {
	newRayJob := func(jobStatus rayv1.JobStatus) *rayv1.RayJob {
		return &rayv1.RayJob{
			ObjectMeta: metav1.ObjectMeta{Name: "rayjob-sample", Namespace: "default"},
			Spec: rayv1.RayJobSpec{
				SubmissionMode: rayv1.SidecarMode,
				RayClusterSpec: &rayv1.RayClusterSpec{},
			},
			Status: rayv1.RayJobStatus{JobStatus: jobStatus, RayClusterName: "raycluster-sample"},
		}
	}

	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "raycluster-sample", Namespace: "default"},
	}

	headPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "head-pod",
			Namespace: "default",
			Labels: map[string]string{
				utils.RayClusterLabelKey:  "raycluster-sample",
				utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
			},
		},
		Status: corev1.PodStatus{
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: utils.SubmitterContainerName,
				State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
					ExitCode:   1,
					Reason:     "Error",
					FinishedAt: metav1.NewTime(time.Now()),
				}},
			}},
		},
	}

	tests := []struct {
		name       string
		jobStatus  rayv1.JobStatus
		wantReason rayv1.JobFailedReason
		wantUpdate bool
	}{
		{
			// Same rule as K8sJobMode: a sidecar that exits non-zero while the job is
			// running is not evidence about the job, so the timeout decides it instead.
			name:      "job running is left to the submitter-finished timeout",
			jobStatus: rayv1.JobStatusRunning,
		},
		{
			name:       "job never observed is a genuine submission failure",
			jobStatus:  rayv1.JobStatusNew,
			wantUpdate: true,
			wantReason: rayv1.SubmissionFailed,
		},
		{
			name:       "job already failed keeps the application failure reason",
			jobStatus:  rayv1.JobStatusFailed,
			wantUpdate: true,
			wantReason: rayv1.AppFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			require.NoError(t, rayv1.AddToScheme(newScheme))
			require.NoError(t, corev1.AddToScheme(newScheme))
			require.NoError(t, batchv1.AddToScheme(newScheme))

			rayJob := newRayJob(tc.jobStatus)
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).
				WithRuntimeObjects(rayJob, rayCluster, headPod).Build()
			r := &RayJobReconciler{Client: fakeClient, Scheme: newScheme}

			shouldUpdate, _, err := r.checkSubmitterAndUpdateStatusIfNeeded(context.Background(), rayJob)
			require.NoError(t, err)
			assert.Equal(t, tc.wantUpdate, shouldUpdate)
			if !tc.wantUpdate {
				assert.Equal(t, tc.jobStatus, rayJob.Status.JobStatus)
				assert.Empty(t, rayJob.Status.JobDeploymentStatus)
				assert.Empty(t, rayJob.Status.Reason)
				return
			}
			assert.Equal(t, rayv1.JobDeploymentStatusFailed, rayJob.Status.JobDeploymentStatus)
			assert.Equal(t, tc.wantReason, rayJob.Status.Reason)
		})
	}
}

func TestCheckSubmitterAndUpdateStatusIfNeeded_K8sJobMode(t *testing.T) {
	failedAt := metav1.NewTime(time.Now())

	newRayJob := func(jobStatus rayv1.JobStatus) *rayv1.RayJob {
		return &rayv1.RayJob{
			ObjectMeta: metav1.ObjectMeta{Name: "rayjob-sample", Namespace: "default"},
			Spec:       rayv1.RayJobSpec{SubmissionMode: rayv1.K8sJobMode},
			Status:     rayv1.RayJobStatus{JobStatus: jobStatus, RayClusterName: "raycluster-sample"},
		}
	}

	failedSubmitterJob := func(rayJob *rayv1.RayJob) *batchv1.Job {
		return &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{Name: rayJob.Name, Namespace: rayJob.Namespace},
			Status: batchv1.JobStatus{Conditions: []batchv1.JobCondition{{
				Type:               batchv1.JobFailed,
				Status:             corev1.ConditionTrue,
				Reason:             "BackoffLimitExceeded",
				Message:            "Job has reached the specified backoff limit",
				LastTransitionTime: failedAt,
			}}},
		}
	}

	tests := []struct {
		name       string
		jobStatus  rayv1.JobStatus
		wantReason rayv1.JobFailedReason
		wantUpdate bool
	}{
		{
			// The submission plainly succeeded, so SubmissionFailed would be wrong. The
			// submitter-finished timeout decides this one on live cluster state instead.
			name:      "job running is left to the submitter-finished timeout",
			jobStatus: rayv1.JobStatusRunning,
		},
		{
			name:       "job never observed is a genuine submission failure",
			jobStatus:  rayv1.JobStatusNew,
			wantUpdate: true,
			wantReason: rayv1.SubmissionFailed,
		},
		{
			name:       "job already failed keeps the application failure reason",
			jobStatus:  rayv1.JobStatusFailed,
			wantUpdate: true,
			wantReason: rayv1.AppFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			require.NoError(t, rayv1.AddToScheme(newScheme))
			require.NoError(t, corev1.AddToScheme(newScheme))
			require.NoError(t, batchv1.AddToScheme(newScheme))

			rayJob := newRayJob(tc.jobStatus)
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).
				WithRuntimeObjects(rayJob, failedSubmitterJob(rayJob)).Build()
			r := &RayJobReconciler{Client: fakeClient, Scheme: newScheme}

			shouldUpdate, finishedAt, err := r.checkSubmitterAndUpdateStatusIfNeeded(context.Background(), rayJob)
			require.NoError(t, err)
			// A failed submitter Job is terminal, so the timeout path always gets a reference point.
			assert.NotNil(t, finishedAt)
			assert.Equal(t, tc.wantUpdate, shouldUpdate)
			if !tc.wantUpdate {
				assert.Equal(t, tc.jobStatus, rayJob.Status.JobStatus)
				assert.Empty(t, rayJob.Status.JobDeploymentStatus)
				assert.Empty(t, rayJob.Status.Reason)
				return
			}
			assert.Equal(t, rayv1.JobDeploymentStatusFailed, rayJob.Status.JobDeploymentStatus)
			assert.Equal(t, tc.wantReason, rayJob.Status.Reason)
		})
	}
}
