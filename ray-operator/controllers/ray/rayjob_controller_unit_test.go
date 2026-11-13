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

// TestReconcileHTTPModeJobNotFound covers the two meanings of a GetJobInfo 404 in HTTPMode
// (https://github.com/ray-project/kuberay/issues/4824). If the job was never observed on the
// cluster, the 404 simply means the job has not been submitted yet, so the initial submission
// must proceed. If the job was already observed, the 404 means the cluster lost the job state
// (e.g. the head Pod was recreated), so the RayJob must transition to Failed and let
// spec.backoffLimit govern retries (consistent with K8sJobMode) instead of being silently
// resubmitted to the new head.
func TestReconcileHTTPModeJobNotFound(t *testing.T) {
	tests := []struct {
		name                        string
		jobStatus                   rayv1.JobStatus
		expectedJobDeploymentStatus rayv1.JobDeploymentStatus
		expectedReason              rayv1.JobFailedReason
		expectedSubmitCalls         int
	}{
		{
			name:                        "job not observed yet, initial submission proceeds",
			jobStatus:                   rayv1.JobStatusNew,
			expectedJobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			expectedSubmitCalls:         1,
		},
		{
			name:                        "previously observed job disappeared, mark Failed instead of resubmitting",
			jobStatus:                   rayv1.JobStatusRunning,
			expectedJobDeploymentStatus: rayv1.JobDeploymentStatusFailed,
			expectedReason:              rayv1.AppFailed,
			expectedSubmitCalls:         0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rayJob := &rayv1.RayJob{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rayjob", Namespace: "default"},
				Spec: rayv1.RayJobSpec{
					SubmissionMode: rayv1.HTTPMode,
					BackoffLimit:   ptr.To[int32](0),
				},
				Status: rayv1.RayJobStatus{
					JobId:               "test-rayjob-abcde",
					JobStatus:           tc.jobStatus,
					JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
				},
			}

			fakeDashboardClient := &utils.FakeRayDashboardClient{}
			submitCalls := 0
			submitJobMock := func(_ context.Context, _ *rayv1.RayJob) (string, error) { //nolint:unparam // This is a mock function so the signature cannot change
				submitCalls++
				return rayJob.Status.JobId, nil
			}
			fakeDashboardClient.SubmitJobMock.Store(&submitJobMock)

			reconcileHTTPModeJobNotFound(ctx, rayJob, fakeDashboardClient)

			assert.Equal(t, tc.expectedSubmitCalls, submitCalls)
			assert.Equal(t, tc.expectedJobDeploymentStatus, rayJob.Status.JobDeploymentStatus)
			assert.Equal(t, tc.expectedReason, rayJob.Status.Reason)
		})
	}
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
