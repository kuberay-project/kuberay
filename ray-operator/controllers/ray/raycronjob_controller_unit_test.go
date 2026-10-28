package ray

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

//nolint:unparam // namespace parameter kept for flexibility in future tests
func rayCronJobTemplate(name string, namespace string, schedule string) *rayv1.RayCronJob {
	return &rayv1.RayCronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: rayv1.RayCronJobSpec{
			Schedule: schedule,
			JobTemplate: rayv1.RayJobSpec{
				Entrypoint: "python test.py",
				RayClusterSpec: &rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Name:  "ray-head",
										Image: "rayproject/ray:2.52.0",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestRayCronJobReconcile_InvalidSchedule(t *testing.T) {
	ctx := context.Background()

	// Create RayCronJob with invalid cron schedule
	rayCronJob := rayCronJobTemplate("invalid-cronjob", "default", "invalid cron string")

	// Create scheme and add types
	scheme := runtime.NewScheme()
	err := rayv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = corev1.AddToScheme(scheme)
	require.NoError(t, err)

	// Create fake client
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rayCronJob).
		Build()

	// Create fake event recorder with a channel to capture events
	fakeRecorder := events.NewFakeRecorder(10)

	// Create reconciler
	reconciler := &RayCronJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
		clock:    clocktesting.NewFakeClock(time.Time{}),
	}

	// Reconcile
	result, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "invalid-cronjob",
			Namespace: "default",
		},
	})

	// Should return no error as validation errors are recorded as events
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	// Verify that a validation error event was recorded
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, "Warning")
		assert.Contains(t, event, "invalid cron schedule")
	default:
		t.Error("Expected a validation error event to be recorded, but none was found")
	}
}

func TestRayCronJobReconcile_FirstSchedule(t *testing.T) {
	ctx := context.Background()

	// Set up test parameters
	cronSchedule := "*/5 * * * *" // Every 5 minutes
	// creationTime=00:00, fakeCurrTime=00:05:30; scheduleTime=Next(00:00)=00:05 ≤ now -> job created
	// nextScheduleTime=Next(00:05)=00:10, requeueAt=00:10-00:05:30=4m30s (positive)
	fakeCurrTime := time.Date(2024, 1, 1, 0, 5, 30, 0, time.UTC)
	creationTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create valid RayCronJob
	rayCronJob := rayCronJobTemplate("test-cronjob", "default", cronSchedule)
	rayCronJob.CreationTimestamp = metav1.NewTime(creationTime)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	err := rayv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = corev1.AddToScheme(scheme)
	require.NoError(t, err)

	// Create fake client with status subresource
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rayCronJob).
		WithStatusSubresource(rayCronJob).
		Build()

	// Create fake clock for deterministic testing
	fakeClock := clocktesting.NewFakeClock(fakeCurrTime)

	// Create reconciler
	reconciler := &RayCronJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: &events.FakeRecorder{},
		clock:    fakeClock,
	}

	// Reconcile
	result, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-cronjob",
			Namespace: "default",
		},
	})

	// Should succeed with a requeue time
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter, "Should requeue for next schedule")

	// scheduleTime = Next(creationTime) = 00:05; nextScheduleTime = Next(scheduleTime) = 00:10
	parsedSchedule, err := cron.ParseStandard(cronSchedule)
	require.NoError(t, err, "Test cron schedule should be valid")
	expectedScheduleTime := parsedSchedule.Next(creationTime)
	expectedNextSchedule := parsedSchedule.Next(expectedScheduleTime)
	expectedRequeueAfter := expectedNextSchedule.Sub(fakeCurrTime)

	// Verify the requeue time is Next(scheduleTime) - now, keeping ticks aligned
	assert.Equal(t, expectedRequeueAfter, result.RequeueAfter,
		"RequeueAfter should be Next(scheduleTime) - now so the controller wakes at the next tick boundary")

	// Check that LastScheduleTime was set to the scheduleTime
	updatedCronJob := &rayv1.RayCronJob{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "test-cronjob",
		Namespace: "default",
	}, updatedCronJob)
	require.NoError(t, err)
	assert.NotNil(t, updatedCronJob.Status.LastScheduleTime)
	assert.True(t, expectedScheduleTime.Equal(updatedCronJob.Status.LastScheduleTime.Time),
		"LastScheduleTime should be set to the scheduleTime. Expected: %v, Got: %v",
		expectedScheduleTime, updatedCronJob.Status.LastScheduleTime.Time)
}

func TestRayCronJobReconcile_CreateRayJob(t *testing.T) {
	ctx := context.Background()

	// Set up test parameters
	cronSchedule := "*/5 * * * *" // Every 5 minutes
	// lastScheduleTime=00:00, now=00:05:30; scheduleTime=Next(00:00)=00:05 ≤ now -> job created
	// nextScheduleTime=Next(00:05)=00:10, requeueAt=00:10-00:05:30=4m30s (positive)
	fakeCurrTime := time.Date(2024, 1, 1, 0, 5, 30, 0, time.UTC)
	lastScheduleTime := metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	rayCronJob := rayCronJobTemplate("test-cronjob", "default", cronSchedule)
	rayCronJob.Status = rayv1.RayCronJobStatus{
		LastScheduleTime: &lastScheduleTime,
	}

	// Create scheme and add types
	scheme := runtime.NewScheme()
	err := rayv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = corev1.AddToScheme(scheme)
	require.NoError(t, err)

	// Create fake client
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rayCronJob).
		WithStatusSubresource(rayCronJob).
		Build()

	// Create fake clock set to a time that should trigger job creation
	fakeClock := clocktesting.NewFakeClock(fakeCurrTime)

	// Create reconciler
	reconciler := &RayCronJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: &events.FakeRecorder{},
		clock:    fakeClock,
	}

	// Reconcile
	result, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-cronjob",
			Namespace: "default",
		},
	})

	// Should succeed with a requeue time
	require.NoError(t, err)
	assert.Positive(t, result.RequeueAfter, "Should requeue for next schedule")

	// scheduleTime=Next(lastScheduleTime)=00:05; nextScheduleTime=Next(scheduleTime)=00:10
	parsedSchedule, err := cron.ParseStandard(cronSchedule)
	require.NoError(t, err, "Test cron schedule should be valid")
	expectedScheduleTime := parsedSchedule.Next(lastScheduleTime.Time)
	expectedNextSchedule := parsedSchedule.Next(expectedScheduleTime)
	expectedRequeueAfter := expectedNextSchedule.Sub(fakeCurrTime)

	// Verify the requeue time is Next(scheduleTime) - now
	assert.Equal(t, expectedRequeueAfter, result.RequeueAfter,
		"RequeueAfter should be Next(scheduleTime) - now")

	// Verify a RayJob was created
	rayJobList := &rayv1.RayJobList{}
	err = fakeClient.List(ctx, rayJobList)
	require.NoError(t, err)
	assert.Len(t, rayJobList.Items, 1, "Should have created one RayJob")

	// Verify RayJob has correct label
	rayJob := rayJobList.Items[0]
	assert.Equal(t, "test-cronjob", rayJob.Labels["ray.io/cronjob-name"])
	assert.Equal(t, "python test.py", rayJob.Spec.Entrypoint)

	// Verify LastScheduleTime was updated to scheduleTime
	updatedCronJob := &rayv1.RayCronJob{}
	err = fakeClient.Get(ctx, types.NamespacedName{
		Name:      "test-cronjob",
		Namespace: "default",
	}, updatedCronJob)
	require.NoError(t, err)
	assert.NotNil(t, updatedCronJob.Status.LastScheduleTime)
	assert.True(t, expectedScheduleTime.Equal(updatedCronJob.Status.LastScheduleTime.Time),
		"LastScheduleTime should be set to scheduleTime. Expected: %v, Got: %v",
		expectedScheduleTime, updatedCronJob.Status.LastScheduleTime.Time)
}

func TestRayCronJobReconcile_Suspend(t *testing.T) {
	ctx := context.Background()

	// Create RayCronJob with suspend=true
	rayCronJob := rayCronJobTemplate("suspended-cronjob", "default", "*/5 * * * *")
	rayCronJob.Spec.Suspend = new(true)

	// Create scheme and add types
	scheme := runtime.NewScheme()
	err := rayv1.AddToScheme(scheme)
	require.NoError(t, err)
	err = corev1.AddToScheme(scheme)
	require.NoError(t, err)

	// Create fake client
	fakeClient := clientFake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rayCronJob).
		Build()

	// Create fake event recorder with a channel to capture events
	fakeRecorder := events.NewFakeRecorder(10)

	// Create reconciler
	reconciler := &RayCronJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
		clock:    clocktesting.NewFakeClock(time.Time{}),
	}

	// Reconcile
	result, err := reconciler.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "suspended-cronjob",
			Namespace: "default",
		},
	})

	// Should return no error and no requeue
	require.NoError(t, err)
	assert.Equal(t, ctrl.Result{}, result)

	// Verify no RayJob was created
	rayJobList := &rayv1.RayJobList{}
	err = fakeClient.List(ctx, rayJobList)
	require.NoError(t, err)
	assert.Empty(t, rayJobList.Items, "Should not create RayJob when suspended")

	// Verify that a suspend event was recorded
	select {
	case event := <-fakeRecorder.Events:
		assert.Contains(t, event, corev1.EventTypeNormal)
		assert.Contains(t, event, string(utils.SuspendedRayCronJob))
		assert.Contains(t, event, "RayCronJob suspended, no new RayJobs will be created")
	default:
		t.Error("Expected a suspend event to be recorded, but none was found")
	}
}

func TestGetRayJobName_Deterministic(t *testing.T) {
	tick := time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)

	// The suffix is the whole-minute Unix timestamp, so the same name + tick always yields
	// the same child name. A duplicate Create for the same tick then collides (AlreadyExists)
	// instead of producing a second RayJob.
	name := getRayJobName("test-cronjob", tick)
	assert.Equal(t, fmt.Sprintf("test-cronjob-%d", tick.Unix()/60), name)

	// Different ticks must yield different names so consecutive scheduled runs don't collide.
	nextTick := time.Date(2024, 1, 1, 0, 10, 0, 0, time.UTC)
	assert.NotEqual(t, name, getRayJobName("test-cronjob", nextTick))
}

// TestRayCronJobReconcile_NoDuplicateOnStaleStatus reproduces the race in
// https://github.com/ray-project/kuberay/issues/4849: the controller reconciles the same
// scheduled tick twice before the LastScheduleTime status write is observed (informer-cache
// lag). The second pass reads a stale LastScheduleTime, passes the schedule gate, and must
// NOT create a second child RayJob for the same tick.
func TestRayCronJobReconcile_NoDuplicateOnStaleStatus(t *testing.T) {
	ctx := context.Background()

	cronSchedule := "*/5 * * * *" // Every 5 minutes
	staleLastSchedule := metav1.NewTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	// now is just past the 00:05:00 tick so the tick is due in both passes.
	fakeCurrTime := time.Date(2024, 1, 1, 0, 5, 30, 0, time.UTC)

	rayCronJob := rayCronJobTemplate("test-cronjob", "default", cronSchedule)
	rayCronJob.Status = rayv1.RayCronJobStatus{LastScheduleTime: &staleLastSchedule}

	scheme := runtime.NewScheme()
	require.NoError(t, rayv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	fakeClient := clientFake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(rayCronJob).
		WithStatusSubresource(rayCronJob).
		Build()

	reconciler := &RayCronJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: &events.FakeRecorder{},
		clock:    clocktesting.NewFakeClock(fakeCurrTime),
	}

	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: "test-cronjob", Namespace: "default"}}

	// Pass 1: schedules the tick and (in the real controller) writes LastScheduleTime.
	_, err := reconciler.Reconcile(ctx, request)
	require.NoError(t, err)

	// Simulate informer-cache lag: pass 1's status write is not yet observable, so reset
	// LastScheduleTime back to its stale value before pass 2 reads it.
	cronJobForReset := &rayv1.RayCronJob{}
	require.NoError(t, fakeClient.Get(ctx, request.NamespacedName, cronJobForReset))
	cronJobForReset.Status.LastScheduleTime = &staleLastSchedule
	require.NoError(t, fakeClient.Status().Update(ctx, cronJobForReset))

	// Pass 2: reads the stale LastScheduleTime and passes the schedule gate again.
	_, err = reconciler.Reconcile(ctx, request)
	require.NoError(t, err)

	rayJobList := &rayv1.RayJobList{}
	require.NoError(t, fakeClient.List(ctx, rayJobList))
	assert.Len(t, rayJobList.Items, 1, "the same scheduled tick must produce exactly one child RayJob")
}

func TestUpdateRayCronJobStatus(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		oldCronJob   *rayv1.RayCronJob
		newCronJob   *rayv1.RayCronJob
		name         string
		shouldUpdate bool
	}{
		{
			name: "LastScheduleTime changed",
			oldCronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Status: rayv1.RayCronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
				},
			},
			newCronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Status: rayv1.RayCronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Date(2024, 1, 1, 0, 5, 0, 0, time.UTC)},
				},
			},
			shouldUpdate: true,
		},
		{
			name: "LastScheduleTime unchanged",
			oldCronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Status: rayv1.RayCronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
				},
			},
			newCronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Status: rayv1.RayCronJobStatus{
					LastScheduleTime: &metav1.Time{Time: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
				},
			},
			shouldUpdate: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Create scheme
			scheme := runtime.NewScheme()
			err := rayv1.AddToScheme(scheme)
			require.NoError(t, err)

			// Create fake client - start with old status
			initialCronJob := tc.oldCronJob.DeepCopy()
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(scheme).
				WithRuntimeObjects(initialCronJob).
				WithStatusSubresource(initialCronJob).
				Build()

			// Create reconciler
			reconciler := &RayCronJobReconciler{
				Client:   fakeClient,
				Scheme:   scheme,
				Recorder: &events.FakeRecorder{},
			}

			// Fetch the current object from the client to get valid resourceVersion
			currentCronJob := &rayv1.RayCronJob{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      "test-cronjob",
				Namespace: "default",
			}, currentCronJob)
			require.NoError(t, err)

			// Update the status of the fetched object to match the new status
			currentCronJob.Status = tc.newCronJob.Status

			// Call updateRayCronJobStatus with the fetched object that has valid resourceVersion
			err = reconciler.updateRayCronJobStatus(ctx, tc.oldCronJob, currentCronJob)
			require.NoError(t, err)

			updatedCronJob := &rayv1.RayCronJob{}
			err = fakeClient.Get(ctx, types.NamespacedName{
				Name:      "test-cronjob",
				Namespace: "default",
			}, updatedCronJob)
			require.NoError(t, err)
			assert.NotNil(t, updatedCronJob.Status.LastScheduleTime)

			if tc.shouldUpdate {
				// Verify the status was updated to the new value
				assert.True(t, tc.newCronJob.Status.LastScheduleTime.Time.Equal(updatedCronJob.Status.LastScheduleTime.Time),
					"Status should be updated to new LastScheduleTime. Expected: %v, Got: %v",
					tc.newCronJob.Status.LastScheduleTime.Time, updatedCronJob.Status.LastScheduleTime.Time)
			} else {
				// Verify the status remained unchanged
				assert.True(t, tc.oldCronJob.Status.LastScheduleTime.Time.Equal(updatedCronJob.Status.LastScheduleTime.Time),
					"Status should remain unchanged. Expected: %v, Got: %v",
					tc.oldCronJob.Status.LastScheduleTime.Time, updatedCronJob.Status.LastScheduleTime.Time)
			}
		})
	}
}

func TestFormatSchedule(t *testing.T) {
	tz := func(s string) *string { return &s }
	tests := []struct {
		name     string
		schedule string
		timeZone *string
		expected string
	}{
		{
			name:     "no timezone",
			schedule: "*/5 * * * *",
			timeZone: nil,
			expected: "*/5 * * * *",
		},
		{
			name:     "with timezone",
			schedule: "0 9 * * *",
			timeZone: tz("Asia/Taipei"),
			expected: "TZ=Asia/Taipei 0 9 * * *",
		},
		{
			name:     "with UTC timezone",
			schedule: "0 0 * * *",
			timeZone: tz("UTC"),
			expected: "TZ=UTC 0 0 * * *",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cronJob := rayCronJobTemplate("test", "default", tc.schedule)
			cronJob.Spec.TimeZone = tc.timeZone
			assert.Equal(t, tc.expected, formatSchedule(cronJob))
		})
	}
}
