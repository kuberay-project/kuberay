package expectations

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestRayClusterExpectationsHeadPod(t *testing.T) {
	ctx := context.Background()
	// Simulate local Informer with fakeClient.
	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := NewRayClusterScaleExpectation(fakeClient)
	namespace := "default"
	rayClusterName := "raycluster-test"
	testPods := getTestPod()

	// Expect create head pod.
	exp.ExpectScalePod(namespace, rayClusterName, HeadGroup, testPods[0].Name, Create)
	// There is no head pod in Informer, return false.
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
	// Add a pod to the informer. This is used to simulate the informer syncing with the head pod in etcd.
	// In reality, it should be automatically done by the informer.
	err := fakeClient.Create(ctx, &testPods[0])
	require.NoError(t, err, "Fail to create head pod")
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
	// Expect delete head pod.
	exp.ExpectScalePod(namespace, rayClusterName, HeadGroup, testPods[0].Name, Delete)
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
	// Delete head pod from the informer.
	err = fakeClient.Delete(ctx, &testPods[0])
	require.NoError(t, err, "Fail to delete head pod")
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
}

func TestRayClusterExpectationsForSamePod(t *testing.T) {
	ctx := context.Background()
	// Simulate local Informer with fakeClient.
	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := NewRayClusterScaleExpectation(fakeClient)
	namespace := "default"
	rayClusterName := "raycluster-test"
	testPods := getTestPod()

	// Expect the same Pod to be created and deleted.
	exp.ExpectScalePod(namespace, rayClusterName, HeadGroup, testPods[0].Name, Create)
	// Delete, override the expectation for the same Pod
	exp.ExpectScalePod(namespace, rayClusterName, HeadGroup, testPods[0].Name, Delete)
	// There is no pod in the informer. Satisfied. And delete expectation.
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
	err := fakeClient.Create(ctx, &testPods[0])
	require.NoError(t, err, "Fail to create head pod")
	// No expectation
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
	err = fakeClient.Delete(ctx, &testPods[0])
	require.NoError(t, err, "Fail to delete head pod")
	// No expectation
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
}

func TestRayClusterExpectationsWorkerGroupPods(t *testing.T) {
	ctx := context.Background()
	// Simulate local Informer with fakeClient.
	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := NewRayClusterScaleExpectation(fakeClient)
	namespace := "default"
	rayClusterName := "raycluster-test"
	groupA := "test-group-a"
	groupB := "test-group-b"
	testPods := getTestPod()
	// Expect create one worker pod in group-a, two worker pods in group-b.
	exp.ExpectScalePod(namespace, rayClusterName, groupA, testPods[0].Name, Create)
	exp.ExpectScalePod(namespace, rayClusterName, groupB, testPods[1].Name, Create)
	exp.ExpectScalePod(namespace, rayClusterName, groupB, testPods[2].Name, Create)
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupA))
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupB))
	require.NoError(t, fakeClient.Create(ctx, &testPods[1]), "Fail to create worker pod2")
	// All pods within the same group are expected to meet.
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupB))
	require.NoError(t, fakeClient.Create(ctx, &testPods[2]), "Fail to create worker pod3")
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupB))
	// Different groups do not affect each other.
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupA))
	require.NoError(t, fakeClient.Create(ctx, &testPods[0]), "Fail to create worker pod1")
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupA))

	// Expect delete.
	exp.ExpectScalePod(namespace, rayClusterName, groupA, testPods[0].Name, Delete)
	exp.ExpectScalePod(namespace, rayClusterName, groupB, testPods[1].Name, Delete)
	exp.ExpectScalePod(namespace, rayClusterName, groupB, testPods[2].Name, Delete)
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupA))
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupB))
	require.NoError(t, fakeClient.Delete(ctx, &testPods[1]), "Fail to delete worker pod2")
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupB))
	require.NoError(t, fakeClient.Delete(ctx, &testPods[2]), "Fail to delete worker pod3")
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupB))
	// Different groups do not affect each other.
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupA))
	require.NoError(t, fakeClient.Delete(ctx, &testPods[0]), "Fail to delete worker pod1")
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, groupA))
}

func TestRayClusterExpectationsDeleteAll(t *testing.T) {
	ctx := context.Background()
	// Simulate local Informer with fakeClient.
	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := NewRayClusterScaleExpectation(fakeClient)
	namespace := "default"
	rayClusterName := "raycluster-test"
	group := "test-group"
	testPods := getTestPod()
	exp.ExpectScalePod(namespace, rayClusterName, HeadGroup, testPods[0].Name, Create)
	exp.ExpectScalePod(namespace, rayClusterName, group, testPods[1].Name, Create)
	exp.ExpectScalePod(namespace, rayClusterName, group, testPods[2].Name, Delete)
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, group))
	// Delete all expectations
	exp.Delete(rayClusterName, namespace)
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, group))
}

func TestRayClusterExpectationsTimeout(t *testing.T) {
	ctx := context.Background()
	// Reduce the timeout duration so that tests don't have to wait for a long time.
	ExpectationsTimeout = 1 * time.Second
	// Simulate local Informer with fakeClient.
	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := NewRayClusterScaleExpectation(fakeClient)
	namespace := "default"
	rayClusterName := "raycluster-test"
	testPods := getTestPod()

	exp.ExpectScalePod(namespace, rayClusterName, HeadGroup, testPods[0].Name, Create)
	assert.False(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
	// Expectations should be released after timeout.
	time.Sleep(ExpectationsTimeout + 1*time.Second)
	assert.True(t, exp.IsSatisfied(ctx, namespace, rayClusterName, HeadGroup))
}

func getTestPod() []corev1.Pod {
	return []corev1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod1",
				Namespace: "default",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod2",
				Namespace: "default",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod3",
				Namespace: "default",
			},
		},
	}
}

func TestIsPodScaled(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		setupFunc      func(client.Client, *corev1.Pod)
		action         ScaleAction
		name           string
		expectedResult bool
	}{
		{
			name:           "Create action - pod exists",
			action:         Create,
			expectedResult: true,
			setupFunc: func(client client.Client, pod *corev1.Pod) {
				err := client.Create(ctx, pod)
				require.NoError(t, err)
			},
		},
		{
			name:           "Create action - pod does not exist",
			action:         Create,
			expectedResult: false,
			setupFunc:      func(_ client.Client, _ *corev1.Pod) {},
		},
		{
			name:           "Delete action - pod exists",
			action:         Delete,
			expectedResult: false,
			setupFunc: func(client client.Client, pod *corev1.Pod) {
				err := client.Create(ctx, pod)
				require.NoError(t, err)
			},
		},
		{
			name:           "Delete action - pod does not exist",
			action:         Delete,
			expectedResult: true,
			setupFunc:      func(_ client.Client, _ *corev1.Pod) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
			exp := &rayClusterScaleExpectationImpl{
				Client:     fakeClient,
				itemsCache: nil, // Not used in isPodScaled
			}

			testPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
			}

			rp := &rayPod{
				name:            testPod.Name,
				namespace:       testPod.Namespace,
				action:          tt.action,
				recordTimestamp: time.Now(),
			}

			tt.setupFunc(fakeClient, testPod)

			result := exp.isPodScaled(ctx, rp)
			assert.Equal(t, tt.expectedResult, result)
		})
	}
}

func TestIsPodScaledTimeout(t *testing.T) {
	ctx := context.Background()

	originalTimeout := ExpectationsTimeout
	ExpectationsTimeout = time.Hour
	defer func() { ExpectationsTimeout = originalTimeout }()

	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects().Build()
	exp := &rayClusterScaleExpectationImpl{
		Client:     fakeClient,
		itemsCache: nil,
	}

	testPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
		},
	}

	rp := &rayPod{
		name:            testPod.Name,
		namespace:       testPod.Namespace,
		action:          Create,
		recordTimestamp: time.Now(),
	}

	result := exp.isPodScaled(ctx, rp)
	assert.False(t, result)

	rp.recordTimestamp = time.Now().Add(-ExpectationsTimeout - time.Second)
	result = exp.isPodScaled(ctx, rp)
	assert.True(t, result)
}

// TestIsPodScaledDeleteTimeout verifies that a Delete expectation whose Pod is stuck
// (never actually deleted, e.g. its node is NotReady so the Pod keeps a deletionTimestamp
// that never clears) is released after the timeout instead of blocking forever.
// Without this, IsSatisfied short-circuits on the unsatisfied Delete expectation and
// freezes all scaling for the entire worker group indefinitely.
func TestIsPodScaledDeleteTimeout(t *testing.T) {
	ctx := context.Background()

	originalTimeout := ExpectationsTimeout
	ExpectationsTimeout = time.Hour
	defer func() { ExpectationsTimeout = originalTimeout }()

	deletionTimestamp := metav1.Now()
	testPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "stuck-pod",
			Namespace:         "default",
			DeletionTimestamp: &deletionTimestamp,
			Finalizers:        []string{"test-finalizer"},
		},
	}

	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects(testPod).Build()
	storedPod := &corev1.Pod{}
	require.NoError(t, fakeClient.Get(ctx, client.ObjectKeyFromObject(testPod), storedPod))
	require.NotNil(t, storedPod.DeletionTimestamp)

	exp := &rayClusterScaleExpectationImpl{
		Client:     fakeClient,
		itemsCache: nil,
	}

	rp := &rayPod{
		name:            testPod.Name,
		namespace:       testPod.Namespace,
		action:          Delete,
		recordTimestamp: time.Now(),
	}

	assert.False(t, exp.isPodScaled(ctx, rp))

	rp.recordTimestamp = time.Now().Add(-ExpectationsTimeout - time.Second)
	assert.True(t, exp.isPodScaled(ctx, rp))
}

func TestIsPodScaledDeleteDoesNotTimeoutBeforeDeletion(t *testing.T) {
	ctx := context.Background()

	originalTimeout := ExpectationsTimeout
	ExpectationsTimeout = time.Hour
	defer func() { ExpectationsTimeout = originalTimeout }()

	testPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-pod",
			Namespace: "default",
		},
	}

	fakeClient := clientFake.NewClientBuilder().WithRuntimeObjects(testPod).Build()
	exp := &rayClusterScaleExpectationImpl{
		Client:     fakeClient,
		itemsCache: nil,
	}

	rp := &rayPod{
		name:            testPod.Name,
		namespace:       testPod.Namespace,
		action:          Delete,
		recordTimestamp: time.Now().Add(-ExpectationsTimeout - time.Second),
	}

	assert.False(t, exp.isPodScaled(ctx, rp))
}
