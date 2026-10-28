package kaischeduler

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func createTestRayCluster(labels map[string]string) *rayv1.RayCluster {
	return &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			Labels:    labels,
		},
	}
}

func createTestRayJob(labels map[string]string) *rayv1.RayJob {
	return &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
			Labels:    labels,
		},
		Spec: rayv1.RayJobSpec{
			RayClusterSpec: &rayv1.RayClusterSpec{},
		},
	}
}

func createTestPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels: map[string]string{
				"ray.io/cluster":   "test-cluster",
				"ray.io/node-type": "worker",
				"app":              "ray",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "ray-worker",
				Image: "rayproject/ray:latest",
			}},
		},
	}
}

func TestAddMetadataToChildResource_WithQueueLabel(t *testing.T) {
	a := assert.New(t)
	scheduler := &KaiScheduler{}
	ctx := context.Background()

	// Create RayCluster with queue label
	rayCluster := createTestRayCluster(map[string]string{
		QueueLabelName: "test-queue",
	})
	pod := createTestPod()

	// Call AddMetadataToChildResource
	scheduler.AddMetadataToChildResource(ctx, rayCluster, pod, "test-group")

	// Assert scheduler name is set to kai-scheduler
	a.Equal("kai-scheduler", pod.Spec.SchedulerName)

	// Assert queue label is propagated to pod
	a.NotNil(pod.Labels)
	a.Equal("test-queue", pod.Labels[QueueLabelName])
}

func TestAddMetadataToChildResource_WithoutQueueLabel(t *testing.T) {
	a := assert.New(t)
	scheduler := &KaiScheduler{}
	ctx := context.Background()

	// Create RayCluster without queue label
	rayCluster := createTestRayCluster(map[string]string{})
	pod := createTestPod()

	// Call AddMetadataToChildResource
	scheduler.AddMetadataToChildResource(ctx, rayCluster, pod, "test-group")

	// Assert scheduler name is still set (always required)
	a.Equal("kai-scheduler", pod.Spec.SchedulerName)

	// Assert queue label is not added to pod when missing from RayCluster
	if pod.Labels != nil {
		_, exists := pod.Labels[QueueLabelName]
		a.False(exists)
	}
}

func TestAddMetadataToChildResource_WithEmptyQueueLabel(t *testing.T) {
	a := assert.New(t)
	scheduler := &KaiScheduler{}
	ctx := context.Background()

	// Create RayCluster with empty queue label
	rayCluster := createTestRayCluster(map[string]string{
		QueueLabelName: "",
	})
	pod := createTestPod()

	// Call AddMetadataToChildResource
	scheduler.AddMetadataToChildResource(ctx, rayCluster, pod, "test-group")

	// Assert scheduler name is still set
	a.Equal("kai-scheduler", pod.Spec.SchedulerName)

	// Assert empty queue label is treated as missing
	if pod.Labels != nil {
		_, exists := pod.Labels[QueueLabelName]
		a.False(exists)
	}
}

func TestAddMetadataToChildResource_PreservesExistingPodLabels(t *testing.T) {
	a := assert.New(t)
	scheduler := &KaiScheduler{}
	ctx := context.Background()

	// Create RayCluster with queue label
	rayCluster := createTestRayCluster(map[string]string{
		QueueLabelName: "test-queue",
	})

	// Create pod with existing labels
	pod := createTestPod()
	pod.Labels = map[string]string{
		"existing-label": "existing-value",
		"app":            "ray",
	}

	// Call AddMetadataToChildResource
	scheduler.AddMetadataToChildResource(ctx, rayCluster, pod, "test-group")

	// Assert scheduler name is set
	a.Equal("kai-scheduler", pod.Spec.SchedulerName)

	// Assert queue label is added
	a.Equal("test-queue", pod.Labels[QueueLabelName])

	// Assert existing labels are preserved
	a.Equal("existing-value", pod.Labels["existing-label"])
	a.Equal("ray", pod.Labels["app"])
}

func TestAddMetadataToChildResource_WithRayJob(t *testing.T) {
	a := assert.New(t)
	scheduler := &KaiScheduler{}
	ctx := context.Background()

	rayJob := createTestRayJob(map[string]string{
		QueueLabelName: "test-queue",
	})
	pod := createTestPod()

	scheduler.AddMetadataToChildResource(ctx, rayJob, pod, "test-group")

	a.Equal("kai-scheduler", pod.Spec.SchedulerName)

	a.NotNil(pod.Labels)
	a.Equal("test-queue", pod.Labels[QueueLabelName])
}

func TestAddMetadataToChildResource_WithRayJob_WithoutQueueLabel(t *testing.T) {
	a := assert.New(t)
	scheduler := &KaiScheduler{}
	ctx := context.Background()

	rayJob := createTestRayJob(map[string]string{})
	pod := createTestPod()

	scheduler.AddMetadataToChildResource(ctx, rayJob, pod, "test-group")

	a.Equal("kai-scheduler", pod.Spec.SchedulerName)

	if pod.Labels != nil {
		_, exists := pod.Labels[QueueLabelName]
		a.False(exists)
	}
}
