package utils

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestAddSchedulerNameToObject(t *testing.T) {
	schedulerName := "test-scheduler"

	t.Run("Pod object should have schedulerName set", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-pod",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{},
		}

		AddSchedulerNameToObject(pod, schedulerName)

		if pod.Spec.SchedulerName != schedulerName {
			t.Errorf("expected schedulerName to be %q, got %q", schedulerName, pod.Spec.SchedulerName)
		}
	})

	t.Run("PodTemplateSpec object should have schedulerName set", func(t *testing.T) {
		podTemplate := &corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-template",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{},
		}

		AddSchedulerNameToObject(podTemplate, schedulerName)

		if podTemplate.Spec.SchedulerName != schedulerName {
			t.Errorf("expected schedulerName to be %q, got %q", schedulerName, podTemplate.Spec.SchedulerName)
		}
	})

	t.Run("RayCluster object should not be modified", func(t *testing.T) {
		// When AddMetadataToChildResource is called with a RayCluster,
		// only the metadata propagation applies. The schedulerName is set later on actual Pods
		// (Head/Worker Pods for RayCluster or submitter Job for RayJob), not on the RayCluster itself.
		// This test validates the intentional silent no-op behavior for unsupported types.
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
								{Name: "test", Image: "test"},
							},
						},
					},
				},
			},
		}

		// Store original state
		originalSchedulerName := rayCluster.Spec.HeadGroupSpec.Template.Spec.SchedulerName

		// This should not panic and should not modify the RayCluster's PodTemplateSpecs
		AddSchedulerNameToObject(rayCluster, schedulerName)

		// Verify the RayCluster's PodTemplateSpec was not modified
		if rayCluster.Spec.HeadGroupSpec.Template.Spec.SchedulerName != originalSchedulerName {
			t.Errorf("RayCluster HeadGroupSpec.Template schedulerName was modified: expected %q, got %q",
				originalSchedulerName, rayCluster.Spec.HeadGroupSpec.Template.Spec.SchedulerName)
		}
	})
}
