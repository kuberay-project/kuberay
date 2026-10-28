package utils

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AddSchedulerNameToObject sets the schedulerName field on Pod and PodTemplateSpec resources.
// Used to assign batch scheduler names to:
//   - Head pod and worker pod in RayCluster
//   - Job in RayJob
func AddSchedulerNameToObject(obj metav1.Object, schedulerName string) {
	switch obj := obj.(type) {
	case *corev1.Pod:
		obj.Spec.SchedulerName = schedulerName
	case *corev1.PodTemplateSpec:
		obj.Spec.SchedulerName = schedulerName
	}
}
