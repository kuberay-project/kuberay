package util

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

// RelevantRayClusterCondition returns a RayCluster's most relevant condition
func RelevantRayClusterCondition(cluster rayv1.RayCluster) *metav1.Condition {
	conditions := cluster.Status.Conditions

	if len(conditions) == 0 {
		return nil
	}

	// The order of precedence in which we return conditions is
	// RayClusterProvisioned > HeadPodReady > RayClusterReplicaFailure > Suspended > Suspending.
	if meta.IsStatusConditionTrue(conditions, string(rayv1.RayClusterProvisioned)) {
		return meta.FindStatusCondition(conditions, string(rayv1.RayClusterProvisioned))
	}
	if meta.IsStatusConditionTrue(conditions, string(rayv1.HeadPodReady)) {
		return meta.FindStatusCondition(conditions, string(rayv1.HeadPodReady))
	}
	if meta.IsStatusConditionTrue(conditions, string(rayv1.RayClusterReplicaFailure)) {
		return meta.FindStatusCondition(conditions, string(rayv1.RayClusterReplicaFailure))
	}
	if meta.IsStatusConditionTrue(conditions, string(rayv1.RayClusterSuspended)) {
		return meta.FindStatusCondition(conditions, string(rayv1.RayClusterSuspended))
	}
	if meta.IsStatusConditionTrue(conditions, string(rayv1.RayClusterSuspending)) {
		return meta.FindStatusCondition(conditions, string(rayv1.RayClusterSuspending))
	}

	return nil
}
