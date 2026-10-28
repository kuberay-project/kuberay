package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

func TestRelevantRayClusterCondition(t *testing.T) {
	provisionedCondition := metav1.Condition{
		Type:   string(rayv1.RayClusterProvisioned),
		Status: metav1.ConditionTrue,
	}
	headPodReadyCondition := metav1.Condition{
		Type:   string(rayv1.HeadPodReady),
		Status: metav1.ConditionTrue,
	}
	replicaFailureCondition := metav1.Condition{
		Type:   string(rayv1.RayClusterReplicaFailure),
		Status: metav1.ConditionTrue,
	}
	suspendedCondition := metav1.Condition{
		Type:   string(rayv1.RayClusterSuspended),
		Status: metav1.ConditionTrue,
	}
	suspendingCondition := metav1.Condition{
		Type:   string(rayv1.RayClusterSuspending),
		Status: metav1.ConditionTrue,
	}

	tests := []struct {
		expectedCondition *metav1.Condition
		name              string
		conditions        []metav1.Condition
	}{
		{
			name:              "passing no conditions should return nil pointer",
			conditions:        []metav1.Condition{},
			expectedCondition: nil,
		},
		{
			name:              "RayClusterProvisioned precedence",
			conditions:        []metav1.Condition{suspendingCondition, suspendedCondition, replicaFailureCondition, headPodReadyCondition, provisionedCondition},
			expectedCondition: &provisionedCondition,
		},
		{
			name:              "HeadPodReady precedence",
			conditions:        []metav1.Condition{suspendingCondition, suspendedCondition, replicaFailureCondition, headPodReadyCondition},
			expectedCondition: &headPodReadyCondition,
		},
		{
			name:              "RayClusterReplicaFailure precedence",
			conditions:        []metav1.Condition{suspendingCondition, suspendedCondition, replicaFailureCondition},
			expectedCondition: &replicaFailureCondition,
		},
		{
			name:              "RayClusterSuspended precedence",
			conditions:        []metav1.Condition{suspendingCondition, suspendedCondition},
			expectedCondition: &suspendedCondition,
		},
		{
			name:              "RayClusterSuspending precedence",
			conditions:        []metav1.Condition{suspendingCondition},
			expectedCondition: &suspendingCondition,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expectedCondition, RelevantRayClusterCondition(rayv1.RayCluster{Status: rayv1.RayClusterStatus{Conditions: tc.conditions}}))
		})
	}
}
