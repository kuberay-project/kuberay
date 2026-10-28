package utils

import (
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestValidateRayJobIndependentRetention(t *testing.T) {
	for _, tc := range []struct {
		name        string
		policy      rayv1.DeletionPolicyType
		retention   int32
		featureGate bool
		wantError   string
	}{
		{name: "normal retention", policy: rayv1.DeleteSelf, retention: 2592000, featureGate: true},
		{name: "test retention", policy: rayv1.DeleteSelf, retention: 604800, featureGate: true},
		{name: "equal deadlines", policy: rayv1.DeleteSelf, retention: 300, featureGate: true},
		{name: "retention before cluster cleanup", policy: rayv1.DeleteSelf, retention: 299, featureGate: true, wantError: "DeleteSelf TTLSeconds must be greater than or equal to ttlSecondsAfterFinished"},
		{name: "negative retention", policy: rayv1.DeleteSelf, retention: -1, featureGate: true, wantError: "TTLSeconds must be non-negative"},
		{name: "cluster rule", policy: rayv1.DeleteCluster, retention: 600, featureGate: true, wantError: "only DeleteSelf is supported"},
		{name: "worker rule", policy: rayv1.DeleteWorkers, retention: 600, featureGate: true, wantError: "only DeleteSelf is supported"},
		{name: "no-op rule", policy: rayv1.DeleteNone, retention: 600, featureGate: true, wantError: "only DeleteSelf is supported"},
		{name: "feature disabled", policy: rayv1.DeleteSelf, retention: 600, wantError: "RayJobDeletionPolicy feature gate must be enabled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, tc.featureGate)
			job := &rayv1.RayJob{Spec: rayv1.RayJobSpec{
				Suspend:                  true,
				ShutdownAfterJobFinishes: true,
				TTLSecondsAfterFinished:  300,
				RayClusterSpec:           createBasicRayClusterSpec(),
				DeletionStrategy: &rayv1.DeletionStrategy{DeletionRules: []rayv1.DeletionRule{
					{Policy: tc.policy, Condition: rayv1.DeletionCondition{JobStatus: ptr.To(rayv1.JobStatusSucceeded), TTLSeconds: tc.retention}},
					{Policy: rayv1.DeleteSelf, Condition: rayv1.DeletionCondition{JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed), TTLSeconds: tc.retention}},
				}},
			}}
			for _, suspend := range []bool{true, false} {
				job.Spec.Suspend = suspend
				err := ValidateRayJobSpec(job)
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
				} else {
					require.NoError(t, err)
				}
			}
		})
	}
}
