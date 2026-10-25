package ray

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/yaml"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestHandleDeletionRulesReportsUnmatchedTerminalStatus(t *testing.T) {
	recorder := events.NewFakeRecorder(1)
	reconciler := &RayJobReconciler{Recorder: recorder}
	jobStatus := rayv1.JobStatusRunning
	ruleJobStatus := rayv1.JobStatusSucceeded
	jobDeploymentStatus := rayv1.JobDeploymentStatusFailed
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayjob-unmatched-rule",
			Namespace: "default",
		},
		Spec: rayv1.RayJobSpec{
			DeletionStrategy: &rayv1.DeletionStrategy{
				DeletionRules: []rayv1.DeletionRule{{
					Condition: rayv1.DeletionCondition{JobStatus: &ruleJobStatus},
					Policy:    rayv1.DeleteCluster,
				}},
			},
		},
		Status: rayv1.RayJobStatus{
			JobStatus:           jobStatus,
			JobDeploymentStatus: jobDeploymentStatus,
		},
	}

	result, err := reconciler.handleDeletionRules(context.Background(), rayJob)
	require.NoError(t, err)
	assert.Zero(t, result.RequeueAfter)

	select {
	case event := <-recorder.Events:
		assert.Contains(t, event, "Warning")
		assert.Contains(t, event, string(utils.NoMatchingDeletionRule))
		assert.Contains(t, event, "jobDeploymentStatus=\"Failed\"")
		assert.Contains(t, event, "No cleanup action will be taken")
	default:
		t.Fatal("expected an unmatched deletion rule warning event, got none")
	}
}

func TestRayJobDeletionRulesSampleRetainsConditionTTLs(t *testing.T) {
	sample, err := os.ReadFile(filepath.Join("..", "..", "config", "samples", "ray-job.deletion-rules.yaml"))
	require.NoError(t, err)

	var rayJob rayv1.RayJob
	require.NoError(t, yaml.Unmarshal(sample, &rayJob))
	require.NotNil(t, rayJob.Spec.DeletionStrategy)
	rules := rayJob.Spec.DeletionStrategy.DeletionRules
	require.Len(t, rules, 6)

	wantTTLSeconds := []int32{30, 60, 90, 30, 60, 90}
	for i, want := range wantTTLSeconds {
		assert.Equalf(t, want, rules[i].Condition.TTLSeconds, "deletion rule %d TTL", i)
	}
}
