package apiserversdk

import (
	"testing"
	"time"

	"github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/api/meta"

	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

var testPollingInterval = 500 * time.Millisecond

func waitForClusterConditions(t *testing.T, tCtx *End2EndTestingContext, clusterName string, expectedConditions []rayv1api.RayClusterConditionType) {
	if len(expectedConditions) == 0 {
		// no expected conditions provided, skip the wait
		return
	}
	// wait for the cluster to be in one of the expected conditions for 1 minutes
	// if it is not in one of those conditions, return an error
	g := gomega.NewWithT(t)
	g.Eventually(func() bool {
		rayCluster, err := tCtx.GetRayClusterByName(clusterName)
		if err != nil {
			t.Logf("Error getting ray cluster '%s': %v", clusterName, err)
			return false
		}
		t.Logf("Waiting for ray cluster '%s' to be in one of the expected conditions %s", clusterName, expectedConditions)
		for _, condition := range expectedConditions {
			if meta.IsStatusConditionTrue(rayCluster.Status.Conditions, string(condition)) {
				t.Logf("Found condition '%s' for ray cluster '%s'", string(condition), clusterName)
				return true
			}
		}
		return false
	}, 3*time.Minute, testPollingInterval).Should(gomega.BeTrue())
}
