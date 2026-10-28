package e2e

import (
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/api/meta"

	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

//go:embed resources/*.py
var files embed.FS

// Refer to https://github.com/ray-project/kuberay/pull/3455 for more info how we observe the right amount resource for e2e test
var (
	TestTimeoutShort         = 1 * time.Minute
	TestTimeoutMedium        = 3 * time.Minute
	TestTimeoutLong          = 5 * time.Minute
	TestPollingInterval      = 500 * time.Millisecond
	ComputeTemplateCPUForE2E = uint32(1) // CPU core
	CompTemplateMemGiBForE2E = uint32(4)
)

// CreateHttpRequest instantiates a http request for the specified endpoint and host
func CreateHttpRequest(method string, host string, endPoint string, body io.Reader) (*http.Request, error) {
	url := host + endPoint
	req, err := http.NewRequestWithContext(context.TODO(), method, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Accept", "application/json")
	req.Header.Add("Content-Type", "application/json")
	return req, nil
}

// MakeBodyReader creates an io.Reader from the supplied string if is not empty after
// trimming the spaces
func MakeBodyReader(s string) io.Reader {
	if strings.TrimSpace(s) != "" {
		return strings.NewReader(s)
	}
	return nil
}

// PrettyPrintResponseBody generates a "pretty" formatted JSON string from the body
func PrettyPrintResponseBody(body io.ReadCloser) (string, error) {
	inputBytez, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, inputBytez, "", "\t"); err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
}

func ReadFileAsString(t *testing.T, fileName string) string {
	file, err := files.ReadFile(fileName)
	require.NoErrorf(t, err, "No error expected when reading embedded file: '%s'", fileName)
	return string(file)
}

// waitForClusterConditions waits for the cluster to be in one of the expected conditions
// if no expected conditions are provided, it skips the wait
func waitForClusterConditions(t *testing.T, tCtx *End2EndTestingContext, clusterName string, expectedConditions []rayv1api.RayClusterConditionType) {
	if len(expectedConditions) == 0 {
		// no expected conditions provided, skip the wait
		return
	}
	// wait for the cluster to be in one of the expected conditions for 3 minutes
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
	}, TestTimeoutMedium, TestPollingInterval).Should(gomega.BeTrue())
}

func waitForRunningCluster(t *testing.T, tCtx *End2EndTestingContext, clusterName string) {
	waitForClusterConditions(t, tCtx, clusterName, []rayv1api.RayClusterConditionType{rayv1api.RayClusterProvisioned})
}

func waitForClusterToDisappear(t *testing.T, tCtx *End2EndTestingContext, clusterName string) {
	// wait for the cluster to disappear
	g := gomega.NewWithT(t)
	g.Eventually(func() bool {
		_, err := tCtx.GetRayClusterByName(clusterName)
		if err != nil && strings.Contains(err.Error(), "rayclusters.ray.io \""+tCtx.GetRayClusterName()+"\" not found") {
			return true
		}
		t.Logf("Found ray cluster '%s'", clusterName)
		return false
	}, TestTimeoutMedium, TestPollingInterval).Should(gomega.BeTrue())
}

func waitForRayJobInExpectedStatuses(t *testing.T, tCtx *End2EndTestingContext, rayJobName string, expectedJobStatuses []rayv1api.JobStatus) {
	// `expectedJobStatuses` is a slice of job statuses that we expect the job to be in
	// wait for the job to be in any of the `expectedJobStatuses` state for 3 minutes
	// if is not in that state, return an error
	g := gomega.NewWithT(t)

	g.Eventually(func() bool {
		rayJob, err := tCtx.GetRayJobByName(rayJobName)
		if err != nil {
			t.Logf("Error getting ray job '%s': %v", rayJobName, err)
			return false
		}
		t.Logf("Waiting for ray job '%s' to be in one of the expected statuses %s", rayJobName, expectedJobStatuses)
		return slices.Contains(expectedJobStatuses, rayJob.Status.JobStatus)
	}, TestTimeoutMedium, TestPollingInterval).Should(gomega.BeTrue())
}

func waitForRayJobToDisappear(t *testing.T, tCtx *End2EndTestingContext, jobName string) {
	// wait for the job to disappear
	// if is not in that state, return an error
	g := gomega.NewWithT(t)
	t.Logf("Starting to wait for ray job %s to be deleted", jobName)
	g.Eventually(func() error {
		rayJob, err := tCtx.GetRayJobByName(jobName)
		if err != nil {
			return err
		}
		t.Logf("Find rayJob with status %v", rayJob.Status.JobStatus)
		return nil
	}, TestTimeoutMedium, TestPollingInterval).Should(gomega.MatchError("rayjobs.ray.io \"" + jobName + "\" not found"))

	t.Logf("Ray job %s successfully deleted", jobName)
}

func waitForServiceToDisappear(t *testing.T, tCtx *End2EndTestingContext, serviceName string) {
	g := gomega.NewWithT(t)
	t.Logf("Starting to wait for service %s to be deleted", serviceName)
	g.Eventually(func() error {
		rayService, err := tCtx.GetRayServiceByName(serviceName)
		if err != nil {
			return err
		}
		t.Logf("Find rayService with status %v", rayService.Status)
		return nil
	}, TestTimeoutMedium, TestPollingInterval).Should(gomega.MatchError("rayservices.ray.io \"" + serviceName + "\" not found"))

	t.Logf("Service %s successfully deleted", serviceName)
}

func clusterSpecEqual(expected, actual *api.ClusterSpec) bool {
	if expected == nil || actual == nil {
		return expected == nil && actual == nil
	}
	// Since default environment variables are added in buildRayClusterSpec but omitted during CRD-to-API conversion,
	// an empty variable may appear in the spec even if the user didn't set it.
	if expected.GetHeadGroupSpec() == nil {
		expected.HeadGroupSpec = &api.HeadGroupSpec{}
	}
	if expected.GetHeadGroupSpec().GetEnvironment() == nil {
		expected.GetHeadGroupSpec().Environment = &api.EnvironmentVariables{}
	}
	for _, wg := range expected.GetWorkerGroupSpec() {
		if wg.Environment == nil {
			wg.Environment = &api.EnvironmentVariables{}
		}
	}
	return proto.Equal(expected, actual)
}

func serviceSpecEqual(expected, actual *api.RayService) bool {
	if !clusterSpecEqual(expected.GetClusterSpec(), actual.GetClusterSpec()) {
		return false
	}
	expectedCopy := proto.Clone(expected).(*api.RayService)
	actualCopy := proto.Clone(actual).(*api.RayService)
	// Clear fields that are not relevant for equality check
	expectedCopy.ClusterSpec, actualCopy.ClusterSpec = nil, nil
	expectedCopy.RayServiceStatus, actualCopy.RayServiceStatus = nil, nil
	expectedCopy.CreatedAt, actualCopy.CreatedAt = nil, nil
	expectedCopy.DeleteAt, actualCopy.DeleteAt = nil, nil
	return proto.Equal(expectedCopy, actualCopy)
}

func jobSpecEqual(expected, actual *api.RayJob) bool {
	if !clusterSpecEqual(expected.GetClusterSpec(), actual.GetClusterSpec()) {
		return false
	}
	expectedCopy := proto.Clone(expected).(*api.RayJob)
	actualCopy := proto.Clone(actual).(*api.RayJob)
	// Clear fields that are not relevant for equality check
	expectedCopy.ClusterSpec, actualCopy.ClusterSpec = nil, nil
	expectedCopy.CreatedAt, actualCopy.CreatedAt = nil, nil
	expectedCopy.DeleteAt, actualCopy.DeleteAt = nil, nil
	expectedCopy.JobStatus, actualCopy.JobStatus = "", ""
	expectedCopy.JobDeploymentStatus, actualCopy.JobDeploymentStatus = "", ""
	expectedCopy.Message, actualCopy.Message = "", ""
	expectedCopy.StartTime, actualCopy.StartTime = nil, nil
	expectedCopy.EndTime, actualCopy.EndTime = nil, nil
	expectedCopy.RayClusterName, actualCopy.RayClusterName = "", ""
	// Version will be ignored if cluster spec not provided
	expectedCopy.Version, actualCopy.Version = "", ""
	return proto.Equal(expectedCopy, actualCopy)
}
