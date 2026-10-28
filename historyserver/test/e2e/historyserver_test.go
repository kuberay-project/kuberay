package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// ansiEscapePattern matches ANSI escape sequences (same pattern as in reader.go)
// Pattern: \x1b\[[0-9;]+m
var ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-9;]+m`)

func TestHistoryServer(t *testing.T) {
	// Share a single S3 client among subtests.
	s3Client := EnsureS3Client(t)

	tests := []struct {
		name     string
		testFunc func(Test, *WithT, *corev1.Namespace, *S3TestClient)
	}{
		{
			name:     "Live cluster: historyserver endpoints should be accessible",
			testFunc: testLiveClusters,
		},
		{
			name:     "Live cluster: grafana health only",
			testFunc: testLiveGrafanaHealth,
		},
		{
			name:     "Live cluster: prometheus health only",
			testFunc: testLivePrometheusHealth,
		},
		{
			name:     "/v0/logs/file endpoint (live cluster)",
			testFunc: testLogFileEndpointLiveCluster,
		},
		{
			name:     "/v0/logs/file endpoint (dead cluster)",
			testFunc: testLogFileEndpointDeadCluster,
		},
		{
			name:     "/v0/logs/stream endpoint",
			testFunc: testLogStreamEndpoint,
		},
		{
			name:     "/api/v0/logs endpoint with glob pattern (dead cluster)",
			testFunc: testNodeLogsEndpointDeadCluster,
		},
		{
			name:     "/api/v0/tasks/timeline endpoint (live cluster)",
			testFunc: testTimelineEndpointLiveCluster,
		},
		{
			name:     "/api/v0/tasks/timeline endpoint (dead cluster)",
			testFunc: testTimelineEndpointDeadCluster,
		},
		{
			name:     "/events endpoint (live cluster)",
			testFunc: testEventsEndpointLiveCluster,
		},
		{
			name:     "/events endpoint (dead cluster)",
			testFunc: testEventsEndpointDeadCluster,
		},
		{
			name:     "/logical/actors endpoint (dead cluster)",
			testFunc: testLogicalActorsEndpointDeadCluster,
		},
		{
			name:     "Live cluster: /api/v0/tasks?detail=1 should return the detailed task information of all task attempts",
			testFunc: testLiveClusterTasks,
		},
		{
			name:     "Dead cluster: /api/v0/tasks should return detailed task information from historical replay",
			testFunc: testDeadClusterTasks,
		},
		{
			name:     "Live cluster: /nodes?view=summary should return the current snapshot containing node summary and resource usage information",
			testFunc: testLiveClusterNodes,
		},
		{
			name:     "Dead cluster: /nodes should return the historical replay containing node summary and resource usage snapshots of a cluster session",
			testFunc: testDeadClusterNodes,
		},
		{
			name:     "Live cluster: /nodes/{node_id} should return the current snapshot containing node summary of the specified node",
			testFunc: testLiveClusterNode,
		},
		{
			name:     "Dead cluster: /nodes/{node_id} should return the historical replay containing node summary snapshots of the specified node in a cluster session",
			testFunc: testDeadClusterNode,
		},
		{
			name:     "Live cluster: cluster_metadata endpoint should return metadata (Ray version, Python version, etc.)",
			testFunc: testLiveClusterMetadata,
		},
		{
			name:     "Dead cluster: cluster_metadata endpoint should return stored metadata from S3",
			testFunc: testDeadClusterMetadata,
		},
		{
			name:     "Dead cluster: /api/v0/placement_groups should return stored placement groups from S3",
			testFunc: testDeadClusterPlacementGroups,
		},
		{
			name:     "Live cluster: /api/v0/tasks/summarize?summary_by=lineage should return the lineage tree of all tasks",
			testFunc: testLiveClusterTaskSummarize,
		},
		{
			name:     "Dead cluster: /api/v0/tasks/summarize?summary_by=lineage should return the lineage tree of all tasks",
			testFunc: testDeadClusterTaskSummarize,
		},
		{
			name:     "Live cluster: /api/v0/tasks/summarize should return the task summary grouped by func_name",
			testFunc: testLiveClusterTaskSummarizeFuncName,
		},
		{
			name:     "Dead cluster: /api/v0/tasks/summarize should return the task summary grouped by func_name",
			testFunc: testDeadClusterTaskSummarizeFuncName,
		},
		{
			name:     "Live and dead cluster: /timezone should return consistent timezone info",
			testFunc: testLiveAndDeadClusterTimezone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)
			namespace := test.NewTestNamespace()

			tt.testFunc(test, g, namespace, s3Client)
		})
	}
}

func testLiveClusters(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	verifyHistoryServerEndpoints(test, g, client, historyServerURL)
	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live clusters E2E test completed successfully")
}

func testLiveGrafanaHealth(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnvWithPrometheusAndGrafana(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	verifyHistoryServerGrafanaHealthEndpoint(test, g, client, historyServerURL, sessionID)
	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live clusters grafana health E2E test completed successfully")
}

func testLivePrometheusHealth(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnvWithPrometheusAndGrafana(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	verifyHistoryServerPrometheusHealthEndpoint(test, g, client, historyServerURL)
	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live clusters prometheus health E2E test completed successfully")
}

// testLogFileEndpointLiveCluster verifies that the history server can fetch log files from a live cluster.
func testLogFileEndpointLiveCluster(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	nodeID := GetOneOfNodeID(g, client, historyServerURL, false)
	filename := "raylet.out"

	logFileTestCases := []struct {
		name           string
		buildURL       func(baseURL, nodeID string) string
		expectedStatus int
	}{
		// lines parameter
		{"lines=100", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=100", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"lines=0 (default 1000)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"lines=-1 (all)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=-1", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// timeout parameter
		{"timeout=5", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&timeout=5", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"timeout=30", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&timeout=30", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// attempt_number parameter
		{"attempt_number=0", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&attempt_number=0", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"attempt_number=1", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&attempt_number=1", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// download_filename parameter
		{"download_filename=custom.log", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&download_filename=custom.log", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// filter_ansi_code parameter
		{"filter_ansi_code=true", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&filter_ansi_code=true", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"filter_ansi_code=false", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&filter_ansi_code=false", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// suffix parameter
		{"suffix=out (default)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&suffix=out", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"suffix=err", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&suffix=err", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// Combined parameters
		{"lines+timeout+filter", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=50&timeout=10&filter_ansi_code=true", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// Missing mandatory parameters
		{"missing node_id and node_ip", func(u, _ string) string { return fmt.Sprintf("%s%s?filename=%s", u, EndpointLogsFile, filename) }, http.StatusBadRequest},
		{"missing filename", func(u, n string) string { return fmt.Sprintf("%s%s?node_id=%s", u, EndpointLogsFile, n) }, http.StatusBadRequest},
		{"missing both", func(u, _ string) string { return fmt.Sprintf("%s%s", u, EndpointLogsFile) }, http.StatusBadRequest},

		// Invalid parameters
		{"invalid lines (string)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=abc", u, EndpointLogsFile, n, filename)
		}, http.StatusBadRequest},
		{"invalid timeout (string)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&timeout=invalid", u, EndpointLogsFile, n, filename)
		}, http.StatusBadRequest},
		{"invalid attempt_number (string)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&attempt_number=xyz", u, EndpointLogsFile, n, filename)
		}, http.StatusBadRequest},
		{"invalid suffix", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&suffix=invalid", u, EndpointLogsFile, n, filename)
		}, http.StatusBadRequest},
		// NOTE: Ray Dashboard will return 500 (Internal Server Error) for the file not found error
		// ref: https://github.com/ray-project/ray/blob/68d01c4c48a59c7768ec9c2359a1859966c446b6/python/ray/dashboard/modules/state/state_head.py#L282-L284
		{"file not found", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=nonexistent.log", u, EndpointLogsFile, n)
		}, http.StatusInternalServerError},
		{"task_id invalid (not found)", func(u, _ string) string { return fmt.Sprintf("%s%s?task_id=nonexistent-task-id", u, EndpointLogsFile) }, http.StatusInternalServerError},
		{"node_ip invalid (non-existent)", func(u, _ string) string {
			return fmt.Sprintf("%s%s?node_ip=192.168.255.255&filename=%s", u, EndpointLogsFile, filename)
		}, http.StatusInternalServerError},
		{"pid invalid (string)", func(u, n string) string { return fmt.Sprintf("%s%s?pid=abc&node_id=%s", u, EndpointLogsFile, n) }, http.StatusBadRequest},
		{"pid non-existent", func(u, n string) string { return fmt.Sprintf("%s%s?pid=999999&node_id=%s", u, EndpointLogsFile, n) }, http.StatusInternalServerError},

		// Path traversal attacks
		{"traversal ../etc/passwd", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=../etc/passwd", u, EndpointLogsFile, n)
		}, http.StatusBadRequest},
		{"traversal ..", func(u, n string) string { return fmt.Sprintf("%s%s?node_id=%s&filename=..", u, EndpointLogsFile, n) }, http.StatusBadRequest},
		{"traversal /etc/passwd", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=/etc/passwd", u, EndpointLogsFile, n)
		}, http.StatusBadRequest},
		{"traversal ../../secret", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=../../secret", u, EndpointLogsFile, n)
		}, http.StatusBadRequest},
		{"traversal in node_id", func(u, _ string) string {
			return fmt.Sprintf("%s%s?node_id=../evil&filename=%s", u, EndpointLogsFile, filename)
		}, http.StatusBadRequest},
	}

	for _, tc := range logFileTestCases {
		test.T().Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			url := tc.buildURL(historyServerURL, nodeID)
			resp, err := client.Get(url)
			g.Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()

			body, err := io.ReadAll(resp.Body)
			g.Expect(err).NotTo(HaveOccurred())

			if resp.StatusCode != tc.expectedStatus {
				LogWithTimestamp(t, "Test case '%s' failed: expected %d, got %d, body: %s",
					tc.name, tc.expectedStatus, resp.StatusCode, string(body))
			}

			if tc.expectedStatus == http.StatusOK {
				g.Expect(body).ToNot(BeEmpty())
			}
		})
	}

	// Sub-test for task_id parameter (live cluster)
	test.T().Run("task_id parameter", func(t *testing.T) {
		g := NewWithT(t)

		// Get all eligible task IDs
		taskIDs := getAllEligibleTaskIDs(g, client, historyServerURL)
		LogWithTimestamp(t, "Found %d eligible task IDs for testing", len(taskIDs))

		var successCount int
		var lastError string

		// Try each task ID until one succeeds
		for _, taskID := range taskIDs {
			LogWithTimestamp(t, "Testing task_id: %s", taskID)

			url := fmt.Sprintf("%s%s?task_id=%s", historyServerURL, EndpointLogsFile, url.QueryEscape(taskID))
			resp, err := client.Get(url)
			if err != nil {
				lastError = fmt.Sprintf("HTTP error for task %s: %v", taskID, err)
				continue
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				successCount++
				LogWithTimestamp(t, "Task %s succeeded, returned %d bytes", taskID, len(body))
				break
			}
			lastError = fmt.Sprintf("task %s returned %d: %s", taskID, resp.StatusCode, string(body))
			LogWithTimestamp(t, "Task %s failed: %s", taskID, lastError)
		}

		g.Expect(successCount).To(BeNumerically(">", 0),
			"At least one task_id should succeed. Last error: %s", lastError)
	})

	// Sub-test for actor_id parameter (live cluster)
	test.T().Run("actor_id parameter", func(t *testing.T) {
		g := NewWithT(t)

		// Get all eligible actor IDs
		actorIDs := getAllEligibleActorIDs(g, client, historyServerURL)
		LogWithTimestamp(t, "Found %d eligible actor IDs for testing", len(actorIDs))

		var successCount int
		var lastError string

		// Try each actor ID until one succeeds
		for _, actorID := range actorIDs {
			LogWithTimestamp(t, "Testing actor_id: %s", actorID)

			url := fmt.Sprintf("%s%s?actor_id=%s", historyServerURL, EndpointLogsFile, url.QueryEscape(actorID))
			resp, err := client.Get(url)
			if err != nil {
				lastError = fmt.Sprintf("HTTP error for actor %s: %v", actorID, err)
				continue
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				successCount++
				LogWithTimestamp(t, "Actor %s succeeded, returned %d bytes", actorID, len(body))
				break
			}
			lastError = fmt.Sprintf("actor %s returned %d: %s", actorID, resp.StatusCode, string(body))
			LogWithTimestamp(t, "Actor %s failed: %s", actorID, lastError)
		}

		g.Expect(successCount).To(BeNumerically(">", 0),
			"At least one actor_id should succeed. Last error: %s", lastError)
	})

	// Sub-test for pid parameter (live cluster)
	test.T().Run("pid parameter", func(t *testing.T) {
		g := NewWithT(t)

		// Get an eligible worker PID and its node ID
		pid, nodeID := getEligibleWorkerPID(g, client, historyServerURL)
		LogWithTimestamp(t, "Found eligible worker PID %d on node %s for testing", pid, nodeID)

		// Test successful case
		url := fmt.Sprintf("%s%s?pid=%d&node_id=%s", historyServerURL, EndpointLogsFile, pid, nodeID)
		resp, err := client.Get(url)
		g.Expect(err).NotTo(HaveOccurred())
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK for valid pid and node_id, got %d: %s", resp.StatusCode, string(body))
		g.Expect(body).ToNot(BeEmpty())

		// Test missing node_id
		url = fmt.Sprintf("%s%s?pid=%d", historyServerURL, EndpointLogsFile, pid)
		resp, err = client.Get(url)
		g.Expect(err).NotTo(HaveOccurred())
		resp.Body.Close()
		g.Expect(resp.StatusCode).To(Equal(http.StatusBadRequest))
	})

	// Sub-test for node_ip parameter (live cluster)
	test.T().Run("node_ip parameter", func(t *testing.T) {
		g := NewWithT(t)

		// Get node IP from head pod (use Pod IP, not Host IP)
		// Ray registers nodes with Pod IP (--node-ip-address flag)
		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		nodeIP := headPod.Status.PodIP
		g.Expect(nodeIP).NotTo(BeEmpty(), "Head pod should have a pod IP")
		LogWithTimestamp(t, "Found head pod with IP: %s", nodeIP)

		// Test successful case: node_ip + filename
		url := fmt.Sprintf("%s%s?node_ip=%s&filename=%s", historyServerURL, EndpointLogsFile, nodeIP, filename)
		resp, err := client.Get(url)
		g.Expect(err).NotTo(HaveOccurred())
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		// For live cluster, the request is proxied to Ray Dashboard
		// The dashboard should be able to resolve node_ip to node_id
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK for valid node_ip, got %d: %s", resp.StatusCode, string(body))
		g.Expect(body).ToNot(BeEmpty())
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Log file endpoint tests completed")
}

// testLogFileEndpointDeadCluster verifies that the history server can fetch log files from S3 after a cluster is deleted.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Delete RayCluster to trigger log upload to S3
// 4. Apply History Server and get its URL
// 5. Verify that the history server can fetch log content from S3
// 6. Verify parameter validation for dead cluster
// 7. Verify security (path traversal) protection
// 8. Delete S3 bucket to ensure test isolation
func testLogFileEndpointDeadCluster(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Capture node IP and ID before deleting cluster (for node_ip tests later)
	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	savedNodeIP := headPod.Status.PodIP
	savedNodeID := GetNodeIDFromPod(test, g, HeadPod(test, rayCluster), "ray-head")
	LogWithTimestamp(test.T(), "Captured node IP %s and node ID %s before cluster deletion", savedNodeIP, savedNodeID)

	// Delete RayCluster to trigger log upload
	DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	nodeID := GetOneOfNodeID(g, client, historyServerURL, false)
	filename := "raylet.out"

	logFileTestCases := []struct {
		name           string
		buildURL       func(baseURL, nodeID string) string
		expectedStatus int
	}{
		// Basic parameters
		{"lines=100", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=100", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"lines=0 (default 1000)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"lines=-1 (all)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=-1", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// timeout parameter
		// NOTE: timeout feature is not yet implemented, we just accept and validate the timeout parameter
		{"timeout=5", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&timeout=5", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"timeout=30", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&timeout=30", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// attempt_number parameter
		{"attempt_number=0", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&attempt_number=0", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"attempt_number=1 (not found)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&attempt_number=1", u, EndpointLogsFile, n, filename)
		}, http.StatusNotFound},

		// download_filename parameter
		{"download_filename=custom.log", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&download_filename=custom.log", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// filter_ansi_code parameter
		{"filter_ansi_code=true", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&filter_ansi_code=true", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"filter_ansi_code=false", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&filter_ansi_code=false", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// suffix parameter
		{"suffix=out (default)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&suffix=out", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"suffix=err", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&suffix=err", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// Combined parameters
		{"lines+timeout+filter", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=50&timeout=10&filter_ansi_code=true", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},
		{"all parameters", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=100&timeout=15&attempt_number=0&download_filename=custom.log&filter_ansi_code=true", u, EndpointLogsFile, n, filename)
		}, http.StatusOK},

		// Missing mandatory parameters
		{"missing node_id and node_ip", func(u, _ string) string { return fmt.Sprintf("%s%s?filename=%s", u, EndpointLogsFile, filename) }, http.StatusBadRequest},
		{"missing filename", func(u, n string) string { return fmt.Sprintf("%s%s?node_id=%s", u, EndpointLogsFile, n) }, http.StatusBadRequest},
		{"missing both", func(u, _ string) string { return fmt.Sprintf("%s%s", u, EndpointLogsFile) }, http.StatusBadRequest},

		// Invalid parameters
		{"invalid lines (string)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=abc", u, EndpointLogsFile, n, filename)
		}, http.StatusBadRequest},
		{"invalid timeout (string)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&timeout=invalid", u, EndpointLogsFile, n, filename)
		}, http.StatusBadRequest},
		{"invalid attempt_number (string)", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&attempt_number=xyz", u, EndpointLogsFile, n, filename)
		}, http.StatusBadRequest},
		{"file not found", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=nonexistent.log", u, EndpointLogsFile, n)
		}, http.StatusNotFound},
		{"invalid suffix", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=%s&suffix=invalid", u, EndpointLogsFile, n, filename)
		}, http.StatusBadRequest},
		{"task_id invalid (not found)", func(u, _ string) string { return fmt.Sprintf("%s%s?task_id=nonexistent-task-id", u, EndpointLogsFile) }, http.StatusBadRequest},
		{"non-existent pid", func(u, n string) string { return fmt.Sprintf("%s%s?pid=999999&node_id=%s", u, EndpointLogsFile, n) }, http.StatusNotFound},

		// node_ip parameter tests
		{"node_ip invalid (non-existent)", func(u, _ string) string {
			return fmt.Sprintf("%s%s?node_ip=192.168.255.255&filename=%s", u, EndpointLogsFile, filename)
		}, http.StatusNotFound},

		// Path traversal attacks
		{"traversal ../etc/passwd", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=../etc/passwd", u, EndpointLogsFile, n)
		}, http.StatusBadRequest},
		{"traversal ..", func(u, n string) string { return fmt.Sprintf("%s%s?node_id=%s&filename=..", u, EndpointLogsFile, n) }, http.StatusBadRequest},
		{"traversal /etc/passwd", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=/etc/passwd", u, EndpointLogsFile, n)
		}, http.StatusBadRequest},
		{"traversal ../../secret", func(u, n string) string {
			return fmt.Sprintf("%s%s?node_id=%s&filename=../../secret", u, EndpointLogsFile, n)
		}, http.StatusBadRequest},
		{"traversal in node_id", func(u, _ string) string {
			return fmt.Sprintf("%s%s?node_id=../evil&filename=%s", u, EndpointLogsFile, filename)
		}, http.StatusBadRequest},
	}

	for _, tc := range logFileTestCases {
		test.T().Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)

			url := tc.buildURL(historyServerURL, nodeID)
			resp, err := client.Get(url)
			g.Expect(err).NotTo(HaveOccurred())
			defer func() {
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}()

			body, err := io.ReadAll(resp.Body)
			g.Expect(err).NotTo(HaveOccurred())

			if resp.StatusCode != tc.expectedStatus {
				LogWithTimestamp(t, "Test case '%s' failed: expected %d, got %d, body: %s",
					tc.name, tc.expectedStatus, resp.StatusCode, string(body))
			}

			g.Expect(resp.StatusCode).To(Equal(tc.expectedStatus),
				"Test case '%s' failed: expected %d, got %d", tc.name, tc.expectedStatus, resp.StatusCode)

			if tc.expectedStatus == http.StatusOK {
				g.Expect(body).ToNot(BeEmpty())
			}
		})
	}

	// Sub-tests for specific parameter behaviors
	test.T().Run("download_filename header validation", func(t *testing.T) {
		g := NewWithT(t)
		// Test with download_filename parameter set
		customFilename := "custom_download.log"
		urlWithDownload := fmt.Sprintf("%s%s?node_id=%s&filename=%s&download_filename=%s", historyServerURL, EndpointLogsFile, nodeID, filename, customFilename)
		resp, err := client.Get(urlWithDownload)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		g.Expect(resp.StatusCode).To(Equal(http.StatusOK))
		contentDisposition := resp.Header.Get("Content-Disposition")
		g.Expect(contentDisposition).To(ContainSubstring("attachment"))
		g.Expect(contentDisposition).To(ContainSubstring(fmt.Sprintf("filename=%s", customFilename)))
	})

	test.T().Run("filter_ansi_code behavior", func(t *testing.T) {
		g := NewWithT(t)
		// Fetch with filter_ansi_code=false (original content with ANSI codes)
		urlWithoutFilter := fmt.Sprintf("%s%s?node_id=%s&filename=%s&filter_ansi_code=false&lines=100", historyServerURL, EndpointLogsFile, nodeID, filename)
		resp, err := client.Get(urlWithoutFilter)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		g.Expect(resp.StatusCode).To(Equal(http.StatusOK))
		bodyWithoutFilter, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())

		// Fetch with filter_ansi_code=true (ANSI codes should be removed)
		urlWithFilter := fmt.Sprintf("%s%s?node_id=%s&filename=%s&filter_ansi_code=true&lines=100", historyServerURL, EndpointLogsFile, nodeID, filename)
		resp2, err := client.Get(urlWithFilter)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp2.Body.Close()

		g.Expect(resp2.StatusCode).To(Equal(http.StatusOK))
		bodyWithFilter, err := io.ReadAll(resp2.Body)
		g.Expect(err).NotTo(HaveOccurred())

		// Check if original content contains ANSI codes using the same pattern as reader.go
		hasAnsiInOriginal := ansiEscapePattern.Match(bodyWithoutFilter)

		if hasAnsiInOriginal {
			LogWithTimestamp(test.T(), "Original log contains ANSI codes, verifying they are filtered")
			// Filtered content should NOT contain ANSI escape sequences
			hasAnsiInFiltered := ansiEscapePattern.Match(bodyWithFilter)
			g.Expect(hasAnsiInFiltered).To(BeFalse(), "Filtered content should not contain ANSI escape sequences")
		} else {
			LogWithTimestamp(test.T(), "Log doesn't contain ANSI codes, check is skipped...")
		}
	})

	test.T().Run("attempt_number behavior", func(t *testing.T) {
		g := NewWithT(t)
		// Test with attempt_number=0
		urlAttempt0 := fmt.Sprintf("%s%s?node_id=%s&filename=%s&attempt_number=0", historyServerURL, EndpointLogsFile, nodeID, filename)
		resp, err := client.Get(urlAttempt0)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		g.Expect(resp.StatusCode).To(Equal(http.StatusOK))
		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(body).ToNot(BeEmpty())
		LogWithTimestamp(test.T(), "attempt_number=0 returned %d bytes", len(body))

		// attempt_number=1 should fail as retry log doesn't exist for normal execution
		urlAttempt1 := fmt.Sprintf("%s%s?node_id=%s&filename=%s&attempt_number=1", historyServerURL, EndpointLogsFile, nodeID, filename)
		resp2, err := client.Get(urlAttempt1)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp2.Body.Close()

		g.Expect(resp2.StatusCode).To(Equal(http.StatusNotFound),
			"attempt_number=1 should return 404 when retry log doesn't exist")
		LogWithTimestamp(test.T(), "attempt_number=1 correctly returns 404 (file not found)")
	})

	// Sub-test for task_id parameter
	test.T().Run("task_id parameter", func(t *testing.T) {
		g := NewWithT(t)

		// Get all eligible task IDs
		taskIDs := getAllEligibleTaskIDs(g, client, historyServerURL)
		LogWithTimestamp(t, "Found %d eligible task IDs for testing", len(taskIDs))

		var successCount int
		var lastError string

		// Try each task ID until one succeeds
		for _, taskID := range taskIDs {
			LogWithTimestamp(t, "Testing task_id: %s", taskID)

			url := fmt.Sprintf("%s%s?task_id=%s", historyServerURL, EndpointLogsFile, url.QueryEscape(taskID))
			resp, err := client.Get(url)
			if err != nil {
				lastError = fmt.Sprintf("HTTP error for task %s: %v", taskID, err)
				continue
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				successCount++
				LogWithTimestamp(t, "Task %s succeeded, returned %d bytes", taskID, len(body))
				break
			}
			lastError = fmt.Sprintf("task %s returned %d: %s", taskID, resp.StatusCode, string(body))
			LogWithTimestamp(t, "Task %s failed: %s", taskID, lastError)
		}

		g.Expect(successCount).To(BeNumerically(">", 0),
			"At least one task_id should succeed. Last error: %s", lastError)
	})

	// Sub-test for actor_id parameter (dead cluster)
	test.T().Run("actor_id parameter", func(t *testing.T) {
		g := NewWithT(t)

		// Get all eligible actor IDs
		actorIDs := getAllEligibleActorIDs(g, client, historyServerURL)
		LogWithTimestamp(t, "Found %d eligible actor IDs for testing", len(actorIDs))

		var successCount int
		var lastError string

		// Try each actor ID until one succeeds
		for _, actorID := range actorIDs {
			LogWithTimestamp(t, "Testing actor_id: %s", actorID)

			url := fmt.Sprintf("%s%s?actor_id=%s", historyServerURL, EndpointLogsFile, url.QueryEscape(actorID))
			resp, err := client.Get(url)
			if err != nil {
				lastError = fmt.Sprintf("HTTP error for actor %s: %v", actorID, err)
				continue
			}

			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				successCount++
				LogWithTimestamp(t, "Actor %s succeeded, returned %d bytes", actorID, len(body))
				break
			}
			lastError = fmt.Sprintf("actor %s returned %d: %s", actorID, resp.StatusCode, string(body))
			LogWithTimestamp(t, "Actor %s failed: %s", actorID, lastError)
		}

		g.Expect(successCount).To(BeNumerically(">", 0),
			"At least one actor_id should succeed. Last error: %s", lastError)
	})

	// Sub-test for pid parameter (dead cluster)
	// NOTE: This test is skipped because Ray export events don't include worker_pid.
	// See: https://github.com/ray-project/ray/issues/60129
	// Worker lifecycle events are not yet exported, so we cannot obtain worker PIDs
	// from historical data for dead clusters.
	test.T().Run("pid parameter", func(t *testing.T) {
		t.Skip("Skipping pid parameter test for dead cluster: worker_pid not available in Ray export events (see https://github.com/ray-project/ray/issues/60129)")
	})

	// Sub-test for node_ip parameter (dead cluster)
	test.T().Run("node_ip parameter", func(t *testing.T) {
		g := NewWithT(t)

		// Use the captured node IP and ID from before cluster deletion
		LogWithTimestamp(t, "Testing node_ip parameter with IP: %s, ID: %s", savedNodeIP, savedNodeID)

		// Test successful case: node_ip + filename
		url := fmt.Sprintf("%s%s?node_ip=%s&filename=%s", historyServerURL, EndpointLogsFile, savedNodeIP, filename)
		resp, err := client.Get(url)
		g.Expect(err).NotTo(HaveOccurred())
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK for valid node_ip, got %d: %s", resp.StatusCode, string(body))
		g.Expect(body).ToNot(BeEmpty())

		// Test that node_ip and node_id point to the same node (should return same content)
		urlWithNodeID := fmt.Sprintf("%s%s?node_id=%s&filename=%s", historyServerURL, EndpointLogsFile, savedNodeID, filename)
		resp2, err := client.Get(urlWithNodeID)
		g.Expect(err).NotTo(HaveOccurred())
		bodyWithNodeID, _ := io.ReadAll(resp2.Body)
		resp2.Body.Close()
		g.Expect(resp2.StatusCode).To(Equal(http.StatusOK))
		g.Expect(body).To(HaveLen(len(bodyWithNodeID)), "node_ip and node_id should return same content")
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster log file endpoint tests completed")
}

// getAllEligibleTaskIDs retrieves all non-actor task IDs with node_id from the /api/v0/tasks endpoint.
// Returns a list of task IDs that are eligible for log file testing.
// Note: We filter out actor tasks because they don't have task_log_info by default
// (unless RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1 is set).
func getAllEligibleTaskIDs(g *WithT, client *http.Client, historyServerURL string) []string {
	var taskIDs []string
	resp, err := client.Get(historyServerURL + "/api/v0/tasks")
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()

	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))

	body, err := io.ReadAll(resp.Body)
	g.Expect(err).NotTo(HaveOccurred())

	var result map[string]any
	err = json.Unmarshal(body, &result)
	g.Expect(err).NotTo(HaveOccurred())

	// Extract task_id from response
	// Response format: {"result": true, "msg": "...", "data": {"result": {"result": [tasks...], ...}}}
	data, ok := result["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "response should have 'data' field")

	dataResult, ok := data["result"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "data should have 'result' field")

	tasks, ok := dataResult["result"].([]any)
	g.Expect(ok).To(BeTrue(), "result should have 'result' array")
	g.Expect(tasks).ToNot(BeEmpty(), "should have at least one task")

	// Find all non-actor tasks with node_id
	for _, t := range tasks {
		task, ok := t.(map[string]any)
		if !ok {
			continue
		}

		// Check if this is an actor task
		actorID, _ := task["actor_id"].(string)
		if actorID != "" {
			// Skip actor tasks - they don't have task_log_info unless
			// RAY_ENABLE_RECORD_ACTOR_TASK_LOGGING=1 is set
			continue
		}

		// Check if it has node_id
		nodeID, _ := task["node_id"].(string)
		if nodeID == "" {
			// If nodeID is empty, it means the task is not scheduled yet. Skip it
			// as it will not have logs
			continue
		}

		// Found a non-actor task with logs
		taskID, ok := task["task_id"].(string)
		if ok && taskID != "" {
			taskIDs = append(taskIDs, taskID)
		}
	}

	g.Expect(taskIDs).ToNot(BeEmpty(), "should have at least one eligible task")

	return taskIDs
}

// getAllEligibleActorIDs retrieves all actor IDs from the /logical/actors endpoint.
// Returns a list of actor IDs.
func getAllEligibleActorIDs(g *WithT, client *http.Client, historyServerURL string) []string {
	var actorIDs []string
	resp, err := client.Get(historyServerURL + "/logical/actors")
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()

	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))

	body, err := io.ReadAll(resp.Body)
	g.Expect(err).NotTo(HaveOccurred())

	var result map[string]any
	err = json.Unmarshal(body, &result)
	g.Expect(err).NotTo(HaveOccurred())

	// Extract actorId from response
	// Response format: {"result": true, "msg": "...", "data": {"actors": {actorId: {...}, ...}}}
	data, ok := result["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "response should have 'data' field")

	actors, ok := data["actors"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "data should have 'actors' field")
	g.Expect(actors).ToNot(BeEmpty(), "should have at least one actor")

	// Find all actors
	for actorID := range actors {
		actorIDs = append(actorIDs, actorID)
	}

	g.Expect(actorIDs).ToNot(BeEmpty(), "should have at least one eligible actor")

	return actorIDs
}

// getEligibleWorkerPID retrieves an eligible worker PID and its node ID for log testing.
// It queries the /api/v0/tasks endpoint to find any task with a valid worker_pid and node_id.
func getEligibleWorkerPID(g *WithT, client *http.Client, historyServerURL string) (pid int, nodeID string) {
	resp, err := client.Get(historyServerURL + "/api/v0/tasks")
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()

	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))

	body, err := io.ReadAll(resp.Body)
	g.Expect(err).NotTo(HaveOccurred())

	var result map[string]any
	err = json.Unmarshal(body, &result)
	g.Expect(err).NotTo(HaveOccurred())

	// Response format: {"result": true, "msg": "...", "data": {"result": {"result": [tasks...], ...}}}
	data, ok := result["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "response should have 'data' field")

	dataResult, ok := data["result"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "data should have 'result' field")

	tasks, ok := dataResult["result"].([]any)
	g.Expect(ok).To(BeTrue(), "result should have 'result' array")
	g.Expect(tasks).ToNot(BeEmpty(), "should have at least one task")

	// Find a task with valid worker_pid and node_id
	for _, t := range tasks {
		task, ok := t.(map[string]any)
		if !ok {
			continue
		}

		// Check for worker_pid (must be non-zero)
		workerPidFloat, pidOk := task["worker_pid"].(float64)
		if !pidOk || workerPidFloat == 0 {
			continue
		}

		// Check for node_id (must be non-empty)
		taskNodeID, nodeOk := task["node_id"].(string)
		if !nodeOk || taskNodeID == "" {
			continue
		}

		// Found an eligible task with worker PID
		return int(workerPidFloat), taskNodeID
	}

	// If we get here, no eligible task was found
	g.Fail("should find at least one task with valid worker_pid and node_id")
	return 0, ""
}

// testLogStreamEndpoint verifies the /v0/logs/stream endpoint behavior for both live and dead clusters.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Apply History Server and get its URL
// 4. Test live cluster: streaming should work (return 200)
// 5. Delete cluster to test dead cluster behavior
// 6. Test dead cluster: streaming should return 501 Not Implemented
// 7. Delete S3 bucket to ensure test isolation
func testLogStreamEndpoint(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	// Test 1: Live cluster - streaming should work
	LogWithTimestamp(test.T(), "Testing /v0/logs/stream for live cluster")
	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	nodeID := GetOneOfNodeID(g, client, historyServerURL, false)
	filename := "raylet.out"
	streamURL := fmt.Sprintf("%s%s?node_id=%s&filename=%s", historyServerURL, EndpointLogsStream, nodeID, filename)

	// Test live cluster streaming endpoint
	test.T().Run("live cluster", func(t *testing.T) {
		g := NewWithT(t)

		resp, err := client.Get(streamURL)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Live cluster should support log streaming")

		resp.Body.Close()
	})

	// Delete RayCluster to test dead cluster behavior
	err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Deleted RayCluster %s/%s", namespace.Name, rayCluster.Name)

	// Wait for cluster to be fully deleted
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	// Get the new cluster info
	deadClusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(deadClusterInfo.SessionName).NotTo(Equal(LiveSessionName))

	// Set context for dead cluster
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, deadClusterInfo.SessionName)

	// Test dead cluster streaming endpoint - should return 501
	test.T().Run("dead cluster", func(t *testing.T) {
		g := NewWithT(t)
		resp2, err := client.Get(streamURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp2.Body.Close()

		g.Expect(resp2.StatusCode).To(Equal(http.StatusNotImplemented), "Dead cluster should return 501 for log streaming")
		body, _ := io.ReadAll(resp2.Body)
		g.Expect(string(body)).To(ContainSubstring("Log streaming only available for live clusters"))
		LogWithTimestamp(test.T(), "Dead cluster correctly returns 501 Not Implemented")
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Log stream endpoint tests completed")
}

// testNodeLogsEndpointDeadCluster verifies that the /api/v0/logs endpoint correctly lists and filters
// log files for a dead cluster using the glob query parameter.
//
// The glob patterns tested mirror the four patterns constructed by the Ray Dashboard frontend:
// https://github.com/ray-project/ray/blob/60d14699591f1150eb71b8f657160cab9f767dfb/python/ray/dashboard/client/src/pages/log/Logs.tsx#L134-L141
//   - Case A: folder + filename search → "events/*RAYLET*"
//   - Case B: root + filename search   → "*dashboard*"
//   - Case C: folder only              → "events/*"
//   - Case D: no glob (omitted)        → server returns all files
//
// The test follows these steps:
// 1. Create a RayCluster and submit a Ray job.
// 2. Delete RayCluster to trigger log upload to S3.
// 3. Deploy the History Server and verify the cluster is listed as a dead session.
// 4. Case A — glob=events/*RAYLET*: a directory prefix combined with a wrapped wildcard matches a specific file in a subdirectory.
// 5. Case B — glob=*dashboard*: wildcards on both sides match any filename containing "dashboard" at root level.
// 6. Case C — glob=events/*: a directory prefix with a bare wildcard lists all files in that subdirectory.
// 7. Case D — no glob parameter: omitting the glob returns all log files (server default).
// 8. glob=raylet* — wildcard suffix matches raylet.out and raylet.err.
// 9. glob=nonexistent-*.xyz — a pattern matching no files returns an empty result.
// 10. glob=events/event_JOBS* — subdirectory prefix is split from the pattern, then the wildcard matches within that subdirectory.
// 11. glob=**/*.out — doublestar pattern recursively matches all .out files across all directories.
// 12. Delete S3 bucket to ensure test isolation.
func testNodeLogsEndpointDeadCluster(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName), "Cluster should be a dead session after deletion")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	nodeID := GetOneOfNodeID(g, client, historyServerURL, false)

	// Case A: subdirectory + filename search — glob=events/*RAYLET*
	test.T().Run("Case A: glob=events/*RAYLET* matches RAYLET event file in subdirectory", func(t *testing.T) {
		g := NewWithT(t)

		logsURL := fmt.Sprintf("%s%s?node_id=%s&glob=%s", historyServerURL, EndpointLogs, nodeID, url.QueryEscape("events/*RAYLET*"))
		resp, err := client.Get(logsURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK, body: %s", string(body))

		result := parseLogsResponse(body)
		g.Expect(result).NotTo(BeNil(), "Response should be parseable, body: %s", string(body))

		// events/*RAYLET* should match only event_RAYLET.log in the events/ subdirectory.
		g.Expect(result).To(HaveLen(1), "Should only have one category, got: %v", result)
		internalFiles, _ := result["internal"].([]any)
		g.Expect(internalFiles).To(ConsistOf("event_RAYLET.log"), "events/*RAYLET* should match exactly event_RAYLET.log")
		LogWithTimestamp(t, "glob=events/*RAYLET* correctly returned %d file", len(internalFiles))
	})

	// Case B: root directory filename search — glob=*dashboard*
	test.T().Run("Case B: glob=*dashboard* matches dashboard files at root", func(t *testing.T) {
		g := NewWithT(t)

		logsURL := fmt.Sprintf("%s%s?node_id=%s&glob=%s", historyServerURL, EndpointLogs, nodeID, url.QueryEscape("*dashboard*"))
		resp, err := client.Get(logsURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK, body: %s", string(body))

		result := parseLogsResponse(body)
		g.Expect(result).NotTo(BeNil(), "Response should be parseable, body: %s", string(body))

		// *dashboard* matches any filename containing "dashboard", which includes files like
		// dashboard_agent.out/err/log (categorized as "agent") and dashboard.out/err/log (categorized as "dashboard").
		// Verify that every returned file contains "dashboard".
		totalFiles := countFiles(result)
		g.Expect(totalFiles).To(BeNumerically(">", 0), "glob=*dashboard* should match at least one file, got: %v", result)
		for category, files := range result {
			fileList, _ := files.([]any)
			for _, f := range fileList {
				g.Expect(f.(string)).To(ContainSubstring("dashboard"),
					"Each file matched by *dashboard* should contain 'dashboard', got %q in category %q", f, category)
			}
		}
		LogWithTimestamp(t, "glob=*dashboard* correctly returned %d dashboard files across %d categories", totalFiles, len(result))
	})

	// Case C: enter subdirectory without search — glob=events/*
	test.T().Run("Case C: glob=events/* lists all files in events subdirectory", func(t *testing.T) {
		g := NewWithT(t)

		logsURL := fmt.Sprintf("%s%s?node_id=%s&glob=%s", historyServerURL, EndpointLogs, nodeID, url.QueryEscape("events/*"))
		resp, err := client.Get(logsURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK, body: %s", string(body))

		result := parseLogsResponse(body)
		g.Expect(result).NotTo(BeNil(), "Response should be parseable, body: %s", string(body))

		// events/* should list all event log files; they all fall into the "internal" category.
		totalFiles := countFiles(result)
		g.Expect(totalFiles).To(BeNumerically(">", 0), "events/* should return at least one file, got: %v", result)
		LogWithTimestamp(t, "glob=events/* correctly returned %d files across %d categories", totalFiles, len(result))
	})

	// Case D: no glob parameter — server returns all log files.
	test.T().Run("Case D: no glob parameter returns all log files", func(t *testing.T) {
		g := NewWithT(t)

		logsURL := fmt.Sprintf("%s%s?node_id=%s", historyServerURL, EndpointLogs, nodeID)
		resp, err := client.Get(logsURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK, body: %s", string(body))

		result := parseLogsResponse(body)
		g.Expect(result).NotTo(BeNil(), "Response should be parseable, body: %s", string(body))
		g.Expect(countFiles(result)).To(BeNumerically(">", 0), "No glob should return all files, got: %v", result)
		LogWithTimestamp(t, "No glob parameter correctly returned %d files across %d categories", countFiles(result), len(result))
	})

	test.T().Run("glob=raylet* matches exactly raylet.out and raylet.err", func(t *testing.T) {
		g := NewWithT(t)

		logsURL := fmt.Sprintf("%s%s?node_id=%s&glob=%s", historyServerURL, EndpointLogs, nodeID, url.QueryEscape("raylet*"))
		resp, err := client.Get(logsURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK, body: %s", string(body))

		result := parseLogsResponse(body)
		g.Expect(result).NotTo(BeNil(), "Response should be parseable, body: %s", string(body))

		// raylet* matches only raylet.out and raylet.err, so only the "raylet" category should be present.
		g.Expect(result).To(HaveLen(1), "Should only have the 'raylet' category, got: %v", result)
		rayletFiles, _ := result["raylet"].([]any)
		g.Expect(rayletFiles).To(ConsistOf("raylet.out", "raylet.err"),
			"raylet* should match exactly raylet.out and raylet.err")
		LogWithTimestamp(t, "glob=raylet* correctly returned %d raylet files", len(rayletFiles))
	})

	test.T().Run("glob pattern matching no files returns empty result", func(t *testing.T) {
		g := NewWithT(t)

		logsURL := fmt.Sprintf("%s%s?node_id=%s&glob=%s", historyServerURL, EndpointLogs, nodeID, url.QueryEscape("nonexistent-*.xyz"))
		resp, err := client.Get(logsURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK even when glob matches nothing, body: %s", string(body))

		result := parseLogsResponse(body)
		g.Expect(result).NotTo(BeNil(), "Response should be parseable, body: %s", string(body))
		g.Expect(countFiles(result)).To(Equal(0), "A glob matching no files should return an empty result")
		LogWithTimestamp(t, "Non-matching glob correctly returned 0 files")
	})

	test.T().Run("glob=events/event_JOBS* matches exactly one file in the events subdirectory", func(t *testing.T) {
		g := NewWithT(t)

		// glob=events/event_JOBS* should match only event_JOBS.log inside the events/ subdirectory.
		// Expected response:
		//   {"data":{"result":{"internal":["event_JOBS.log"]}},"msg":"","result":true}
		//
		// Always use the head node ID to avoid flakiness since events/event_JOBS.log is only present on the head node.
		// Ref: https://github.com/ray-project/ray/blob/20eae5b1/python/ray/dashboard/modules/job/job_head.py#L397-L399
		headNodeID := GetOneOfNodeID(g, client, historyServerURL, true)
		logsURL := fmt.Sprintf("%s%s?node_id=%s&glob=%s", historyServerURL, EndpointLogs, headNodeID, url.QueryEscape("events/event_JOBS*"))
		resp, err := client.Get(logsURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK, body: %s", string(body))

		result := parseLogsResponse(body)
		g.Expect(result).NotTo(BeNil(), "Response should be parseable, body: %s", string(body))

		// events/event_JOBS* maps to the "internal" category; no other category should be present.
		g.Expect(result).To(HaveLen(1), "Should only have the 'internal' category, got: %v", result)
		internalFiles, _ := result["internal"].([]any)
		g.Expect(internalFiles).To(ConsistOf("event_JOBS.log"), "glob=events/event_JOBS* should match exactly event_JOBS.log")
		LogWithTimestamp(t, "glob=events/event_JOBS* correctly returned %d file", len(internalFiles))
	})

	test.T().Run("glob=**/*.out recursively matches all .out files across all directories", func(t *testing.T) {
		g := NewWithT(t)

		// glob=**/*.out should recursively match all .out files in the log directory and any subdirectories.
		// Expected response contains .out files across multiple categories, e.g.:
		//   {"data":{"result":{"agent":[...],"autoscaler":[...],"dashboard":[...],...}},"msg":"","result":true}
		logsURL := fmt.Sprintf("%s%s?node_id=%s&glob=%s", historyServerURL, EndpointLogs, nodeID, url.QueryEscape("**/*.out"))
		resp, err := client.Get(logsURL)
		g.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(http.StatusOK), "Expected OK, body: %s", string(body))

		result := parseLogsResponse(body)
		g.Expect(result).NotTo(BeNil(), "Response should be parseable, body: %s", string(body))

		// **/*.out should match at least the well-known .out files at root level.
		totalFiles := countFiles(result)
		g.Expect(totalFiles).To(BeNumerically(">", 0), "**/*.out should return at least one file, got: %v", result)

		// Every returned file must end with .out regardless of which category it falls into.
		for category, files := range result {
			fileList, _ := files.([]any)
			for _, f := range fileList {
				g.Expect(f.(string)).To(HaveSuffix(".out"),
					"All files matched by **/*.out should end with .out, got %q in category %q", f, category)
			}
		}

		LogWithTimestamp(t, "glob=**/*.out correctly returned %d .out files across %d categories", totalFiles, len(result))
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster /api/v0/logs glob endpoint tests completed")
}

// parseLogsResponse parses the /api/v0/logs response body and returns the categorized
// file map from data.result. Returns nil on any parse failure.
func parseLogsResponse(body []byte) map[string]any {
	var resp map[string]any
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil
	}
	data, ok := resp["data"].(map[string]any)
	if !ok {
		return nil
	}
	result, ok := data["result"].(map[string]any)
	if !ok {
		return nil
	}
	return result
}

// countFiles counts the total number of file entries across all categories in the result map.
func countFiles(result map[string]any) int {
	total := 0
	for _, v := range result {
		if files, ok := v.([]any); ok {
			total += len(files)
		}
	}
	return total
}

// testTimelineEndpointLiveCluster verifies that the history server can return timeline data from a live cluster.
//
// The test follows these steps:
// 1. Create a RayCluster and submit a Ray job.
// 2. Deploy the History Server and ensure the cluster is listed as a live session.
// 3. Verify the /api/v0/tasks/timeline endpoint behavior for a live cluster:
//   - Without params: returns a non-empty Chrome Tracing JSON array with required metadata
//     and trace events.
//   - With job_id=<id>: returns only events for the specified job.
//   - With download=1: sets Content-Disposition to attachment and includes a filename.
//   - With download=1&job_id=<id>: filename includes the job_id.
//
// 4. Delete S3 bucket to ensure test isolation
func testTimelineEndpointLiveCluster(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	jobID := GetOneOfJobID(g, client, historyServerURL)

	test.T().Run("should return valid timeline data", func(t *testing.T) {
		g := NewWithT(t)
		verifyTimelineResponse(g, client, historyServerURL, "", false)
	})
	test.T().Run("with valid job_id returns filtered events", func(t *testing.T) {
		g := NewWithT(t)
		verifyTimelineResponse(g, client, historyServerURL, jobID, false)
	})

	test.T().Run("download=1 sets Content-Disposition and filename", func(t *testing.T) {
		g := NewWithT(t)
		verifyTimelineResponse(g, client, historyServerURL, "", true)
	})

	test.T().Run("download=1 with job_id sets filename with job_id", func(t *testing.T) {
		g := NewWithT(t)
		verifyTimelineResponse(g, client, historyServerURL, jobID, true)
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster timeline endpoint test completed")
}

// testTimelineEndpointDeadCluster verifies that the history server can serve task timeline data from S3
// after a Ray cluster is deleted, and that the timeline endpoint supports query parameters.
//
// The test follows these steps:
// 1. Create a RayCluster and submit a Ray job.
// 2. Delete the RayCluster to trigger event export/upload to S3 and wait until deletion completes.
// 3. Deploy the History Server and switch the client context to a non-live (archived) session.
// 4. Verify the /api/v0/tasks/timeline endpoint behavior:
//   - Without params: returns a non-empty Chrome Tracing JSON array with required metadata + trace events.
//   - With job_id=<id>: returns only events for the specified job (job_id may require hex->base64 normalization
//     depending on the source endpoint/session).
//   - With download=1: sets Content-Disposition to attachment and includes a filename.
//   - With download=1&job_id=<id>: filename includes the job_id.
//
// 5. Delete S3 bucket to ensure test isolation
func testTimelineEndpointDeadCluster(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Delete RayCluster to trigger event upload
	err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Deleted RayCluster %s/%s", namespace.Name, rayCluster.Name)

	// Wait for cluster to be fully deleted (ensures events are uploaded to S3)
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	jobID := GetOneOfJobID(g, client, historyServerURL)

	test.T().Run("should return timeline data from S3", func(t *testing.T) {
		g := NewWithT(t)
		verifyTimelineResponse(g, client, historyServerURL, "", false)
	})
	test.T().Run("with valid job_id returns filtered events", func(t *testing.T) {
		g := NewWithT(t)
		verifyTimelineResponse(g, client, historyServerURL, jobID, false)
	})

	test.T().Run("download=1 sets Content-Disposition and filename", func(t *testing.T) {
		g := NewWithT(t)
		verifyTimelineResponse(g, client, historyServerURL, "", true)
	})

	test.T().Run("download=1 with job_id sets filename with job_id", func(t *testing.T) {
		g := NewWithT(t)
		verifyTimelineResponse(g, client, historyServerURL, jobID, true)
	})
	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster timeline endpoint test completed")
}

// verifyTimelineResponse verifies the timeline endpoint returns valid Chrome Tracing format.
// jobID: optional filter; empty means no job_id query param.
// download: if true, adds download=1 and asserts Content-Disposition header and filename.
func verifyTimelineResponse(g *WithT, client *http.Client, historyServerURL string, jobID string, download bool) {
	baseURL := historyServerURL + "/api/v0/tasks/timeline"
	if jobID != "" || download {
		params := url.Values{}
		if jobID != "" {
			params.Set("job_id", jobID)
		}
		if download {
			params.Set("download", "1")
		}
		baseURL += "?" + params.Encode()
	}

	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(baseURL)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

		if download {
			cd := resp.Header.Get("Content-Disposition")
			gg.Expect(cd).To(ContainSubstring("attachment"), "Content-Disposition should contain attachment")
			gg.Expect(cd).To(ContainSubstring("filename="), "Content-Disposition should contain filename")
			if jobID != "" {
				gg.Expect(cd).To(ContainSubstring(jobID), "filename should contain job_id when job_id filter is set")
			}
		}

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())

		var events []map[string]any
		err = json.Unmarshal(body, &events)
		gg.Expect(err).NotTo(HaveOccurred())

		// Should have at least some events
		gg.Expect(events).ToNot(BeEmpty(), "Timeline should have at least one event")

		// Verify all the job_id are same
		if jobID != "" {
			for _, event := range events {
				ph, _ := event["ph"].(string)
				if ph != "X" {
					continue
				}
				args, ok := event["args"].(map[string]any)
				gg.Expect(ok).To(BeTrue(), "trace event should have args")
				jid, ok := args["job_id"]
				gg.Expect(ok).To(BeTrue(), "trace event args should have job_id")
				gg.Expect(jid).To(Equal(jobID),
					"when job_id filter is %q, every trace event's args.job_id must be the same, got %v", jobID, jid)
			}
		}

		// Verify metadata and trace events exist
		hasProcessName := false
		hasThreadName := false
		hasTraceEvent := false

		for _, event := range events {
			name, _ := event["name"].(string)
			ph, _ := event["ph"].(string)

			if name == "process_name" && ph == "M" {
				hasProcessName = true
				argsAny, exists := event["args"]
				gg.Expect(exists).To(BeTrue(), "process_name should have 'args' field")

				args, ok := argsAny.(map[string]any)
				gg.Expect(ok).To(BeTrue(), "process_name args should be a map[string]any")
				gg.Expect(args["name"]).NotTo(BeNil(), "process_name args should have 'name'")
			}
			if name == "thread_name" && ph == "M" {
				hasThreadName = true
			}
			if ph == "X" {
				hasTraceEvent = true
				// Verify trace event has required fields
				gg.Expect(event["cat"]).NotTo(BeNil(), "Trace event should have 'cat'")
				gg.Expect(event["ts"]).NotTo(BeNil(), "Trace event should have 'ts'")
				gg.Expect(event["dur"]).NotTo(BeNil(), "Trace event should have 'dur'")
				gg.Expect(event["args"]).NotTo(BeNil(), "Trace event should have 'args'")

				// Verify args structure
				argsAny, exists := event["args"]
				gg.Expect(exists).To(BeTrue(), "Trace event should have 'args' field")

				args, ok := argsAny.(map[string]any)
				gg.Expect(ok).To(BeTrue(), "Trace event args should be a map[string]any")
				gg.Expect(args["task_id"]).NotTo(BeNil(), "args should have 'task_id'")
				gg.Expect(args["job_id"]).NotTo(BeNil(), "args should have 'job_id'")
			}
		}

		gg.Expect(hasProcessName).To(BeTrue(), "Should have process_name metadata event")
		gg.Expect(hasThreadName).To(BeTrue(), "Should have thread_name metadata event")
		gg.Expect(hasTraceEvent).To(BeTrue(), "Should have at least one trace event")
	}, TestTimeoutShort).Should(Succeed())
}

// testLogicalActorsEndpointDeadCluster verifies that the history server can return actors from the
// in-memory ClusterActorMap after a cluster is deleted.
//
// Data flow explanation:
// The history server does not fetch actors directly from S3. Instead:
// 1. Collector pushes events to S3 on cluster deletion
// 2. Storage Reader reads event files from S3
// 3. Event Handler processes events and populates ClusterActorMap
// 4. The /logical/actors endpoint returns actors from the in-memory ClusterActorMap
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster (generates actor events)
// 3. Delete RayCluster to trigger log upload to S3 (and event processing)
// 4. Apply History Server and get its URL
// 5. Verify that the history server returns actors via /logical/actors endpoint
// 6. Verify that the history server returns a single actor via /logical/actors/{actor_id} endpoint
// 7. Delete S3 bucket to ensure test isolation
func testLogicalActorsEndpointDeadCluster(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Delete RayCluster to trigger log upload to S3
	err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Deleted RayCluster %s/%s", namespace.Name, rayCluster.Name)

	// Wait for cluster to be fully deleted (ensures logs are uploaded to S3 and events are processed)
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	test.T().Run("should return actors from history server", func(t *testing.T) {
		g := NewWithT(t)
		g.Eventually(func(gg Gomega) {
			resp, err := client.Get(historyServerURL + EndpointLogicalActors)
			gg.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			gg.Expect(err).NotTo(HaveOccurred())

			var result map[string]any
			err = json.Unmarshal(body, &result)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(result["result"]).To(BeTrue())
			gg.Expect(result["msg"]).To(Equal("All actors fetched."))

			// Verify data.actors exists and is a map
			data, ok := result["data"].(map[string]any)
			gg.Expect(ok).To(BeTrue())
			actors, ok := data["actors"].(map[string]any)
			gg.Expect(ok).To(BeTrue())
			gg.Expect(actors).ToNot(BeEmpty(), "should have at least one actor")

			// Verify actor schema matches formatActorForResponse format
			for _, actorData := range actors {
				actor, ok := actorData.(map[string]any)
				gg.Expect(ok).To(BeTrue(), "actor should be a map")
				gg.Expect(actor["actorId"]).NotTo(BeNil(), "actor should have actorId")
				gg.Expect(actor["jobId"]).NotTo(BeNil(), "actor should have jobId")
				gg.Expect(actor["state"]).NotTo(BeNil(), "actor should have state")
				gg.Expect(actor["address"]).NotTo(BeNil(), "actor should have address")
				address, ok := actor["address"].(map[string]any)
				gg.Expect(ok).To(BeTrue(), "address should be a map")
				gg.Expect(address["nodeId"]).NotTo(BeNil(), "address should have nodeId")
				gg.Expect(address["ipAddress"]).NotTo(BeNil(), "address should have ipAddress")
				break // Only verify the first actor
			}

			LogWithTimestamp(t, "Found %d actors from history server", len(actors))
		}, TestTimeoutShort).Should(Succeed())
	})

	test.T().Run("should return single actor from history server", func(t *testing.T) {
		g := NewWithT(t)

		actorID := GetOneOfActorID(g, client, historyServerURL)

		// Now test the single actor endpoint
		g.Eventually(func(gg Gomega) {
			singleActorURL := fmt.Sprintf("%s/logical/actors/%s", historyServerURL, actorID)
			resp, err := client.Get(singleActorURL)
			gg.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			gg.Expect(err).NotTo(HaveOccurred())

			var result map[string]any
			err = json.Unmarshal(body, &result)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(result["result"]).To(BeTrue())
			gg.Expect(result["msg"]).To(Equal("Actor fetched."))

			// Verify data.detail exists and contains actorId
			data, ok := result["data"].(map[string]any)
			gg.Expect(ok).To(BeTrue())
			detail, ok := data["detail"].(map[string]any)
			gg.Expect(ok).To(BeTrue())

			// Verify actor schema matches formatActorForResponse format
			gg.Expect(detail["actorId"]).To(Equal(actorID))
			gg.Expect(detail["jobId"]).NotTo(BeNil())
			gg.Expect(detail["state"]).NotTo(BeNil())
			gg.Expect(detail["address"]).NotTo(BeNil())
			address, ok := detail["address"].(map[string]any)
			gg.Expect(ok).To(BeTrue(), "address should be a map")
			gg.Expect(address["nodeId"]).NotTo(BeNil())
			gg.Expect(address["ipAddress"]).NotTo(BeNil())

			LogWithTimestamp(t, "Successfully fetched actor %s from history server", actorID)
		}, TestTimeoutShort).Should(Succeed())
	})

	test.T().Run("should handle non-existent actor", func(t *testing.T) {
		g := NewWithT(t)
		g.Eventually(func(gg Gomega) {
			fakeActorID := "ffffffffffffffffffffffffffffffffffffffff"
			singleActorURL := fmt.Sprintf("%s/logical/actors/%s", historyServerURL, fakeActorID)
			resp, err := client.Get(singleActorURL)
			gg.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			gg.Expect(err).NotTo(HaveOccurred())

			var result map[string]any
			err = json.Unmarshal(body, &result)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(result["result"]).To(BeFalse())
			gg.Expect(result["msg"]).To(Equal("Actor not found."))
		}, TestTimeoutShort).Should(Succeed())
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster logical actors endpoint tests completed")
}

// testLiveClusterTasks verifies that the /v0/tasks?detail=1 endpoint for a live cluster will return the
// detailed task information of all task attempts.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Apply History Server and get its URL
// 4. Get the cluster information and set the cluster context with the session name 'live'
// 5. Hit /api/v0/tasks?detail=1 to get the detailed task information of all task attempts
// 6. Verify the response status code is 200
// 7. Verify the response API schema
// 8. Delete S3 bucket to ensure test isolation
func testLiveClusterTasks(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	endpoint := EndpointTasks + "?detail=1"

	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	endpointURL := historyServerURL + endpoint
	LogWithTimestamp(test.T(), "Testing %s endpoint for live cluster: %s", endpoint, endpointURL)

	var tasksResp map[string]any
	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(endpointURL)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(resp.StatusCode).To(Equal(http.StatusOK),
			"[GET] %s should return 200, got %d: %s", endpointURL, resp.StatusCode, string(body))

		err = json.Unmarshal(body, &tasksResp)
		gg.Expect(err).NotTo(HaveOccurred())
	}, TestTimeoutShort).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying /api/v0/tasks?detail=1 response schema for live cluster")
	verifyTasksRespSchema(test, g, tasksResp, true)

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster /api/v0/tasks?detail=1 tests completed successfully")
}

// testDeadClusterTasks verifies that the /api/v0/tasks endpoint for a dead cluster returns
// detailed task information reconstructed from historical events.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing cluster and wait for completion
// 3. Delete the Ray cluster to trigger event flushing and wait for cluster deletion to complete
// 4. Apply History Server and get its URL
// 5. Get the cluster information and set the cluster context with the session name of the dead cluster
// 6. Run /api/v0/tasks endpoint with various query parameters to test limit, detail, exclude_driver, and filters
// 7. Verify the response status code is 200
// 8. Verify the response API schema
// 9. Delete S3 bucket to ensure test isolation
//
// NOTE: timeout is not tested because tasks are in-memory and retrieval is typically fast.
func testDeadClusterTasks(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Delete the Ray cluster to trigger event flushing.
	LogWithTimestamp(test.T(), "Deleting RayCluster %s/%s to trigger event flushing", rayCluster.Namespace, rayCluster.Name)
	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(SatisfyAll(Not(BeEmpty()), Not(Equal(LiveSessionName))))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	verifyDeadClusterTaskLogInfo(g, client, historyServerURL)

	jobIDs := getAllEligibleJobIDs(g, client, historyServerURL)
	jobIDForFilter := jobIDs[0]

	verifyTasksEndpoint := func(
		t *testing.T,
		tcName,
		queryParams string,
		detail bool,
		expectedStatus int,
	) {
		g := NewWithT(t)

		url := historyServerURL + EndpointTasks + "?" + queryParams
		resp, err := client.Get(url)
		g.Expect(err).NotTo(HaveOccurred())
		defer func() {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()

		body, err := io.ReadAll(resp.Body)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(resp.StatusCode).To(Equal(expectedStatus),
			"Test case '%s' failed: expected %d, got %d", tcName, expectedStatus, resp.StatusCode)

		if resp.StatusCode == http.StatusOK {
			var tasksResp map[string]any
			err = json.Unmarshal(body, &tasksResp)
			g.Expect(err).NotTo(HaveOccurred())

			LogWithTimestamp(test.T(), "Verifying %s response schema for dead cluster", url)
			verifyTasksRespSchema(test, g, tasksResp, detail)
		}
	}

	filterTestCases := []struct {
		name           string
		queryParams    string
		expectedStatus int
	}{
		// Basic / default
		{"no params (default)", "", http.StatusOK},

		// limit
		{"limit=1", "limit=1", http.StatusOK},
		{"limit=100", "limit=100", http.StatusOK},
		{"limit=10000", "limit=10000", http.StatusOK},
		{"limit=0", "limit=0", http.StatusOK},
		{"invalid limit (string)", "limit=abc", http.StatusBadRequest},

		// exclude_driver
		{"exclude_driver=true", "exclude_driver=true", http.StatusOK},
		{"exclude_driver=false", "exclude_driver=false", http.StatusOK},
		{"invalid exclude_driver (string)", "exclude_driver=invalid", http.StatusBadRequest},

		// Single filter
		{"state=FINISHED", "filter_keys=state&filter_predicates==&filter_values=FINISHED", http.StatusOK},
		{"state!=PENDING_ARGS_AVAIL", "filter_keys=state&filter_predicates=!=&filter_values=PENDING_ARGS_AVAIL", http.StatusOK},
		{fmt.Sprintf("job_id=%s", jobIDForFilter), "filter_keys=job_id&filter_predicates==&filter_values=" + jobIDForFilter, http.StatusOK},

		// Multiple filters
		{"statte=FINISHED & type=ACTOR_TASK", "filter_keys=state&filter_keys=task_type&filter_predicates==&filter_predicates==&filter_values=FINISHED&filter_values=ACTOR_TASK", http.StatusOK},

		// Invalid filters
		{"len(filter_keys) != len(filter_values)", "filter_keys=state&filter_keys=job_id&filter_predicates==&filter_values=FINISHED", http.StatusBadRequest},
		{"filter_keys only (missing predicates and values)", "filter_keys=state", http.StatusBadRequest},

		// Combined
		{"limit=5 & detail=true", "limit=5&detail=true", http.StatusOK},
		{"limit=5 & exclude_driver=true", "limit=5&exclude_driver=true", http.StatusOK},
		{"limit=5 & detail=true & exclude_driver=false", "limit=5&detail=true&exclude_driver=false", http.StatusOK},
		{"limit=10 & state=FINISHED", "limit=10&filter_keys=state&filter_predicates==&filter_values=FINISHED", http.StatusOK},
		{"limit=10 & detail=true & exclude_driver=false & state=FINISHED", "limit=10&detail=true&exclude_driver=false&filter_keys=state&filter_predicates==&filter_values=FINISHED", http.StatusOK},
	}
	for _, tc := range filterTestCases {
		test.T().Run(tc.name, func(t *testing.T) {
			verifyTasksEndpoint(t, tc.name, tc.queryParams, false, tc.expectedStatus)
		})
	}

	detailTestCases := []struct {
		name           string
		queryParams    string
		detail         bool
		expectedStatus int
	}{
		{"detail=true", "detail=true", true, http.StatusOK},
		{"detail=false", "detail=false", false, http.StatusOK},
		{"detail=1", "detail=1", true, http.StatusOK},
		{"invalid detail (string)", "detail=invalid", false, http.StatusBadRequest},
	}
	for _, tc := range detailTestCases {
		test.T().Run(tc.name, func(t *testing.T) {
			verifyTasksEndpoint(t, tc.name, tc.queryParams, tc.detail, tc.expectedStatus)
		})
	}

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster /api/v0/tasks tests completed successfully")
}

func verifyDeadClusterTaskLogInfo(g *WithT, client *http.Client, historyServerURL string) {
	resp, err := client.Get(historyServerURL + EndpointTasks + "?detail=1")
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))

	body, err := io.ReadAll(resp.Body)
	g.Expect(err).NotTo(HaveOccurred())
	var response map[string]any
	g.Expect(json.Unmarshal(body, &response)).To(Succeed())

	data, ok := response["data"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	result, ok := data["result"].(map[string]any)
	g.Expect(ok).To(BeTrue())
	tasks, ok := result["result"].([]any)
	g.Expect(ok).To(BeTrue())

	var taskID, nodeID, stdoutFile string
	for _, value := range tasks {
		task, ok := value.(map[string]any)
		if !ok || task["name"] != "my_task" || task["type"] != "NORMAL_TASK" || task["state"] != "FINISHED" {
			continue
		}
		workerID, ok := task["worker_id"].(string)
		if !ok || workerID == "" {
			continue
		}
		taskLogInfo, ok := task["task_log_info"].(map[string]any)
		if !ok {
			continue
		}
		stdoutFileValue, _ := taskLogInfo["stdout_file"].(string)
		stdoutStart, startOK := taskLogInfo["stdout_start"].(float64)
		stdoutEnd, endOK := taskLogInfo["stdout_end"].(float64)
		if stdoutFileValue != "" && startOK && endOK && stdoutStart > 0 && stdoutEnd > stdoutStart {
			taskID, _ = task["task_id"].(string)
			nodeID, _ = task["node_id"].(string)
			stdoutFile = stdoutFileValue
			break
		}
	}
	g.Expect(taskID).NotTo(BeEmpty(),
		"completed my_task should expose a complete stdout byte range")
	g.Expect(nodeID).NotTo(BeEmpty())

	workerLogURL := fmt.Sprintf("%s%s?node_id=%s&filename=%s&lines=-1",
		historyServerURL,
		EndpointLogsFile,
		url.QueryEscape(nodeID),
		url.QueryEscape(path.Base(stdoutFile)),
	)
	workerLogResp, err := client.Get(workerLogURL)
	g.Expect(err).NotTo(HaveOccurred())
	defer workerLogResp.Body.Close()
	g.Expect(workerLogResp.StatusCode).To(Equal(http.StatusOK))
	workerLogBody, err := io.ReadAll(workerLogResp.Body)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(string(workerLogBody)).To(ContainSubstring("Processing 0"))
	g.Expect(string(workerLogBody)).To(ContainSubstring("Processing 1"))
	g.Expect(string(workerLogBody)).To(ContainSubstring("Processing 2"))

	logURL := fmt.Sprintf("%s%s?task_id=%s&suffix=out&lines=-1",
		historyServerURL, EndpointLogsFile, url.QueryEscape(taskID))
	logResp, err := client.Get(logURL)
	g.Expect(err).NotTo(HaveOccurred())
	defer logResp.Body.Close()
	g.Expect(logResp.StatusCode).To(Equal(http.StatusOK))
	logBody, err := io.ReadAll(logResp.Body)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(string(logBody)).To(ContainSubstring("Processing 1"))
	g.Expect(string(logBody)).NotTo(ContainSubstring("Processing 0"))
	g.Expect(string(logBody)).NotTo(ContainSubstring("Processing 2"))
}

// testLiveClusterNodes verifies that the /nodes?view=summary endpoint for a live cluster will return the current
// snapshot containing node summary and resource usage information.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing cluster and wait for completion
// 3. Apply History Server and get its URL
// 4. Get the cluster information and set the cluster context with the session name 'live'
// 5. Hit /nodes?view=summary to get the current snapshot containing node summary and resource usage information
// 6. Verify the response status code is 200
// 7. Verify the response API schema
// 8. Delete S3 bucket to ensure test isolation
func testLiveClusterNodes(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	// Explicitly specify the view parameter to get the current snapshot.
	// If the view parameter is not specified, the following error will be returned:
	// {"result": false, "msg": "Unknown view None", "data": {}}
	// Ref: https://github.com/ray-project/kuberay/pull/4412.
	endpoint := "/nodes?view=summary"

	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	LogWithTimestamp(test.T(), "Verifying /nodes response schema for live cluster (isLive=true)")
	endpointURL := historyServerURL + endpoint
	verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
		verifyNodesRespSchema(test, g, data, true)
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster /nodes?view=summary tests completed successfully")
}

// testDeadClusterNodes verifies that the /nodes endpoint for a dead cluster will return the historical replay
// containing node summary and resource usage snapshots of a cluster session.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing cluster and wait for completion
// 3. Delete the Ray cluster to trigger event flushing and wait for cluster deletion to complete
// 4. Apply History Server and get its URL
// 5. Get the cluster information and set the cluster context with the session name of the dead cluster
// 6. Hit /nodes endpoint to get the historical replay containing node summary and resource usage snapshots
// 7. Verify the response status code is 200
// 8. Verify the response API schema
// 9. Delete S3 bucket to ensure test isolation
func testDeadClusterNodes(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Delete the Ray cluster to trigger event flushing.
	LogWithTimestamp(test.T(), "Deleting RayCluster %s/%s to trigger event flushing", rayCluster.Namespace, rayCluster.Name)
	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(SatisfyAll(Not(BeEmpty()), Not(Equal(LiveSessionName))))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpointURL := historyServerURL + EndpointNodes
	LogWithTimestamp(test.T(), "Testing %s endpoint for dead cluster: %s", EndpointNodes, endpointURL)

	LogWithTimestamp(test.T(), "Verifying /nodes?view=summary response schema for dead cluster (isLive=false)")
	verifySingleEndpoint(test, g, client, endpointURL+"?view=summary", func(test Test, g *WithT, data map[string]any) {
		verifyNodesRespSchema(test, g, data, false)
	})

	LogWithTimestamp(test.T(), "Verifying /nodes?view=hostNameList response schema for dead cluster (isLive=false)")
	verifySingleEndpoint(test, g, client, endpointURL+"?view=hostNameList", func(test Test, g *WithT, data map[string]any) {
		verifyNodesHostNameListSchema(test, g, data, false)
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster /nodes tests completed successfully")
}

// testLiveClusterNode verifies that the /nodes/{node_id} endpoint for a live cluster will return the current snapshot
// containing node summary of the specified node.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing cluster and wait for completion
// 3. Get the head and worker node IDs
// 4. Apply History Server and get its URL
// 5. Get the cluster information and set the cluster context with the session name 'live'
// 6. Hit /nodes/{node_id} for both the head node and the worker node:
//   - Get the node details of the specified node
//   - Verify the response status code is 200
//   - Verify the response API schema
//
// 7. Delete S3 bucket to ensure test isolation
func testLiveClusterNode(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	headNodeID := GetNodeIDFromPod(test, g, HeadPod(test, rayCluster), "ray-head")
	workerNodeID := GetNodeIDFromPod(test, g, FirstWorkerPod(test, rayCluster), "ray-worker")

	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	for _, nodeId := range []string{headNodeID, workerNodeID} {
		LogWithTimestamp(test.T(), "Verifying /nodes/%s response schema for live cluster (isLive=true)", nodeId)

		endpoint := fmt.Sprintf("%s/%s", EndpointNodes, nodeId)
		endpointURL := historyServerURL + endpoint
		verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
			verifyNodeRespSchema(test, g, data, true)
		})
	}

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster /nodes/{node_id} tests completed successfully")
}

// testDeadClusterNode verifies that the /nodes/{node_id} endpoint for a dead cluster will return the historical replay
// containing node summary snapshots of the specified node in a cluster session.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing cluster and wait for completion
// 3. Get the head and worker node IDs
// 4. Delete the Ray cluster to trigger event flushing and wait for cluster deletion to complete
// 5. Apply History Server and get its URL
// 6. Get the cluster information and set the cluster context with the session name of the dead cluster
// 7. Hit /nodes/{node_id} for both the head node and the worker node:
//   - Get the node details of the specified node
//   - Verify the response status code is 200
//   - Verify the response API schema
//
// 8. Delete S3 bucket to ensure test isolation
func testDeadClusterNode(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	headNodeID := GetNodeIDFromPod(test, g, HeadPod(test, rayCluster), "ray-head")
	workerNodeID := GetNodeIDFromPod(test, g, FirstWorkerPod(test, rayCluster), "ray-worker")

	// Delete the Ray cluster to trigger event flushing.
	LogWithTimestamp(test.T(), "Deleting RayCluster %s/%s to trigger event flushing", rayCluster.Namespace, rayCluster.Name)
	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(SatisfyAll(Not(BeEmpty()), Not(Equal(LiveSessionName))))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	for _, nodeId := range []string{headNodeID, workerNodeID} {
		LogWithTimestamp(test.T(), "Verifying /nodes/%s response schema for dead cluster (isLive=false)", nodeId)

		endpoint := fmt.Sprintf("%s/%s", EndpointNodes, nodeId)
		endpointURL := historyServerURL + endpoint
		verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
			verifyNodeRespSchema(test, g, data, false)
		})
	}

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster /nodes/{node_id} tests completed successfully")
}

// testLiveClusterMetadata verifies that the /api/v0/cluster_metadata endpoint proxies to the
// live Ray Dashboard and returns valid cluster metadata.
func testLiveClusterMetadata(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpoint := "/api/v0/cluster_metadata"
	LogWithTimestamp(test.T(), "Testing live cluster endpoint: %s", endpoint)

	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(historyServerURL + endpoint)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(resp.StatusCode).To(Equal(http.StatusOK),
			"Endpoint %s should return 200, got %d: %s", endpoint, resp.StatusCode, string(body))

		var metadata map[string]any
		err = json.Unmarshal(body, &metadata)
		gg.Expect(err).NotTo(HaveOccurred(), "Cluster metadata should be valid JSON")
		gg.Expect(metadata).To(HaveKey("data"), "Cluster metadata should contain data field")
		data, ok := metadata["data"].(map[string]any)
		gg.Expect(ok).To(BeTrue(), "data field should be a JSON object")
		gg.Expect(data).To(HaveKey("rayVersion"))
		gg.Expect(data).To(HaveKey("pythonVersion"))
		LogWithTimestamp(test.T(), "Live cluster metadata: %s", string(body))
	}, TestTimeoutShort).Should(Succeed())

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster /api/v0/cluster_metadata test completed successfully")
}

// testDeadClusterMetadata verifies that the /api/v0/cluster_metadata endpoint returns stored
// cluster metadata from S3 for a dead (deleted) cluster.
func testDeadClusterMetadata(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)

	// Wait for cluster metadata to be stored in S3 by the collector before deleting the cluster.
	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	storageKey := utils.EndpointPathToStorageKey("/api/v0/cluster_metadata")
	sessionDir := clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID)
	metaKey := fmt.Sprintf("%s/%s/%s", sessionDir, utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME, storageKey)
	LogWithTimestamp(test.T(), "Waiting for cluster metadata to appear at S3 key: %s", metaKey)

	g.Eventually(func(gg Gomega) {
		gg.Expect(s3Client.StatObject(S3BucketName, metaKey)).To(Succeed())
	}, TestTimeoutMedium).Should(Succeed())

	// Delete the Ray cluster.
	LogWithTimestamp(test.T(), "Deleting RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)
	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	// Deploy the history server and query the dead cluster metadata.
	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(SatisfyAll(Not(BeEmpty()), Not(Equal(LiveSessionName))))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpoint := "/api/v0/cluster_metadata"
	LogWithTimestamp(test.T(), "Testing dead cluster endpoint: %s", endpoint)

	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(historyServerURL + endpoint)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(resp.StatusCode).To(Equal(http.StatusOK),
			"Endpoint %s should return 200, got %d: %s", endpoint, resp.StatusCode, string(body))

		var metadata map[string]any
		err = json.Unmarshal(body, &metadata)
		gg.Expect(err).NotTo(HaveOccurred(), "Cluster metadata should be valid JSON")
		gg.Expect(metadata).To(HaveKey("data"), "Cluster metadata should contain data field")
		data, ok := metadata["data"].(map[string]any)
		gg.Expect(ok).To(BeTrue(), "data field should be a JSON object")
		gg.Expect(data).To(HaveKey("rayVersion"))
		gg.Expect(data).To(HaveKey("pythonVersion"))
		LogWithTimestamp(test.T(), "Dead cluster metadata: %s", string(body))
	}, TestTimeoutShort).Should(Succeed())

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster /api/v0/cluster_metadata test completed successfully")
}

// testDeadClusterPlacementGroups verifies that the /api/v0/placement_groups endpoint returns
// stored placement groups data from S3 for a dead (deleted) cluster.
//
// This endpoint is served by the getFetchedEndpoint fallback handler (/{subpath:*}),
// which reads the data from S3 at {sessionName}/fetched_endpoints/restful__api__v0__placement_groups.
//
// The test flow mirrors testDeadClusterMetadata:
// 1. Deploy a cluster with the collector
// 2. Submit a RayJob that creates a detached placement group
// 3. Wait for placement groups data to appear in S3 (written by periodic endpoint polling)
// 4. Delete the cluster
// 5. Deploy the history server and query /api/v0/placement_groups
// 6. Verify the response is valid JSON with a non-empty placement_groups list
func testDeadClusterPlacementGroups(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)

	// Submit a RayJob that creates a detached placement group named "test_pg".
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Wait for placement groups data to be stored in S3 by the collector before deleting the cluster.
	// The collector stores the endpoint with query params, so the storage key includes them.
	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	storageKey := utils.EndpointPathToStorageKey("/api/v0/placement_groups?detail=1&limit=10000")
	sessionDir := clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID)
	pgKey := fmt.Sprintf("%s/%s/%s", sessionDir, utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME, storageKey)
	LogWithTimestamp(test.T(), "Waiting for placement groups data to appear at S3 key: %s", pgKey)

	g.Eventually(func(gg Gomega) {
		gg.Expect(s3Client.StatObject(S3BucketName, pgKey)).To(Succeed())
	}, TestTimeoutMedium).Should(Succeed())

	// Delete the Ray cluster.
	LogWithTimestamp(test.T(), "Deleting RayCluster %s/%s", rayCluster.Namespace, rayCluster.Name)
	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	// Deploy the history server and query the dead cluster placement groups.
	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(SatisfyAll(Not(BeEmpty()), Not(Equal(LiveSessionName))))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	// Use the same query params as the frontend to match the storage key.
	endpoint := "/api/v0/placement_groups?detail=1&limit=10000"
	LogWithTimestamp(test.T(), "Testing dead cluster endpoint: %s", endpoint)

	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(historyServerURL + endpoint)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(resp.StatusCode).To(Equal(http.StatusOK),
			"Endpoint %s should return 200, got %d: %s", endpoint, resp.StatusCode, string(body))

		var response map[string]any
		err = json.Unmarshal(body, &response)
		gg.Expect(err).NotTo(HaveOccurred(), "Placement groups response should be valid JSON")
		// The Ray State API v2 returns {"result": true, "msg": "", "data": {"result": {"total": N, "result": [...], ...}}}.
		// The history server preserves this envelope and backfills bundles within each
		// placement group when the stored response omits that frontend-required field.
		gg.Expect(response).To(HaveKey("result"), "Placement groups response should contain result field")
		gg.Expect(response["result"]).To(BeTrue(), "result field should be true")
		gg.Expect(response).To(HaveKey("data"), "Placement groups response should contain data field")
		data, ok := response["data"].(map[string]any)
		gg.Expect(ok).To(BeTrue(), "data field should be a JSON object")
		gg.Expect(data).To(HaveKey("result"), "data should contain result field")
		resultObj, ok := data["result"].(map[string]any)
		gg.Expect(ok).To(BeTrue(), "data.result field should be a JSON object")
		gg.Expect(resultObj).To(HaveKey("result"), "data.result should contain result field")

		pgList, ok := resultObj["result"].([]any)
		gg.Expect(ok).To(BeTrue(), "data.result.result should be a JSON array")
		gg.Expect(pgList).NotTo(BeEmpty(), "placement groups list should not be empty (RayJob creates a detached PG)")
		LogWithTimestamp(test.T(), "Dead cluster placement groups: %s", string(body))
	}, TestTimeoutShort).Should(Succeed())

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster /api/v0/placement_groups test completed successfully")
}

// testLiveClusterTaskSummarize verifies that the /api/v0/tasks/summarize?summary_by=lineage endpoint
// for a live cluster returns a valid lineage tree.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Apply History Server and get its URL
// 4. Get the cluster information and set the cluster context with the session name 'live'
// 5. Hit /api/v0/tasks/summarize?summary_by=lineage to get the lineage tree
// 6. Verify the response status code is 200
// 7. Verify the response API schema
// 8. Delete S3 bucket to ensure test isolation
func testLiveClusterTaskSummarize(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	endpoint := EndpointTasksSummarize + "?summary_by=lineage"
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpointURL := historyServerURL + endpoint
	LogWithTimestamp(test.T(), "Testing %s endpoint for live cluster: %s", endpoint, endpointURL)
	verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
		verifyTaskSummarizeLineageRespSchema(test, g, data)
	})
	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster /api/v0/tasks/summarize?summary_by=lineage tests completed successfully")
}

// testDeadClusterTaskSummarize verifies that the /api/v0/tasks/summarize?summary_by=lineage endpoint
// for a dead cluster returns a valid lineage tree built from historical events.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing cluster and wait for completion
// 3. Delete the Ray cluster to trigger event flushing and wait for cluster deletion to complete
// 4. Apply History Server and get its URL
// 5. Get the cluster information and set the cluster context with the session name of the dead cluster
// 6. Hit /api/v0/tasks/summarize?summary_by=lineage to get the lineage tree
// 7. Verify the response status code is 200
// 8. Verify the response API schema
// 9. Delete S3 bucket to ensure test isolation
func testDeadClusterTaskSummarize(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	endpoint := EndpointTasksSummarize + "?summary_by=lineage"
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	// Delete the Ray cluster to trigger event flushing.
	LogWithTimestamp(test.T(), "Deleting RayCluster %s/%s to trigger event flushing", rayCluster.Namespace, rayCluster.Name)
	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)
	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(SatisfyAll(Not(BeEmpty()), Not(Equal(LiveSessionName))))
	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpointURL := historyServerURL + endpoint
	LogWithTimestamp(test.T(), "Testing %s endpoint for dead cluster: %s", endpoint, endpointURL)

	verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
		verifyTaskSummarizeLineageRespSchema(test, g, data)
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster /api/v0/tasks/summarize?summary_by=lineage tests completed successfully")
}

// testLiveClusterTaskSummarizeFuncName verifies that the /api/v0/tasks/summarize endpoint (default func_name mode)
// for a live cluster returns a valid task summary grouped by function name.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Apply History Server and get its URL
// 4. Get the cluster information and set the cluster context with the session name 'live'
// 5. Hit /api/v0/tasks/summarize to get the func_name summary
// 6. Verify the response status code is 200
// 7. Verify the response API schema
// 8. Delete S3 bucket to ensure test isolation
func testLiveClusterTaskSummarizeFuncName(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	endpoint := EndpointTasksSummarize
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")
	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	endpointURL := historyServerURL + endpoint
	LogWithTimestamp(test.T(), "Testing %s endpoint for live cluster: %s", endpoint, endpointURL)
	verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
		verifyTaskSummarizeFuncNameRespSchema(test, g, data)
	})
	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster /api/v0/tasks/summarize (func_name) tests completed successfully")
}

// testDeadClusterTaskSummarizeFuncName verifies that the /api/v0/tasks/summarize endpoint (default func_name mode)
// for a dead cluster returns a valid task summary grouped by function name built from historical events.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing cluster and wait for completion
// 3. Delete the Ray cluster to trigger event flushing and wait for cluster deletion to complete
// 4. Apply History Server and get its URL
// 5. Get the cluster information and set the cluster context with the session name of the dead cluster
// 6. Hit /api/v0/tasks/summarize to get the func_name summary
// 7. Verify the response status code is 200
// 8. Verify the response API schema
// 9. Delete S3 bucket to ensure test isolation
func testDeadClusterTaskSummarizeFuncName(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	endpoint := EndpointTasksSummarize

	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Delete the Ray cluster to trigger event flushing.
	LogWithTimestamp(test.T(), "Deleting RayCluster %s/%s to trigger event flushing", rayCluster.Namespace, rayCluster.Name)
	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(SatisfyAll(Not(BeEmpty()), Not(Equal(LiveSessionName))))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpointURL := historyServerURL + endpoint
	LogWithTimestamp(test.T(), "Testing %s endpoint for dead cluster: %s", endpoint, endpointURL)

	verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
		verifyTaskSummarizeFuncNameRespSchema(test, g, data)
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster /api/v0/tasks/summarize (func_name) tests completed successfully")
}

// setClusterContext sets the cluster context via /enter_cluster/ endpoint and verifies the response.
func setClusterContext(test Test, g *WithT, client *http.Client, historyServerURL, namespace, clusterName, session string) {
	enterURL := fmt.Sprintf("%s/enter_cluster/%s/raycluster/%s/%s", historyServerURL, namespace, clusterName, session)
	LogWithTimestamp(test.T(), "Setting cluster context: %s", enterURL)

	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(enterURL)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())

		var result map[string]any
		err = json.Unmarshal(body, &result)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(result["result"]).To(Equal("success"))
		gg.Expect(result["name"]).To(Equal(clusterName))
		gg.Expect(result["namespace"]).To(Equal(namespace))
		gg.Expect(result["session"]).To(Equal(session))
	}, TestTimeoutShort).Should(Succeed())
}

// verifyHistoryServerEndpoints tests all history server endpoints
func verifyHistoryServerEndpoints(test Test, g *WithT, client *http.Client, historyServerURL string) {
	for _, endpoint := range HistoryServerEndpoints {
		LogWithTimestamp(test.T(), "Testing history server endpoint: %s", endpoint)
		g.Eventually(func(gg Gomega) {
			resp, err := client.Get(historyServerURL + endpoint)
			gg.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(resp.StatusCode).To(Equal(http.StatusOK),
				"Endpoint %s should return 200, got %d: %s", endpoint, resp.StatusCode, string(body))

			LogWithTimestamp(test.T(), "Endpoint %s returned status %d", endpoint, resp.StatusCode)
		}, TestTimeoutShort).Should(Succeed())
	}
}

// verifyHistoryServerGrafanaHealthEndpoint tests the /api/grafana_health endpoint
func verifyHistoryServerGrafanaHealthEndpoint(test Test, g *WithT, client *http.Client, historyServerURL string, sessionID string) {
	endpoint := HistoryServerEndpointGrafanaHealth
	LogWithTimestamp(test.T(), "Testing history server endpoint: %s", endpoint)

	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(historyServerURL + endpoint)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(resp.StatusCode).To(Equal(200),
			"Endpoint %s should return 200, got %d: %s", endpoint, resp.StatusCode, string(body))

		gg.Expect(body).To(MatchJSON(fmt.Sprintf(HistoryServerGrafanaHealthResponse, RayGrafanaIframeHost, sessionID)))
		LogWithTimestamp(test.T(), "Endpoint %s returned status %d", endpoint, resp.StatusCode)
	}, TestTimeoutShort).Should(Succeed())
}

// verifyHistoryServerPrometheusHealthEndpoint tests the /api/prometheus_health endpoint
func verifyHistoryServerPrometheusHealthEndpoint(test Test, g *WithT, client *http.Client, historyServerURL string) {
	endpoint := HistoryServerEndpointPrometheusHealth
	LogWithTimestamp(test.T(), "Testing history server endpoint: %s", endpoint)

	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(historyServerURL + endpoint)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(resp.StatusCode).To(Equal(200),
			"Endpoint %s should return 200, got %d: %s", endpoint, resp.StatusCode, string(body))

		var result map[string]any
		err = json.Unmarshal(body, &result)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(result["result"]).To(BeTrue(), "Response should have result=true")
		gg.Expect(result["msg"]).To(ContainSubstring("prometheus running"), "Response message should contain 'prometheus running'")
		LogWithTimestamp(test.T(), "Endpoint %s returned status %d with valid response", endpoint, resp.StatusCode)
	}, TestTimeoutShort).Should(Succeed())
}

// getClusterFromList retrieves a cluster from the /clusters/ endpoint by name and namespace.
func getClusterFromList(test Test, g *WithT, historyServerURL, clusterName, namespace string) *utils.ClusterInfo {
	LogWithTimestamp(test.T(), "Getting cluster %s/%s from /clusters/ endpoint", namespace, clusterName)

	client := CreateHTTPClientWithCookieJar(test, g)
	var result *utils.ClusterInfo
	g.Eventually(func(gg Gomega) {
		result = nil // Reset to avoid stale value from previous iteration
		resp, err := client.Get(historyServerURL + "/clusters/")
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())

		var clusters []utils.ClusterInfo
		err = json.Unmarshal(body, &clusters)
		gg.Expect(err).NotTo(HaveOccurred())

		for i, c := range clusters {
			if c.Name == clusterName && c.Namespace == namespace {
				result = &clusters[i]
				break
			}
		}
		gg.Expect(result).NotTo(BeNil(), "Cluster %s/%s should be in the list", namespace, clusterName)
		LogWithTimestamp(test.T(), "Found cluster: %s/%s with sessionName=%s",
			result.Namespace, result.Name, result.SessionName)
	}, TestTimeoutMedium).Should(Succeed())

	return result
}

// getAllEligibleJobIDs retrieves all job IDs from the /api/v0/tasks endpoint for the task filtering test cases.
func getAllEligibleJobIDs(g *WithT, client *http.Client, historyServerURL string) []string {
	var jobIDs []string

	resp, err := client.Get(historyServerURL + EndpointTasks)
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()

	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))
	body, err := io.ReadAll(resp.Body)
	g.Expect(err).NotTo(HaveOccurred())

	var tasksResp map[string]any
	err = json.Unmarshal(body, &tasksResp)
	g.Expect(err).NotTo(HaveOccurred())

	tasksData, ok := tasksResp["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data' should be a map")
	tasksDataResult, ok := tasksData["result"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'result' should be a map")
	formattedTasks, ok := tasksDataResult["result"].([]any)
	g.Expect(ok).To(BeTrue(), "'result' should be an array")
	for _, formattedTask := range formattedTasks {
		task, ok := formattedTask.(map[string]any)
		g.Expect(ok).To(BeTrue(), "formattedTask should be a map")
		g.Expect(task).To(HaveKey("job_id"))
		jobID, ok := task["job_id"].(string)
		g.Expect(ok).To(BeTrue(), "job_id should be a string")
		if jobID == "" {
			continue
		}
		jobIDs = append(jobIDs, jobID)
	}
	g.Expect(jobIDs).ToNot(BeEmpty(), "should have at least one eligible job ID")

	return jobIDs
}

// verifyTasksRespSchema verifies that the /v0/tasks response is valid according to the API schema.
func verifyTasksRespSchema(test Test, g *WithT, tasksResp map[string]any, detail bool) {
	// Verify top-level fields.
	g.Expect(tasksResp).To(HaveKeyWithValue("result", BeTrue()))
	g.Expect(tasksResp).To(HaveKeyWithValue("msg", Equal("")))
	g.Expect(tasksResp).To(HaveKey("data"))

	tasksData, ok := tasksResp["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data' should be a map")
	g.Expect(tasksData).To(HaveKey("result"))

	tasksDataResult, ok := tasksData["result"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'result' should be a map")
	g.Expect(tasksDataResult).To(HaveKey("total"))
	g.Expect(tasksDataResult).To(HaveKey("result"))
	g.Expect(tasksDataResult).To(HaveKey("num_after_truncation"))
	g.Expect(tasksDataResult).To(HaveKey("num_filtered"))
	g.Expect(tasksDataResult).To(HaveKey("partial_failure_warning"))
	g.Expect(tasksDataResult).To(HaveKey("warnings"))

	// Verify formatted task fields.
	formattedTasks, ok := tasksDataResult["result"].([]any)
	g.Expect(ok).To(BeTrue(), "'result' should be an array")
	LogWithTimestamp(test.T(), "Verifying formatted task fields for each task attempt")
	for _, formattedTask := range formattedTasks {
		formattedTaskMap, ok := formattedTask.(map[string]any)
		g.Expect(ok).To(BeTrue(), "formattedTask should be a map")
		g.Expect(formattedTaskMap).To(HaveKey("task_id"))
		g.Expect(formattedTaskMap).To(HaveKey("attempt_number"))
		g.Expect(formattedTaskMap).To(HaveKey("name"))
		g.Expect(formattedTaskMap).To(HaveKey("state"))
		g.Expect(formattedTaskMap).To(HaveKey("job_id"))
		g.Expect(formattedTaskMap).To(HaveKey("actor_id"))
		g.Expect(formattedTaskMap).To(HaveKey("type"))
		g.Expect(formattedTaskMap).To(HaveKey("func_or_class_name"))
		g.Expect(formattedTaskMap).To(HaveKey("parent_task_id"))
		g.Expect(formattedTaskMap).To(HaveKey("node_id"))
		g.Expect(formattedTaskMap).To(HaveKey("worker_id"))
		g.Expect(formattedTaskMap).To(HaveKey("worker_pid"))
		g.Expect(formattedTaskMap).To(HaveKey("error_type"))
		if detail {
			g.Expect(formattedTaskMap).To(HaveKey("language"))
			g.Expect(formattedTaskMap).To(HaveKey("required_resources"))
			g.Expect(formattedTaskMap).To(HaveKey("runtime_env_info"))
			g.Expect(formattedTaskMap).To(HaveKey("placement_group_id"))
			g.Expect(formattedTaskMap).To(HaveKey("events"))
			// g.Expect(formattedTaskMap).To(HaveKey("profiling_data"))
			g.Expect(formattedTaskMap).To(HaveKey("creation_time_ms"))
			g.Expect(formattedTaskMap).To(HaveKey("start_time_ms"))
			g.Expect(formattedTaskMap).To(HaveKey("end_time_ms"))
			g.Expect(formattedTaskMap).To(HaveKey("task_log_info"))
			g.Expect(formattedTaskMap).To(HaveKey("error_message"))
			g.Expect(formattedTaskMap).To(HaveKey("is_debugger_paused"))
			g.Expect(formattedTaskMap).To(HaveKey("call_site"))
			g.Expect(formattedTaskMap).To(HaveKey("label_selector"))
		}
	}
	LogWithTimestamp(test.T(), "Task response schema verified successfully")
}

// verifySingleEndpoint verifies the response schema of a single endpoint.
func verifySingleEndpoint(test Test, g *WithT, client *http.Client, endpointURL string, verifySchema func(test Test, g *WithT, data map[string]any)) {
	var respData map[string]any
	g.Eventually(func(gg Gomega) {
		resp, err := client.Get(endpointURL)
		gg.Expect(err).NotTo(HaveOccurred())
		defer resp.Body.Close()
		gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

		body, err := io.ReadAll(resp.Body)
		gg.Expect(err).NotTo(HaveOccurred())

		err = json.Unmarshal(body, &respData)
		gg.Expect(err).NotTo(HaveOccurred())
	}, TestTimeoutShort).Should(Succeed())

	verifySchema(test, g, respData)
}

// TODO(jiangjiawei1103): Make verification for node-related endpoints more robust.
// verifyNodesRespSchema verifies that the /nodes response is valid according to the API schema.
// Both live and dead clusters now return the same format (flat array of latest snapshots).
// isLive is kept for signature compatibility but no longer affects validation.
func verifyNodesRespSchema(test Test, g *WithT, nodesResp map[string]any, _ bool) {
	// Verify top-level fields.
	g.Expect(nodesResp).To(HaveKeyWithValue("result", BeTrue()))
	g.Expect(nodesResp).To(HaveKeyWithValue("msg", Equal("Node summary fetched.")))
	g.Expect(nodesResp).To(HaveKey("data"))

	data, ok := nodesResp["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data' should be a map")
	g.Expect(data).To(HaveKey("summary"))
	g.Expect(data).To(HaveKey("nodeLogicalResources"))

	// Verify summary field.
	LogWithTimestamp(test.T(), "Verifying summary field")
	summary, ok := data["summary"].([]any)
	g.Expect(ok).To(BeTrue(), "'summary' should be an array")

	// Both live and dead clusters now return a flat array of node summaries.
	// Dead cluster uses the latest snapshot per node to match the Ray Dashboard API format.
	g.Expect(summary).To(HaveLen(2), "Should have 2 node summaries (one head node and one worker node)")
	for _, nodeSummary := range summary {
		nodeSummarySnapshot, ok := nodeSummary.(map[string]any)
		g.Expect(ok).To(BeTrue(), "nodeSummary should be a map")
		verifyNodeSummarySchema(test, g, nodeSummarySnapshot)
	}

	// Verify nodeLogicalResources field.
	LogWithTimestamp(test.T(), "Verifying nodeLogicalResources field")
	nodeLogicalResources, ok := data["nodeLogicalResources"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'nodeLogicalResources' should be a map")

	// Both live and dead clusters return {nodeId: string} format.
	// Dead cluster uses the latest resource string per node.
	g.Expect(nodeLogicalResources).To(HaveLen(2), "Should have 2 resource strings (one head node and one worker node)")
	for nodeId, resourceString := range nodeLogicalResources {
		g.Expect(nodeId).NotTo(BeEmpty())
		g.Expect(resourceString).NotTo(BeEmpty())
	}

	LogWithTimestamp(test.T(), "/nodes response schema verification completed")
}

// verifyNodeRespSchema verifies that the /nodes/{node_id} response is valid according to the API schema.
// Both live and dead clusters now return the same format (single object with latest snapshot).
// isLive is kept for signature compatibility but no longer affects validation.
func verifyNodeRespSchema(test Test, g *WithT, nodeResp map[string]any, _ bool) {
	// Verify top-level fields.
	g.Expect(nodeResp).To(HaveKeyWithValue("result", BeTrue()))
	g.Expect(nodeResp).To(HaveKeyWithValue("msg", Equal("Node details fetched.")))
	g.Expect(nodeResp).To(HaveKey("data"))

	data, ok := nodeResp["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data' should be a map")
	g.Expect(data).To(HaveKey("detail"))

	// Both live and dead clusters return detail as a single node object (latest snapshot).
	nodeSummarySnapshot, ok := data["detail"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'detail' should be a map")
	verifyNodeSummarySchema(test, g, nodeSummarySnapshot)
}

// verifyNodeSummarySchema verifies that the node summary contains key fields.
func verifyNodeSummarySchema(_ Test, g *WithT, nodeSummary map[string]any) {
	for _, field := range []string{"now", "hostname", "ip", "raylet"} {
		g.Expect(nodeSummary).To(HaveKey(field))
	}

	// Verify raylet field.
	raylet, ok := nodeSummary["raylet"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'raylet' should be a map")
	g.Expect(raylet).To(HaveKey("storeStats"))
	g.Expect(raylet).To(HaveKey("nodeId"))
	g.Expect(raylet).To(HaveKey("nodeManagerAddress"))
	g.Expect(raylet).To(HaveKey("nodeManagerHostname"))
	g.Expect(raylet).To(HaveKey("rayletSocketName"))
	g.Expect(raylet).To(HaveKey("objectStoreSocketName"))
	g.Expect(raylet).To(HaveKey("metricsExportPort"))
	g.Expect(raylet).To(HaveKey("resourcesTotal"))
	g.Expect(raylet).To(HaveKey("nodeName"))
	g.Expect(raylet).To(HaveKey("instanceId"))
	g.Expect(raylet).To(HaveKey("nodeTypeName"))
	g.Expect(raylet).To(HaveKey("instanceTypeName"))
	g.Expect(raylet).To(HaveKey("startTimeMs"))
	g.Expect(raylet).To(HaveKey("isHeadNode"))
	g.Expect(raylet).To(HaveKey("labels"))
	g.Expect(raylet).To(HaveKey("state"))
	g.Expect(raylet).To(HaveKey("endTimeMs"))
	g.Expect(raylet).To(HaveKey("stateMessage"))
}

// verifyTaskSummarizeLineageRespSchema verifies that the /api/v0/tasks/summarize?summary_by=lineage
// response is valid according to the API schema.
func verifyTaskSummarizeLineageRespSchema(test Test, g *WithT, resp map[string]any) {
	// Verify top-level fields.
	g.Expect(resp).To(HaveKeyWithValue("result", BeTrue()))
	g.Expect(resp).To(HaveKey("data"))

	data, ok := resp["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data' should be a map")
	g.Expect(data).To(HaveKey("result"))

	dataResult, ok := data["result"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data.result' should be a map")

	g.Expect(dataResult).To(HaveKey("total"))
	g.Expect(dataResult).To(HaveKey("num_after_truncation"))
	g.Expect(dataResult).To(HaveKey("num_filtered"))
	g.Expect(dataResult).To(HaveKey("result"))
	g.Expect(dataResult).To(HaveKey("partial_failure_warning"))

	innerResult, ok := dataResult["result"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data.result.result' should be a map")
	g.Expect(innerResult).To(HaveKey("node_id_to_summary"))

	nodeIDToSummary, ok := innerResult["node_id_to_summary"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'node_id_to_summary' should be a map")

	// At least one entry in node_id_to_summary.
	g.Expect(nodeIDToSummary).ToNot(BeEmpty(), "should have at least one node_id_to_summary entry")

	// Verify each TaskSummaries entry.
	for key, summaryVal := range nodeIDToSummary {
		LogWithTimestamp(test.T(), "Verifying TaskSummaries for key: %s", key)

		taskSummaries, ok := summaryVal.(map[string]any)
		g.Expect(ok).To(BeTrue(), "'%s' should be a map", key)

		g.Expect(taskSummaries).To(HaveKey("summary"))
		g.Expect(taskSummaries).To(HaveKey("total_tasks"))
		g.Expect(taskSummaries).To(HaveKey("total_actor_tasks"))
		g.Expect(taskSummaries).To(HaveKey("total_actor_scheduled"))
		g.Expect(taskSummaries).To(HaveKey("summary_by"))
		g.Expect(taskSummaries["summary_by"]).To(Equal("lineage"))

		// Verify summary is an array with at least one entry.
		summary, ok := taskSummaries["summary"].([]any)
		g.Expect(ok).To(BeTrue(), "'summary' should be an array")
		g.Expect(summary).ToNot(BeEmpty(), "should have at least one summary entry")

		// Verify each NestedTaskSummary node (schema check).
		for _, entry := range summary {
			verifyNestedTaskSummarySchema(g, entry)
		}

		// Verify lineage tree structural properties produced by rayjob.yaml scenarios.
		// Walk the entire tree once to collect statistics, then assert.
		stats := &lineageStats{
			typeSeen:                  make(map[string]bool),
			groupLinkAlwaysNull:       true,
			nonGroupLinkAlwaysPresent: true,
		}
		for _, entry := range summary {
			collectLineageStats(entry, stats)
		}

		// Check 1: All 5 node types must appear.
		// rayjob.yaml produces NORMAL_TASK (my_task, parent_task, child_task, repeated_task),
		// ACTOR_CREATION_TASK (Counter.__init__), ACTOR_TASK (Counter.increment, Counter.get_count),
		// ACTOR (Counter group), and GROUP (repeated_task ×3, Counter.increment ×2).
		for _, expectedType := range []string{"NORMAL_TASK", "ACTOR_CREATION_TASK", "ACTOR_TASK", "ACTOR", "GROUP"} {
			g.Expect(stats.typeSeen).To(HaveKey(expectedType),
				"lineage tree should contain node type %s", expectedType)
		}

		// Check 2: At least one NORMAL_TASK has non-empty children (Scenario 2: parent_task → child_task).
		g.Expect(stats.hasNestedNormalTask).To(BeTrue(),
			"lineage tree should have at least one NORMAL_TASK with children (nested tasks)")

		// Check 3: At least one GROUP exists with >1 children (Scenario 3: repeated_task ×3).
		g.Expect(stats.hasGroupWithMultipleChildren).To(BeTrue(),
			"lineage tree should have at least one GROUP with multiple children (merged siblings)")

		// Check 4: At least one ACTOR contains an ACTOR_CREATION_TASK child (Scenario 4: Counter).
		g.Expect(stats.hasActorWithCreationTask).To(BeTrue(),
			"lineage tree should have at least one ACTOR containing an ACTOR_CREATION_TASK child")

		// Check 5: GROUP nodes have link=null (not a non-null object).
		g.Expect(stats.groupLinkAlwaysNull).To(BeTrue(),
			"GROUP nodes should have link=null, matching Ray Dashboard behavior")

		// Check 6: Non-GROUP nodes (task/actor) have non-null link with type and id.
		g.Expect(stats.nonGroupLinkAlwaysPresent).To(BeTrue(),
			"non-GROUP nodes should have a non-null link with type and id")
	}

	LogWithTimestamp(test.T(), "Task summarize lineage response schema verification completed")
}

// verifyNestedTaskSummarySchema recursively verifies the schema of a NestedTaskSummary node
// in the lineage tree, including all children.
func verifyNestedTaskSummarySchema(g *WithT, entry any) {
	node, ok := entry.(map[string]any)
	g.Expect(ok).To(BeTrue(), "NestedTaskSummary should be a map")

	g.Expect(node).To(HaveKey("name"))
	g.Expect(node).To(HaveKey("key"))
	g.Expect(node).To(HaveKey("type"))
	g.Expect(node).To(HaveKey("timestamp"))
	g.Expect(node).To(HaveKey("state_counts"))
	g.Expect(node).To(HaveKey("children"))

	// Verify state_counts is a map with at least one entry.
	stateCounts, ok := node["state_counts"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'state_counts' should be a map")
	g.Expect(stateCounts).ToNot(BeEmpty(), "should have at least one state count")

	// Verify type is one of the known node types.
	nodeType, ok := node["type"].(string)
	g.Expect(ok).To(BeTrue(), "'type' should be a string")
	g.Expect(nodeType).To(BeElementOf("NORMAL_TASK", "ACTOR_CREATION_TASK", "ACTOR_TASK", "ACTOR", "GROUP"))

	// Verify children is an array and recursively verify each child.
	children, ok := node["children"].([]any)
	g.Expect(ok).To(BeTrue(), "'children' should be an array")
	for _, child := range children {
		verifyNestedTaskSummarySchema(g, child)
	}

	// Verify link if present (optional field, absent on GROUP nodes).
	if link, exists := node["link"]; exists && link != nil {
		linkMap, ok := link.(map[string]any)
		g.Expect(ok).To(BeTrue(), "'link' should be a map")
		g.Expect(linkMap).To(HaveKey("type"))
		g.Expect(linkMap).To(HaveKey("id"))
		linkType, ok := linkMap["type"].(string)
		g.Expect(ok).To(BeTrue(), "'link.type' should be a string")
		g.Expect(linkType).To(BeElementOf("task", "actor"))
	}
}

// lineageStats holds statistics collected from a single walk of the lineage tree.
// Used to verify that rayjob.yaml scenarios produced the expected tree structure.
type lineageStats struct {
	typeSeen                     map[string]bool // all node types encountered
	hasNestedNormalTask          bool            // at least one NORMAL_TASK has non-empty children
	hasGroupWithMultipleChildren bool            // at least one GROUP has >1 children
	hasActorWithCreationTask     bool            // at least one ACTOR contains ACTOR_CREATION_TASK child
	groupLinkAlwaysNull          bool            // all GROUP nodes have link=null
	nonGroupLinkAlwaysPresent    bool            // all non-GROUP nodes have non-null link
}

// collectLineageStats recursively walks a NestedTaskSummary node and updates stats.
func collectLineageStats(entry any, stats *lineageStats) {
	node, ok := entry.(map[string]any)
	if !ok {
		return
	}

	nodeType, _ := node["type"].(string)
	stats.typeSeen[nodeType] = true

	children, _ := node["children"].([]any)

	// Check 2: NORMAL_TASK with non-empty children (nested tasks).
	if nodeType == "NORMAL_TASK" && len(children) > 0 {
		stats.hasNestedNormalTask = true
	}

	// Check 3: GROUP with >1 children (merged siblings).
	if nodeType == "GROUP" && len(children) > 1 {
		stats.hasGroupWithMultipleChildren = true
	}

	// Check 4: ACTOR contains at least one ACTOR_CREATION_TASK child.
	if nodeType == "ACTOR" {
		for _, child := range children {
			if childNode, ok := child.(map[string]any); ok {
				if childType, _ := childNode["type"].(string); childType == "ACTOR_CREATION_TASK" {
					stats.hasActorWithCreationTask = true
					break
				}
			}
		}
	}

	// Check 5: GROUP link should be null.
	if nodeType == "GROUP" {
		if link, exists := node["link"]; exists && link != nil {
			stats.groupLinkAlwaysNull = false
		}
	}

	// Check 6: Non-GROUP nodes should have non-null link.
	if nodeType != "GROUP" {
		link, exists := node["link"]
		if !exists || link == nil {
			stats.nonGroupLinkAlwaysPresent = false
		}
	}

	// Recurse into children.
	for _, child := range children {
		collectLineageStats(child, stats)
	}
}

// verifyTaskSummarizeFuncNameRespSchema verifies that the /api/v0/tasks/summarize (default func_name mode)
// response is valid according to the API schema.
//
// Expected response structure (matching Ray Dashboard's rest_response format):
//
//	{
//	  "result": true, "msg": "",
//	  "data": { "result": {
//	    "total": <int>, "num_after_truncation": <int>, "num_filtered": <int>,
//	    "result": { "node_id_to_summary": { "cluster": {
//	      "summary": { "<func_name>": { "func_or_class_name": ..., "type": ..., "state_counts": {...} } },
//	      "total_tasks": <int>, "total_actor_tasks": <int>, "total_actor_scheduled": <int>,
//	      "summary_by": "func_name"
//	    }}}
//	  }}
//	}
func verifyTaskSummarizeFuncNameRespSchema(test Test, g *WithT, resp map[string]any) {
	// Verify top-level fields.
	g.Expect(resp).To(HaveKeyWithValue("result", BeTrue()))
	g.Expect(resp).To(HaveKey("data"))

	data, ok := resp["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data' should be a map")
	g.Expect(data).To(HaveKey("result"))

	dataResult, ok := data["result"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data.result' should be a map")

	g.Expect(dataResult).To(HaveKey("total"))
	g.Expect(dataResult).To(HaveKey("num_after_truncation"))
	g.Expect(dataResult).To(HaveKey("num_filtered"))
	g.Expect(dataResult).To(HaveKey("result"))
	g.Expect(dataResult).To(HaveKey("partial_failure_warning"))

	innerResult, ok := dataResult["result"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data.result.result' should be a map")
	g.Expect(innerResult).To(HaveKey("node_id_to_summary"))

	nodeIDToSummary, ok := innerResult["node_id_to_summary"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'node_id_to_summary' should be a map")

	// At least one entry in node_id_to_summary.
	g.Expect(nodeIDToSummary).ToNot(BeEmpty(), "should have at least one node_id_to_summary entry")

	// Verify each TaskSummariesByFuncName entry.
	for key, summaryVal := range nodeIDToSummary {
		LogWithTimestamp(test.T(), "Verifying TaskSummariesByFuncName for key: %s", key)

		taskSummaries, ok := summaryVal.(map[string]any)
		g.Expect(ok).To(BeTrue(), "'%s' should be a map", key)

		g.Expect(taskSummaries).To(HaveKey("summary"))
		g.Expect(taskSummaries).To(HaveKey("total_tasks"))
		g.Expect(taskSummaries).To(HaveKey("total_actor_tasks"))
		g.Expect(taskSummaries).To(HaveKey("total_actor_scheduled"))
		g.Expect(taskSummaries).To(HaveKey("summary_by"))
		g.Expect(taskSummaries["summary_by"]).To(Equal("func_name"))

		// Verify summary is a map (func_name mode uses map, unlike lineage which uses array).
		summary, ok := taskSummaries["summary"].(map[string]any)
		g.Expect(ok).To(BeTrue(), "'summary' should be a map")
		g.Expect(summary).ToNot(BeEmpty(), "should have at least one summary entry")

		// Verify each TaskSummaryPerFuncOrClassName entry.
		for funcName, entryVal := range summary {
			LogWithTimestamp(test.T(), "Verifying TaskSummaryPerFuncOrClassName for func: %s", funcName)

			entry, ok := entryVal.(map[string]any)
			g.Expect(ok).To(BeTrue(), "'%s' should be a map", funcName)

			g.Expect(entry).To(HaveKey("func_or_class_name"))
			g.Expect(entry).To(HaveKey("type"))
			g.Expect(entry).To(HaveKey("state_counts"))

			// func_or_class_name should match the map key.
			g.Expect(entry["func_or_class_name"]).To(Equal(funcName))

			// type should be one of the known task types.
			entryType, ok := entry["type"].(string)
			g.Expect(ok).To(BeTrue(), "'type' should be a string")
			g.Expect(entryType).To(BeElementOf("NORMAL_TASK", "ACTOR_CREATION_TASK", "ACTOR_TASK"))

			// state_counts should be a map with at least one entry.
			stateCounts, ok := entry["state_counts"].(map[string]any)
			g.Expect(ok).To(BeTrue(), "'state_counts' should be a map")
			g.Expect(stateCounts).ToNot(BeEmpty(), "should have at least one state count")
		}
	}

	LogWithTimestamp(test.T(), "Task summarize func_name response schema verification completed")
}

// verifyNodesHostNameListSchema verifies that the /nodes?view=hostNameList response is valid according to the API schema.
func verifyNodesHostNameListSchema(_ Test, g *WithT, nodesResp map[string]any, _ bool) {
	g.Expect(nodesResp).To(HaveKeyWithValue("result", BeTrue()))
	g.Expect(nodesResp).To(HaveKeyWithValue("msg", Equal("Node hostname list fetched.")))
	g.Expect(nodesResp).To(HaveKey("data"))
	data, ok := nodesResp["data"].(map[string]any)
	g.Expect(ok).To(BeTrue(), "'data' should be a map")
	g.Expect(data).To(HaveKey("hostNameList"))
}

// testEventsEndpointLiveCluster verifies that the /events endpoint works for a live cluster.
// For live clusters, the request is proxied to Ray Dashboard.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Apply History Server and get its URL
// 4. Get the cluster info from the list and verify sessionName='live'
// 5. Set cluster context via /enter_cluster/ endpoint
// 6. Verify that the /events endpoint returns events with proper structure
// 7. Delete S3 bucket to ensure test isolation
func testEventsEndpointLiveCluster(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	test.T().Run("should return events", func(t *testing.T) {
		g := NewWithT(t)
		g.Eventually(func(gg Gomega) {
			resp, err := client.Get(historyServerURL + "/events")
			gg.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			gg.Expect(err).NotTo(HaveOccurred())

			var result map[string]any
			err = json.Unmarshal(body, &result)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(result["result"]).To(BeTrue())

			// Verify data.events exists
			data, ok := result["data"].(map[string]any)
			gg.Expect(ok).To(BeTrue(), "response should have 'data' field")
			_, ok = data["events"].(map[string]any)
			gg.Expect(ok).To(BeTrue(), "data should have 'events' field")
		}, TestTimeoutShort).Should(Succeed())
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster events endpoint test completed")
}

// testEventsEndpointDeadCluster verifies that the /events endpoint works for a dead cluster.
// Events are retrieved from the EventHandler's in-memory ClusterLogEventMap.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Delete RayCluster to trigger event upload to S3
// 4. Apply History Server and get its URL
// 5. Get the cluster info from the list and verify sessionName != 'live'
// 6. Set cluster context via /enter_cluster/ endpoint
// 7. Verify that the /events endpoint returns events from storage with proper structure
// 8. Verify that the /events endpoint supports job_id filter (non-existent job_id)
// 9. Verify that the /events endpoint handles empty job_id parameter correctly
// 10. Delete S3 bucket to ensure test isolation
func testEventsEndpointDeadCluster(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Delete RayCluster to trigger event upload
	err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Deleted RayCluster %s/%s", namespace.Name, rayCluster.Name)

	// Wait for cluster to be fully deleted
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	test.T().Run("should return events from storage", func(t *testing.T) {
		g := NewWithT(t)
		g.Eventually(func(gg Gomega) {
			resp, err := client.Get(historyServerURL + "/events")
			gg.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			gg.Expect(err).NotTo(HaveOccurred())

			var result map[string]any
			err = json.Unmarshal(body, &result)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(result["result"]).To(BeTrue())
			gg.Expect(result["msg"]).To(Equal("All events fetched."))

			// Verify data.events exists (may be empty if no events were collected)
			data, ok := result["data"].(map[string]any)
			gg.Expect(ok).To(BeTrue(), "response should have 'data' field")
			events, ok := data["events"].(map[string]any)
			gg.Expect(ok).To(BeTrue(), "data should have 'events' field")

			// If we have events, verify their structure matches Cluster Events format
			// Cluster Events are read from logs/{nodeId}/events/event_*.log files
			// Fields: eventId, sourceType, sourceHostname, sourcePid, severity, label, message, timestamp, customFields
			for jobID, jobEvents := range events {
				eventsList, ok := jobEvents.([]any)
				gg.Expect(ok).To(BeTrue(), "events for job %s should be an array", jobID)
				for _, event := range eventsList {
					eventMap, ok := event.(map[string]any)
					gg.Expect(ok).To(BeTrue(), "each event should be an object")
					// Verify required Cluster Event fields exist (camelCase format from ToAPIResponse)
					gg.Expect(eventMap).To(HaveKey("eventId"))
					gg.Expect(eventMap).To(HaveKey("sourceType"))
					gg.Expect(eventMap).To(HaveKey("severity"))
					gg.Expect(eventMap).To(HaveKey("timestamp"))
					gg.Expect(eventMap).To(HaveKey("message"))
				}
			}
		}, TestTimeoutMedium).Should(Succeed())
	})

	test.T().Run("should support job_id filter", func(t *testing.T) {
		g := NewWithT(t)
		g.Eventually(func(gg Gomega) {
			// Use a non-existent job_id to test the filter
			resp, err := client.Get(historyServerURL + "/events?job_id=nonexistent")
			gg.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			gg.Expect(err).NotTo(HaveOccurred())

			var result map[string]any
			err = json.Unmarshal(body, &result)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(result["result"]).To(BeTrue())
			gg.Expect(result["msg"]).To(Equal("Job events fetched."))

			data, ok := result["data"].(map[string]any)
			gg.Expect(ok).To(BeTrue())
			gg.Expect(data["jobId"]).To(Equal("nonexistent"))
			events, ok := data["events"].([]any)
			gg.Expect(ok).To(BeTrue())
			gg.Expect(events).To(BeEmpty()) // No events for non-existent job
		}, TestTimeoutShort).Should(Succeed())
	})

	test.T().Run("should return empty for empty job_id parameter", func(t *testing.T) {
		// This test verifies the behavior when job_id is provided but empty (/events?job_id=)
		// It should align with Ray Dashboard behavior: filter by empty string (return empty)
		g := NewWithT(t)
		g.Eventually(func(gg Gomega) {
			resp, err := client.Get(historyServerURL + "/events?job_id=")
			gg.Expect(err).NotTo(HaveOccurred())
			defer resp.Body.Close()
			gg.Expect(resp.StatusCode).To(Equal(http.StatusOK))

			body, err := io.ReadAll(resp.Body)
			gg.Expect(err).NotTo(HaveOccurred())

			var result map[string]any
			err = json.Unmarshal(body, &result)
			gg.Expect(err).NotTo(HaveOccurred())
			gg.Expect(result["result"]).To(BeTrue())
			gg.Expect(result["msg"]).To(Equal("Job events fetched."))

			data, ok := result["data"].(map[string]any)
			gg.Expect(ok).To(BeTrue())
			gg.Expect(data["jobId"]).To(Equal(""))
			events, ok := data["events"].([]any)
			gg.Expect(ok).To(BeTrue())
			gg.Expect(events).To(BeEmpty()) // Empty job_id filters by empty string
		}, TestTimeoutShort).Should(Succeed())
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster events endpoint tests completed")
}

// testLiveAndDeadClusterTimezone verifies the /timezone endpoint for both live and dead clusters,
// and checks that the dead cluster returns the same timezone data as the live cluster.
//
// The /timezone endpoint returns the head node's system timezone, so no RayJob submission is needed.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Apply History Server and get its URL
// 3. Get live cluster info and set cluster context
// 4. Verify /timezone returns valid JSON with non-empty 'offset' and 'value' fields
// 5. Record the live cluster's offset and value
// 6. Delete the RayCluster to trigger data upload to S3
// 7. Get the dead cluster info and switch cluster context
// 8. Verify /timezone returns valid JSON from storage with non-empty 'offset' and 'value'
// 9. Verify dead cluster offset and value match the live cluster values
// 10. Delete S3 bucket to ensure test isolation
func testLiveAndDeadClusterTimezone(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	// --- Live cluster ---
	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpointURL := fmt.Sprintf("%s%s", historyServerURL, EndpointTimezone)

	var liveOffset, liveValue string
	test.T().Run("live cluster should proxy /timezone and return timezone info", func(_ *testing.T) {
		verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
			// The Ray Dashboard /timezone endpoint returns {"offset": ..., "value": ...}
			g.Expect(data).To(HaveKey("offset"), "response should have 'offset' field")
			g.Expect(data).To(HaveKey("value"), "response should have 'value' field")
			g.Expect(data["offset"]).NotTo(BeEmpty(), "offset should not be empty")
			g.Expect(data["value"]).NotTo(BeEmpty(), "value should not be empty")
			liveOffset = data["offset"].(string)
			liveValue = data["value"].(string)
			LogWithTimestamp(test.T(), "Live cluster timezone: offset=%s, value=%s", liveOffset, liveValue)
		})
	})

	// --- Delete cluster ---
	DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)

	// --- Dead cluster ---
	deadClusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(deadClusterInfo.SessionName).NotTo(Equal(LiveSessionName))

	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, deadClusterInfo.SessionName)

	test.T().Run("dead cluster should return timezone from storage matching live cluster", func(_ *testing.T) {
		verifySingleEndpoint(test, g, client, endpointURL, func(test Test, g *WithT, data map[string]any) {
			// Dead cluster: timezone data is read from S3 storage
			g.Expect(data).To(HaveKey("offset"), "response should have 'offset' field")
			g.Expect(data).To(HaveKey("value"), "response should have 'value' field")
			g.Expect(data["offset"]).NotTo(BeEmpty(), "offset should not be empty")
			g.Expect(data["value"]).NotTo(BeEmpty(), "value should not be empty")
			// Verify consistency: dead cluster timezone should match the live cluster values
			g.Expect(data["offset"]).To(Equal(liveOffset), "dead cluster offset should match live cluster offset")
			g.Expect(data["value"]).To(Equal(liveValue), "dead cluster value should match live cluster value")
			LogWithTimestamp(test.T(), "Dead cluster timezone matches live: offset=%s, value=%s", data["offset"], data["value"])
		})
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Timezone endpoint tests completed successfully")
}

// testLiveClusterStatus verifies that the /api/cluster_status endpoint works for a live cluster.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Apply History Server and get its URL
// 4. Get the cluster info from the list and verify sessionName='live'
// 5. Set cluster context via /enter_cluster/ endpoint
// 6. Verify /api/cluster_status returns valid JSON response with result=true
// 7. Verify /api/cluster_status?format=1 returns formatted cluster status string containing "Autoscaler status"
// 8. Delete S3 bucket to ensure test isolation
func testLiveClusterStatus(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, "", EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpointURL := fmt.Sprintf("%s%s", historyServerURL, EndpointClusterStatus)

	test.T().Run("should proxy /api/cluster_status to live clusters", func(_ *testing.T) {
		verifySingleEndpoint(test, g, client, endpointURL, func(_ Test, g *WithT, data map[string]any) {
			g.Expect(data).To(HaveKeyWithValue("result", true))
			g.Expect(data).To(HaveKeyWithValue("msg", "Got cluster status."))
			g.Expect(data).To(HaveKey("data"))
		})
	})

	test.T().Run("should proxy for /api/cluster_status?format=1", func(_ *testing.T) {
		verifySingleEndpoint(test, g, client, endpointURL+"?format=1", func(_ Test, g *WithT, data map[string]any) {
			g.Expect(data).To(HaveKeyWithValue("result", true))
			g.Expect(data).To(HaveKeyWithValue("msg", "Got formatted cluster status."))
			g.Expect(data).To(HaveKey("data"))
			respData, ok := data["data"].(map[string]any)
			g.Expect(ok).To(BeTrue(), "'data' should be a map")
			clusterStatus, ok := respData["clusterStatus"].(string)
			g.Expect(ok).To(BeTrue(), "'clusterStatus' should be a string")
			g.Expect(clusterStatus).To(ContainSubstring("Autoscaler status"))
		})
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Live cluster status E2E test completed successfully")
}

// testDeadClusterStatus verifies that the /api/cluster_status endpoint works for a dead cluster.
// Cluster status is reconstructed from debug_state.txt and task/actor data stored in S3.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster
// 2. Submit a Ray job to the existing cluster
// 3. Delete RayCluster to trigger data upload to S3
// 4. Apply History Server and get its URL
// 5. Get the cluster info from the list and verify sessionName != 'live'
// 6. Set cluster context via /enter_cluster/ endpoint
// 7. Verify /api/cluster_status returns valid JSON response with result=true
// 8. Verify /api/cluster_status?format=1 returns formatted cluster status containing "Autoscaler status"
// 9. Delete S3 bucket to ensure test isolation
func testDeadClusterStatus(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	endpointURL := fmt.Sprintf("%s%s", historyServerURL, EndpointClusterStatus)

	test.T().Run("should return cluster status with /api/cluster_status", func(_ *testing.T) {
		verifySingleEndpoint(test, g, client, endpointURL, func(_ Test, g *WithT, data map[string]any) {
			g.Expect(data).To(HaveKeyWithValue("result", true))
			g.Expect(data).To(HaveKeyWithValue("msg", "Got cluster status."))
			g.Expect(data).To(HaveKey("data"))
		})
	})

	test.T().Run("should return formatted cluster status with /api/cluster_status?format=1", func(_ *testing.T) {
		verifySingleEndpoint(test, g, client, endpointURL+"?format=1", func(_ Test, g *WithT, data map[string]any) {
			g.Expect(data).To(HaveKeyWithValue("result", true))
			g.Expect(data).To(HaveKeyWithValue("msg", "Got formatted cluster status."))
			g.Expect(data).To(HaveKey("data"))
			respData, ok := data["data"].(map[string]any)
			g.Expect(ok).To(BeTrue(), "'data' should be a map")
			clusterStatus, ok := respData["clusterStatus"].(string)
			g.Expect(ok).To(BeTrue(), "'clusterStatus' should be a string")
			g.Expect(clusterStatus).To(ContainSubstring("Autoscaler status"))
		})
	})

	DeleteS3Bucket(test, g, s3Client)
	LogWithTimestamp(test.T(), "Dead cluster status E2E test completed successfully")
}
