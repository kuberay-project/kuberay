package e2e

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

// rayEvent contains specific fields in the Ray event JSON schema. For now, we keep only two fields,
// eventId and eventType while ensuring future extensibility (e.g., sessionName, timestamp, sourceType, etc.).
type rayEvent struct {
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
}

// decodeJSONLEvents decodes an event JSONL file the collector uploaded, one
// JSON object per line.
func decodeJSONLEvents(r io.Reader) ([]rayEvent, error) {
	var events []rayEvent
	dec := json.NewDecoder(r)
	for {
		var evt rayEvent
		if err := dec.Decode(&evt); err == io.EOF {
			return events, nil
		} else if err != nil {
			return nil, err
		}
		events = append(events, evt)
	}
}

func TestCollector(t *testing.T) {
	// Share a single S3 client among subtests.
	s3Client := EnsureS3Client(t)

	tests := []struct {
		name     string
		testFunc func(Test, *WithT, *corev1.Namespace, *S3TestClient)
	}{
		{
			name:     "Happy path: Logs and events should be uploaded to S3 on deletion",
			testFunc: testCollectorUploadOnGracefulShutdown,
		},
		{
			name:     "Simulate OOMKilled behavior: Single session single node logs and events should be uploaded to S3 after the ray-head container is restarted",
			testFunc: testCollectorSeparatesFilesBySession,
		},
		{
			name:     "Collector restart: should scan prev-logs and resume uploads left by a crash",
			testFunc: testCollectorResumesUploadsOnRestart,
		},
		{
			name:     "Cluster metadata: collector should fetch and store cluster_metadata endpoint data once on startup",
			testFunc: testCollectorStoresClusterMetadata,
		},
		{
			name:     "Placement groups: collector should poll and store placement_groups endpoint data",
			testFunc: testCollectorStoresPlacementGroups,
		},
		{
			name:     "Timezone: collector should fetch and store timezone endpoint data once on startup",
			testFunc: testCollectorStoresTimezone,
		},
		{
			name:     "Serve applications: collector should poll and store a converged Serve snapshot",
			testFunc: testCollectorStoresServeApplications,
		},
		{
			name:     "Ray Data datasets: collector should store per-job datasets only for the job that used Ray Data",
			testFunc: testCollectorStoresDataDatasets,
		},
		{
			name:     "Token auth: collector should authenticate against the Ray Dashboard instead of crash-looping",
			testFunc: testCollectorWithTokenAuth,
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

// testCollectorUploadOnGracefulShutdown verifies that logs, node_events, and job_events are successfully uploaded to S3 on cluster deletion.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing Ray cluster
// 3. Get the sessionID and nodeID for further verification
// 4. Delete the Ray cluster to trigger log uploading and event flushing on deletion. When the Ray cluster is deleted,
// logs, node_events, and job_events are processed as follows:
//   - logs: Trigger RayLogHandler.processSessionLatestLog to process logs under /tmp/ray/session_latest
//   - node_events: Trigger EventCollector.flushEvents, which calls ec.flushNodeEventsForHour to process in-memory node events
//   - job_events: Trigger EventCollector.flushEvents, which calls ec.flushJobEventsForHour to process in-memory job events
//
// 5. Verify logs, node_events, and job_events are successfully uploaded to S3. Expected S3 path structure:
//   - {S3BucketName}/log/cluster-history/raycluster/{clusterNamespace}/{clusterName}/{sessionID}/{nodeID}/logs/...
//   - {S3BucketName}/log/cluster-history/raycluster/{clusterNamespace}/{clusterName}/{sessionID}/{nodeID}/node_events/...
//   - {S3BucketName}/log/cluster-history/raycluster/{clusterNamespace}/{clusterName}/{sessionID}/{nodeID}/job_events/...
//
// For detailed verification logic, please refer to verifyS3SessionDirs.
//
// 6. Delete S3 bucket to ensure test isolation
func testCollectorUploadOnGracefulShutdown(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)

	// Submit a Ray job to the existing cluster.
	_ = ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	// Define variables for constructing S3 object prefix.
	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	headNodeID := GetNodeIDFromPod(test, g, HeadPod(test, rayCluster), "ray-head")
	workerNodeID := GetNodeIDFromPod(test, g, FirstWorkerPod(test, rayCluster), "ray-worker")
	sessionPrefix := fmt.Sprintf("%s/", clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID))

	// Delete the Ray cluster to trigger log uploading and event flushing on deletion.
	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	// Verify logs, node_events, and job_events are successfully uploaded to S3.
	verifyS3SessionDirs(test, g, s3Client, sessionPrefix, headNodeID, workerNodeID)

	// Delete S3 bucket to ensure test isolation.
	DeleteS3Bucket(test, g, s3Client)
}

// testCollectorSeparatesFilesBySession verifies that logs and node_events are successfully uploaded to S3 after the ray-head container is restarted.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Submit a Ray job to the existing cluster
// 3. Get the sessionID and nodeID for further verification
// 4. Kill the main process of ray-head container to trigger a container restart
// 5. Verify the old session logs have been processed on disk by checking existence of old session directories:
//   - /tmp/ray/prev-logs/{sessionID}
//   - /tmp/ray/persist-complete-logs/{sessionID}
//
// NOTE: Logs under /tmp/ray/session_latest are moved to /tmp/ray/prev-logs by the Ray container startup command.
//
// 6. Verify logs and node_events are successfully uploaded to S3. Expected S3 path structure:
//   - {S3BucketName}/log/{clusterName}_{clusterNamespace}/{sessionID}/logs/...
//   - {S3BucketName}/log/{clusterName}_{clusterNamespace}/{sessionID}/node_events/...
//
// 7. Delete S3 bucket to ensure test isolation
func testCollectorSeparatesFilesBySession(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)

	// Submit a Ray job to the existing cluster.
	_ = ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	headNodeID := GetNodeIDFromPod(test, g, HeadPod(test, rayCluster), "ray-head")
	workerNodeID := GetNodeIDFromPod(test, g, FirstWorkerPod(test, rayCluster), "ray-worker")
	sessionPrefix := fmt.Sprintf("%s/", clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID))

	// NOTE: We use `kill 1` to simulate Kubernetes OOMKilled behavior.
	// Before Kubernetes 1.28 (cgroups v1), if one process in a multi-process container exceeded its memory limit,
	// the Linux OOM killer might kill only that process, leaving the container running and making OOM events hard to detect.
	// Since Kubernetes 1.28 (with cgroups v2 enabled), `memory.oom.group` is enabled by default: when any process in a cgroup
	// hits the memory limit, all processes in the container are killed together, thereby triggering container restart.
	// For more details, please refer to https://github.com/kubernetes/kubernetes/pull/117793
	killContainerAndWaitForRestart(test, g, HeadPod(test, rayCluster), "ray-head")
	killContainerAndWaitForRestart(test, g, FirstWorkerPod(test, rayCluster), "ray-worker")

	verifySessionDirectoriesExist(test, g, rayCluster, sessionID)
	verifyS3SessionDirs(test, g, s3Client, sessionPrefix, headNodeID, workerNodeID)

	DeleteS3Bucket(test, g, s3Client)
}

// testCollectorResumesUploadsOnRestart verifies that the Collector scans and resumes uploads from
// the prev-logs directory on startup.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector and ensuring an empty S3 bucket.
// 2. Inject leftover logs before killing the collector:
//   - file1.log -> /tmp/ray/persist-complete-logs/{sessionID}/{nodeID}/logs/ (already uploaded)
//   - file2.log -> /tmp/ray/prev-logs/{sessionID}/{nodeID}/logs/ (pending upload)
//     Note: node_events are not injected or verified here; they are handled by the event collector,
//     and prev-logs processing only covers the logs directory.
//
// 3. Kill the collector sidecar container to trigger a container restart.
// 4. Wait for the collector container to restart and become Ready.
// 5. Verify S3 uploads: recovered log objects exist under log/{clusterName}_{clusterNamespace}/{sessionID}/logs/ and have content.
// 6. Verify local state: the node directory is present under persist-complete-logs and removed from prev-logs.
// 7. Clean up the S3 bucket to ensure test isolation.
func testCollectorResumesUploadsOnRestart(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)

	// Directory variables for easier maintenance
	prevLogsBaseDir := utils.GetRayPrevLogsPath()
	persistCompleteBaseDir := utils.GetRayPersistCompletePath()

	// Use namespace name to ensure test isolation (avoid conflicts from previous test runs)
	dummySessionID := fmt.Sprintf("test-recovery-session-%s", namespace.Name)
	dummyNodeID := fmt.Sprintf("head-node-%s", namespace.Name)
	sessionPrefix := fmt.Sprintf("%s/", clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, dummySessionID))

	// Inject "leftover" logs BEFORE killing collector.
	// This ensures files exist when collector restarts and performs its initial scan.
	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Injecting logs into %s before killing collector", prevLogsBaseDir)
	sessionDir := filepath.Join(prevLogsBaseDir, dummySessionID, dummyNodeID)
	persistDir := filepath.Join(persistCompleteBaseDir, dummySessionID, dummyNodeID)
	injectCmd := fmt.Sprintf(
		"mkdir -p %s/logs && "+
			"echo 'file1 content' > %s/logs/file1.log && "+
			"mkdir -p %s/logs && "+
			"echo 'file2 content' > %s/logs/file2.log",
		persistDir,
		persistDir,
		sessionDir,
		sessionDir,
	)
	_, stderr := ExecPodCmd(test, headPod, "ray-head", []string{"sh", "-c", injectCmd})
	g.Expect(stderr.String()).To(BeEmpty())

	// Kill the collector container to trigger a restart.
	// When collector restarts, WatchPrevLogsLoops() will scan prev-logs and find the injected files.
	LogWithTimestamp(test.T(), "Killing collector container to test startup scanning of prev-logs")
	_, stderrKill := ExecPodCmd(test, headPod, "collector", []string{"kill", "1"})
	g.Expect(stderrKill.String()).To(BeEmpty())

	// Wait for collector container to restart and become ready.
	LogWithTimestamp(test.T(), "Waiting for collector container to restart and become ready")
	g.Eventually(func(gg Gomega) {
		updatedPod, err := GetHeadPod(test, rayCluster)
		gg.Expect(err).NotTo(HaveOccurred())
		cs, err := GetContainerStatusByName(updatedPod, "collector")
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cs.RestartCount).To(BeNumerically(">", 0))
		gg.Expect(cs.Ready).To(BeTrue())
	}, TestTimeoutMedium).Should(Succeed())

	// Verify that file2.log was actually uploaded to S3.
	// file1.log should NOT be uploaded because it was already marked as "completed" in persist-complete-logs.
	// file2.log should be uploaded because it was in prev-logs (pending upload).
	LogWithTimestamp(test.T(), "Verifying file2.log was uploaded to S3 (idempotency check)")
	g.Eventually(func(gg Gomega) {
		// List all objects under the session logs prefix
		uploadedKeys, err := s3Client.ListObjectKeys(S3BucketName, sessionPrefix)
		gg.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Found uploaded objects: %v", uploadedKeys)

		// Verify file2.log exists in S3 (it was in prev-logs, so it should be uploaded)
		hasFile2 := false
		for _, key := range uploadedKeys {
			if strings.HasSuffix(key, "file2.log") {
				hasFile2 = true
				break
			}
		}
		gg.Expect(hasFile2).To(BeTrue(), "file2.log should be uploaded to S3 because it was in prev-logs")

		// Note: file1.log was only placed in persist-complete-logs (local marker),
		// it was never actually uploaded to S3 in this test scenario.
		// The persist-complete-logs directory is just a local marker to prevent re-upload.
	}, TestTimeoutMedium).Should(Succeed())

	// Verify local state: the node directory should be moved from prev-logs to persist-complete-logs.
	LogWithTimestamp(test.T(), "Verifying local state: node directory should be moved to %s", persistCompleteBaseDir)
	g.Eventually(func(gg Gomega) {
		currentHeadPod, err := GetHeadPod(test, rayCluster)
		gg.Expect(err).NotTo(HaveOccurred())
		// Check that the node directory exists in persist-complete-logs
		persistPath := filepath.Join(persistCompleteBaseDir, dummySessionID, dummyNodeID)
		checkCmd := fmt.Sprintf("test -d %s && echo 'exists'", persistPath)
		stdout, stderrCheck := ExecPodCmd(test, currentHeadPod, "ray-head", []string{"sh", "-c", checkCmd})
		gg.Expect(stderrCheck.String()).To(BeEmpty())
		gg.Expect(strings.TrimSpace(stdout.String())).To(Equal("exists"), "Node directory should be in persist-complete-logs")

		// Check that the node directory is gone from prev-logs
		prevPath := filepath.Join(prevLogsBaseDir, dummySessionID, dummyNodeID)
		checkGoneCmd := fmt.Sprintf("test ! -d %s && echo 'gone'", prevPath)
		stdoutGone, stderrGone := ExecPodCmd(test, currentHeadPod, "ray-head", []string{"sh", "-c", checkGoneCmd})
		gg.Expect(stderrGone.String()).To(BeEmpty())
		gg.Expect(strings.TrimSpace(stdoutGone.String())).To(Equal("gone"), "Node directory should be cleaned from prev-logs")
	}, TestTimeoutMedium).Should(Succeed())

	DeleteS3Bucket(test, g, s3Client)
}

func verifySessionDirectoriesExist(test Test, g *WithT, rayCluster *rayv1.RayCluster, sessionID string) {
	for _, dir := range []string{"prev-logs", "persist-complete-logs"} {
		dirPath := filepath.Join(utils.GetTmpRayRoot(), dir, sessionID)
		LogWithTimestamp(test.T(), "Checking if session directory %s exists in %s", sessionID, dirPath)
		g.Eventually(func(gg Gomega) {
			headPod, err := GetHeadPod(test, rayCluster)
			gg.Expect(err).NotTo(HaveOccurred())
			stdout, stderr := ExecPodCmd(test, headPod, "ray-head", []string{"sh", "-c", fmt.Sprintf("test -d %s && echo 'exists' || echo 'not found'", dirPath)})
			gg.Expect(stderr.String()).To(BeEmpty())
			gg.Expect(strings.TrimSpace(stdout.String())).To(Equal("exists"), "Session directory %s should exist in %s", sessionID, dirPath)
		}, TestTimeoutMedium).Should(Succeed(), "Session directory %s should exist in %s", sessionID, dirPath)
	}
}

// testCollectorStoresClusterMetadata verifies that the Head collector fetches /api/v0/cluster_metadata
// from the Ray Dashboard once on startup and stores the result in S3 under the session path.
//
// The metadata is stored per session because different sessions can use different Ray images,
// resulting in different rayVersion / pythonVersion values.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Get the sessionID from the head pod to build the expected S3 key
// 3. Wait for the cluster metadata file to appear in S3 at {sessionName}/fetched_endpoints/restful__api__v0__cluster_metadata
// 4. Read the file and verify it contains valid JSON with the expected schema
// 5. Delete S3 bucket to ensure test isolation
func testCollectorStoresClusterMetadata(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)

	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	storageKey := utils.EndpointPathToStorageKey("/api/v0/cluster_metadata")
	sessionDir := clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID)
	metaKey := fmt.Sprintf("%s/%s/%s", sessionDir, utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME, storageKey)

	LogWithTimestamp(test.T(), "Waiting for cluster metadata to appear at S3 key: %s", metaKey)

	var metadataBody []byte
	g.Eventually(func(gg Gomega) {
		body, err := s3Client.ReadObject(S3BucketName, metaKey)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(body).NotTo(BeEmpty(), "Cluster metadata file should not be empty")

		// Verify it is valid JSON
		var metadata map[string]any
		err = json.Unmarshal(body, &metadata)
		gg.Expect(err).NotTo(HaveOccurred(), "Cluster metadata should be valid JSON")

		// The Ray dashboard returns {"result": true, "data": {"rayVersion": ..., "pythonVersion": ...}}.
		// Verify the "data" sub-object contains expected fields.
		gg.Expect(metadata).To(HaveKey("data"), "Cluster metadata should contain data field")
		data, ok := metadata["data"].(map[string]any)
		gg.Expect(ok).To(BeTrue(), "data field should be a JSON object")
		gg.Expect(data).To(HaveKey("rayVersion"), "Cluster metadata should contain rayVersion")
		gg.Expect(data).To(HaveKey("pythonVersion"), "Cluster metadata should contain pythonVersion")

		metadataBody = body
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Cluster metadata stored successfully: %s", string(metadataBody))

	DeleteS3Bucket(test, g, s3Client)
}

// testCollectorStoresTimezone verifies that the Head collector fetches /timezone
// from the Ray Dashboard once on startup and stores the result in S3 under the session path.
//
// The timezone data is stored per session under fetched_endpoints/ in storage.
//
// The test case follows these steps:
// 1. Prepare test environment by applying a Ray cluster with the collector
// 2. Assert the timezone data reaches S3 with the expected key and schema
// 3. Delete S3 bucket to ensure test isolation
func testCollectorStoresTimezone(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)

	assertTimezoneStored(test, g, rayCluster, s3Client)

	DeleteS3Bucket(test, g, s3Client)
}

func assertTimezoneStored(test Test, g *WithT, rayCluster *rayv1.RayCluster, s3Client *S3TestClient) {
	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	storageKey := utils.EndpointPathToStorageKey(EndpointTimezone)
	sessionDir := clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID)
	timezoneKey := fmt.Sprintf("%s/%s/%s", sessionDir, utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME, storageKey)

	LogWithTimestamp(test.T(), "Waiting for timezone data to appear at S3 key: %s", timezoneKey)

	var timezoneBody []byte
	g.Eventually(func(gg Gomega) {
		body, err := s3Client.ReadObject(S3BucketName, timezoneKey)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(body).NotTo(BeEmpty(), "Timezone file should not be empty")

		// Verify it is valid JSON
		var timezone map[string]any
		err = json.Unmarshal(body, &timezone)
		gg.Expect(err).NotTo(HaveOccurred(), "Timezone data should be valid JSON")

		// The Ray dashboard /timezone endpoint returns {"offset": ..., "value": ...}.
		gg.Expect(timezone).To(HaveKey("offset"), "Timezone data should contain offset field")
		gg.Expect(timezone).To(HaveKey("value"), "Timezone data should contain value field")
		gg.Expect(timezone["offset"]).NotTo(BeEmpty(), "Timezone offset should not be empty")
		gg.Expect(timezone["value"]).NotTo(BeEmpty(), "Timezone value should not be empty")

		timezoneBody = body
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Timezone data stored successfully: %s", string(timezoneBody))
}

// testCollectorStoresPlacementGroups verifies the collector stores the placement_groups
// snapshot; the RayJob creates a detached placement group so the PG outlives the job.
// The history-server replay of this endpoint is covered by testDeadClusterPlacementGroups.
func testCollectorStoresPlacementGroups(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := PrepareTestEnv(test, g, namespace, s3Client)

	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	// Matches placementGroupsEndpoint in the collector's poll.go, query params included.
	storageKey := utils.EndpointPathToStorageKey("/api/v0/placement_groups?detail=1&limit=10000")
	sessionDir := clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID)
	pgKey := fmt.Sprintf("%s/%s/%s", sessionDir, utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME, storageKey)

	LogWithTimestamp(test.T(), "Waiting for placement groups data to appear at S3 key: %s", pgKey)
	g.Eventually(func(gg Gomega) {
		assertPlacementGroupsNonEmpty(gg, readS3Object(gg, s3Client, pgKey))
	}, TestTimeoutMedium).Should(Succeed())

	DeleteS3Bucket(test, g, s3Client)
}

// assertPlacementGroupsNonEmpty requires at least one PG in the State API envelope
// {"result": true, "data": {"result": {"result": [...]}}}; the RayJob creates a detached one.
func assertPlacementGroupsNonEmpty(g Gomega, body []byte) {
	var response struct {
		Result bool `json:"result"`
		Data   struct {
			Result struct {
				Result []json.RawMessage `json:"result"`
			} `json:"result"`
		} `json:"data"`
	}
	g.Expect(json.Unmarshal(body, &response)).To(Succeed(), "placement groups response should be valid JSON")
	g.Expect(response.Result).To(BeTrue(), "state API result field should be true")
	g.Expect(response.Data.Result.Result).NotTo(BeEmpty(), "placement groups list should not be empty")
}

// verifyS3SessionDirs verifies file contents in logs/, node_events/, and job_events/ directories under a session prefix in S3.
// There are two phases of verification:
// 1. Verify file contents in logs/ directory
//   - For the head node, verify raylet.out, gcs_server.out, and monitor.out exist
//   - For the worker node, verify raylet.out exists
//
// 2. Verify event type coverage in node_events/ and job_events/ directories
//   - Aggregate all events from node_events/ and job_events/ directories
//   - Verify that all potential event types are present in the aggregated events
//
// NOTE: Since flushed node and job events are nondeterministic, we need to aggregate them first before verifying event type coverage.
func verifyS3SessionDirs(test Test, g *WithT, s3Client *S3TestClient, sessionPrefix string, headNodeID string, workerNodeID string) {
	// Verify file contents in logs/ directory.
	headLogDirPrefix := fmt.Sprintf("%s%s/logs", sessionPrefix, headNodeID)
	workerLogDirPrefix := fmt.Sprintf("%s%s/logs", sessionPrefix, workerNodeID)

	LogWithTimestamp(test.T(), "Verifying raylet.out, gcs_server.out, and monitor.out exist in head log directory %s", headLogDirPrefix)
	for _, fileName := range []string{"raylet.out", "gcs_server.out", "monitor.out"} {
		assertFileExist(test, g, s3Client, headLogDirPrefix, fileName)
	}

	LogWithTimestamp(test.T(), "Verifying raylet.out exists in worker log directory %s", workerLogDirPrefix)
	assertFileExist(test, g, s3Client, workerLogDirPrefix, "raylet.out")

	// Verify event type coverage in node_events/ and job_events/ directories.
	LogWithTimestamp(test.T(), "Verifying all %d event types are covered, except for EVENT_TYPE_UNSPECIFIED: %v", len(types.AllEventTypes)-1, types.AllEventTypes)
	g.Eventually(func(gg Gomega) {
		events, err := loadRayEventsFromS3(s3Client, S3BucketName, sessionPrefix)
		gg.Expect(err).NotTo(HaveOccurred())

		assertAllEventTypesCovered(test, gg, events)
	}, TestTimeoutMedium).Should(Succeed())
}

// killContainerAndWaitForRestart kills the main process of a container and waits for the container to restart and become ready.
func killContainerAndWaitForRestart(test Test, g *WithT, getPod func() (*corev1.Pod, error), containerName string) {
	LogWithTimestamp(test.T(), "Killing main process of %s container to trigger a restart", containerName)
	g.Eventually(func(gg Gomega) {
		pod, err := getPod()
		gg.Expect(err).NotTo(HaveOccurred())
		_, stderr := ExecPodCmd(test, pod, containerName, []string{"kill", "1"})
		gg.Expect(stderr.String()).To(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed(), "Failed to kill main process of %s container", containerName)

	LogWithTimestamp(test.T(), "Waiting for %s container to restart and become ready", containerName)
	g.Eventually(func(gg Gomega) {
		pod, err := getPod()
		gg.Expect(err).NotTo(HaveOccurred())
		containerStatus, err := GetContainerStatusByName(pod, containerName)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(containerStatus.RestartCount).To(BeNumerically(">", 0))
		gg.Expect(containerStatus.Ready).To(BeTrue())
	}, TestTimeoutShort).Should(Succeed(), "%s container should restart and become ready", containerName)
}

// loadRayEventsFromS3 loads Ray events from S3.
func loadRayEventsFromS3(s3Client *S3TestClient, bucket string, prefix string) ([]rayEvent, error) {
	var events []rayEvent

	// List all file objects in the directory.
	keys, err := s3Client.ListObjectKeys(bucket, prefix)
	if err != nil {
		return nil, err
	}

	for _, fileKey := range keys {
		if !strings.Contains(fileKey, "/node_events/") && !strings.Contains(fileKey, "/job_events/") {
			continue
		}

		// Get the file object content and decode it into Ray events.
		content, err := s3Client.ReadObject(bucket, fileKey)
		if err != nil {
			return nil, err
		}

		var reader io.Reader = bytes.NewReader(content)
		if strings.HasSuffix(fileKey, ".gz") {
			gzReader, err := gzip.NewReader(reader)
			if err != nil {
				return nil, fmt.Errorf("failed to create gzip reader for %s: %w", fileKey, err)
			}
			reader = gzReader
		}

		fileEvents, err := decodeJSONLEvents(reader)
		if err != nil {
			return nil, fmt.Errorf("failed to decode Ray events from %s: %w", fileKey, err)
		}

		events = append(events, fileEvents...)
	}

	return events, nil
}

// assertFileExist verifies that a file object exists under the given log directory prefix.
// For a Ray cluster with one head node and one worker node, there are two log directories to verify:
//   - logs/<headNodeID>/
//   - logs/<workerNodeID>/
func assertFileExist(test Test, g *WithT, s3Client *S3TestClient, nodeLogDirPrefix string, fileName string) {
	fileKey := fmt.Sprintf("%s/%s", nodeLogDirPrefix, fileName)
	LogWithTimestamp(test.T(), "Verifying file %s exists", fileKey)
	g.Eventually(func(gg Gomega) {
		gg.Expect(s3Client.StatObject(S3BucketName, fileKey)).To(Succeed())
		LogWithTimestamp(test.T(), "Verified file %s exists", fileKey)
	}, TestTimeoutMedium).Should(Succeed(), "Failed to verify file %s exists", fileKey)
}

// assertAllEventTypesCovered verifies that all potential event types are present in the events uploaded to S3.
// NOTE: EVENT_TYPE_UNSPECIFIED is excluded from verification.
//
// This function accepts Gomega (not *WithT) because it's called from within g.Eventually() callbacks,
// which provide a nested Gomega instance for assertions.
func assertAllEventTypesCovered(test Test, g Gomega, events []rayEvent) {
	foundEventTypes := map[string]bool{}
	for _, event := range events {
		foundEventTypes[event.EventType] = true
	}

	for _, eventType := range types.AllEventTypes {
		if eventType == types.EVENT_TYPE_UNSPECIFIED {
			LogWithTimestamp(test.T(), "Skipping verification for EVENT_TYPE_UNSPECIFIED")
			continue
		}
		g.Expect(foundEventTypes[string(eventType)]).To(BeTrue(), "Event type %s not found", eventType)
	}
}

// readS3Object reads an object body, failing the enclosing Eventually if it is absent.
func readS3Object(g Gomega, s3Client *S3TestClient, key string) []byte {
	body, err := s3Client.ReadObject(S3BucketName, key)
	g.Expect(err).NotTo(HaveOccurred())
	return body
}

// listFetchedEndpoints lists polled-endpoint objects by storage-key prefix. Listing beats
// constructing the key: the session name would have to come from an already-deleted head pod.
func listFetchedEndpoints(g Gomega, s3Client *S3TestClient, clusterPrefix, storageKeyPrefix string) []string {
	marker := "/" + utils.RAY_SESSIONDIR_FETCHED_ENDPOINTS_NAME + "/"

	allKeys, err := s3Client.ListObjectKeys(S3BucketName, clusterPrefix)
	g.Expect(err).NotTo(HaveOccurred())

	var keys []string
	for _, key := range allKeys {
		idx := strings.Index(key, marker)
		if idx >= 0 && strings.HasPrefix(key[idx+len(marker):], storageKeyPrefix) {
			keys = append(keys, key)
		}
	}
	return keys
}

// enterClusterForOwner sets the session cookie via enter_cluster, which takes the owner's
// name and resolves the generated cluster name itself; a plain RayCluster is its own owner.
func enterClusterForOwner(test Test, g *WithT, client *http.Client, historyServerURL, namespace, ownerKind, ownerName, clusterName, session string) {
	enterURL := fmt.Sprintf("%s/enter_cluster/%s/%s/%s/%s", historyServerURL, namespace, ownerKind, ownerName, session)
	LogWithTimestamp(test.T(), "Setting cluster context: %s", enterURL)

	g.Eventually(func(gg Gomega) {
		var result map[string]any
		gg.Expect(json.Unmarshal(getHistoryServerJSON(gg, client, enterURL), &result)).To(Succeed())
		gg.Expect(result["result"]).To(Equal("success"))
		gg.Expect(result["name"]).To(Equal(clusterName), "enter_cluster should resolve the owner to its generated cluster")
		gg.Expect(result["session"]).To(Equal(session))
	}, TestTimeoutShort).Should(Succeed())
}

// getHistoryServerJSON GETs a history server URL and returns the body, requiring 200.
func getHistoryServerJSON(g Gomega, client *http.Client, url string) []byte {
	resp, err := client.Get(url)
	g.Expect(err).NotTo(HaveOccurred())
	defer resp.Body.Close()
	g.Expect(resp.StatusCode).To(Equal(http.StatusOK))

	body, err := io.ReadAll(resp.Body)
	g.Expect(err).NotTo(HaveOccurred())
	return body
}

// testCollectorStoresServeApplications verifies the collector stores a converged Serve
// snapshot, then replays it through the history server after the RayService is deleted.
// The round trip matters: collector and server derive the storage key from different
// inputs, and a mismatch surfaces as a valid-but-empty 200, not an error.
func testCollectorStoresServeApplications(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayService := ApplyRayServiceAndWaitForRunning(test, g, namespace)
	clusterName := rayService.Status.ActiveServiceStatus.RayClusterName
	clusterPrefix := clusterlogs.Prefix("", utils.RayServiceKind, rayService.Name, namespace.Name, clusterName)

	// Matches serveApplicationsEndpoint in the collector's poll.go.
	storageKey := utils.EndpointPathToStorageKey("/api/serve/applications/")
	LogWithTimestamp(test.T(), "Waiting for a converged Serve snapshot under S3 prefix: %s", clusterPrefix)

	g.Eventually(func(gg Gomega) {
		keys := listFetchedEndpoints(gg, s3Client, clusterPrefix, storageKey)
		gg.Expect(keys).To(HaveLen(1), "the head collector stores exactly one Serve snapshot per session")
		assertServeAppConverged(gg, readS3Object(gg, s3Client, keys[0]))
	}, TestTimeoutMedium).Should(Succeed())

	DeleteRayServiceAndWait(test, g, namespace.Name, rayService.Name, clusterName)

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)
	clusterInfo := getClusterFromList(test, g, historyServerURL, clusterName, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName), "Cluster should be a dead session after deletion")

	client := CreateHTTPClientWithCookieJar(test, g)
	enterClusterForOwner(test, g, client, historyServerURL, namespace.Name,
		utils.RayServiceKind, rayService.Name, clusterName, clusterInfo.SessionName)

	LogWithTimestamp(test.T(), "Replaying /api/serve/applications/ through the history server")
	g.Eventually(func(gg Gomega) {
		assertServeAppConverged(gg, getHistoryServerJSON(gg, client, historyServerURL+"/api/serve/applications/"))
	}, TestTimeoutShort).Should(Succeed())

	DeleteS3Bucket(test, g, s3Client)
}

// assertServeAppConverged requires RUNNING/HEALTHY: a mid-deploy snapshot and the empty
// fallback are both valid JSON.
func assertServeAppConverged(g Gomega, body []byte) {
	var response struct {
		Applications map[string]struct {
			Status      string `json:"status"`
			Deployments map[string]struct {
				Status string `json:"status"`
			} `json:"deployments"`
		} `json:"applications"`
	}
	g.Expect(json.Unmarshal(body, &response)).To(Succeed(), "Serve response should be valid JSON")

	app, ok := response.Applications["historyserver-demo"]
	g.Expect(ok).To(BeTrue(), "applications should contain historyserver-demo, got %v", response.Applications)
	g.Expect(app.Status).To(Equal("RUNNING"), "historyserver-demo should have converged")

	deployment, ok := app.Deployments["NoOp"]
	g.Expect(ok).To(BeTrue(), "historyserver-demo should contain the NoOp deployment")
	g.Expect(deployment.Status).To(Equal("HEALTHY"), "NoOp replica should be healthy")
}

// testCollectorStoresDataDatasets verifies per-job Ray Data snapshots: exactly one object
// (jobs without datasets are never stored), surviving a self-shutdown cluster, and served
// back by the history server for the URI the frontend requests.
func testCollectorStoresDataDatasets(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayJob := ApplyRayDataJobAndWaitForCompletion(test, g, namespace)
	clusterName := rayJob.Status.RayClusterName
	clusterPrefix := clusterlogs.Prefix("", utils.RayJobKind, rayJob.Name, namespace.Name, clusterName)

	// "/api/data/datasets/" maps to "restful__api__data__datasets"; per-job keys append
	// "__{job_id}", so this prefix matches every stored job.
	storageKeyPrefix := utils.EndpointPathToStorageKey("/api/data/datasets/")
	LogWithTimestamp(test.T(), "Waiting for a non-empty datasets object under S3 prefix: %s", clusterPrefix)

	var jobID string
	g.Eventually(func(gg Gomega) {
		keys := listFetchedEndpoints(gg, s3Client, clusterPrefix, storageKeyPrefix)
		gg.Expect(keys).To(HaveLen(1),
			"exactly one job used Ray Data, so exactly one datasets object should exist")
		assertDatasetsNonEmpty(gg, readS3Object(gg, s3Client, keys[0]))

		// Ray assigns the job ID, so recover it from the key the collector chose.
		jobID = strings.TrimPrefix(path.Base(keys[0]), storageKeyPrefix+"__")
		gg.Expect(jobID).NotTo(BeEmpty())
	}, TestTimeoutMedium).Should(Succeed())

	// shutdownAfterJobFinishes tears the cluster down on its own, which is what turns
	// the session into a replayable one.
	g.Eventually(func() error {
		_, err := GetRayCluster(test, namespace.Name, clusterName)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	ApplyHistoryServer(test, g, namespace, "")
	historyServerURL := GetHistoryServerURL(test, g, namespace)
	clusterInfo := getClusterFromList(test, g, historyServerURL, clusterName, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName), "Cluster should be a dead session after shutdown")

	client := CreateHTTPClientWithCookieJar(test, g)
	enterClusterForOwner(test, g, client, historyServerURL, namespace.Name,
		utils.RayJobKind, rayJob.Name, clusterName, clusterInfo.SessionName)

	datasetsURL := fmt.Sprintf("%s/api/data/datasets/%s", historyServerURL, jobID)
	LogWithTimestamp(test.T(), "Replaying %s through the history server", datasetsURL)
	g.Eventually(func(gg Gomega) {
		assertDatasetsNonEmpty(gg, getHistoryServerJSON(gg, client, datasetsURL))
	}, TestTimeoutShort).Should(Succeed())

	DeleteS3Bucket(test, g, s3Client)
}

// assertDatasetsNonEmpty requires real stats: empty coming back means the object was missing.
func assertDatasetsNonEmpty(g Gomega, body []byte) {
	var response struct {
		Datasets []json.RawMessage `json:"datasets"`
	}
	g.Expect(json.Unmarshal(body, &response)).To(Succeed(), "datasets response should be valid JSON")
	g.Expect(response.Datasets).NotTo(BeEmpty(), "datasets must be non-empty")
}

func testCollectorWithTokenAuth(test Test, g *WithT, namespace *corev1.Namespace, s3Client *S3TestClient) {
	rayCluster := ApplyRayClusterWithCollectorTokenAuth(test, g, namespace)

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())

	// Guard against a vacuous pass: if the Dashboard served this cluster without credentials the
	// rest of the assertions would hold even with the fix reverted. Probed with Python because
	// the Ray images ship no curl.
	probe := `python3 - <<'PY'
import urllib.error, urllib.request
try:
    urllib.request.urlopen("http://localhost:8265/api/v0/nodes?limit=1", timeout=10)
    print("200")
except urllib.error.HTTPError as err:
    print(err.code)
PY`
	stdout, stderr := ExecPodCmd(test, headPod, "ray-head", []string{"sh", "-c", probe})
	g.Expect(stderr.String()).To(BeEmpty())
	g.Expect(strings.TrimSpace(stdout.String())).To(Equal("401"))

	// Data in storage is the real proof: the collector only writes the timezone object after an
	// authenticated Dashboard call succeeds.
	assertTimezoneStored(test, g, rayCluster, s3Client)

	DeleteS3Bucket(test, g, s3Client)
}
