package e2e

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestAzureBlobCollector(t *testing.T) {
	azureClient := EnsureAzureBlobClient(t)

	tests := []struct {
		name     string
		testFunc func(Test, *WithT, *corev1.Namespace, *azblob.Client)
	}{
		{
			name:     "Happy path: Logs and events should be uploaded to Azure Blob on deletion",
			testFunc: testAzureBlobUploadOnGracefulShutdown,
		},
		{
			name:     "Simulate OOMKilled behavior: Single session single node logs and events should be uploaded to Azure Blob after the ray-head container is restarted",
			testFunc: testAzureBlobSeparatesFilesBySession,
		},
		{
			name:     "Collector restart: should scan prev-logs and resume uploads left by a crash",
			testFunc: testAzureBlobResumesUploadsOnRestart,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)
			namespace := test.NewTestNamespace()

			tt.testFunc(test, g, namespace, azureClient)
		})
	}
}

// testAzureBlobUploadOnGracefulShutdown verifies that logs, node_events, and job_events are successfully uploaded to Azure Blob on cluster deletion.
func testAzureBlobUploadOnGracefulShutdown(test Test, g *WithT, namespace *corev1.Namespace, azureClient *azblob.Client) {
	rayCluster := PrepareAzureBlobTestEnv(test, g, namespace, azureClient)

	_ = ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	headNodeID := GetNodeIDFromPod(test, g, HeadPod(test, rayCluster), "ray-head")
	workerNodeID := GetNodeIDFromPod(test, g, FirstWorkerPod(test, rayCluster), "ray-worker")
	sessionPrefix := fmt.Sprintf("%s/", clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID))

	err := test.Client().Ray().RayV1().
		RayClusters(rayCluster.Namespace).
		Delete(test.Ctx(), rayCluster.Name, metav1.DeleteOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	g.Eventually(func() error {
		_, err := GetRayCluster(test, rayCluster.Namespace, rayCluster.Name)
		return err
	}, TestTimeoutMedium).Should(WithTransform(k8serrors.IsNotFound, BeTrue()))

	verifyAzureBlobSessionDirs(test, g, azureClient, sessionPrefix, headNodeID, workerNodeID)

	DeleteAzureBlobContainer(test, g, azureClient)
}

// testAzureBlobSeparatesFilesBySession verifies that logs and node_events are successfully uploaded to Azure Blob after the ray-head container is restarted.
func testAzureBlobSeparatesFilesBySession(test Test, g *WithT, namespace *corev1.Namespace, azureClient *azblob.Client) {
	rayCluster := PrepareAzureBlobTestEnv(test, g, namespace, azureClient)

	_ = ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	sessionID := GetSessionIDFromHeadPod(test, g, rayCluster)
	headNodeID := GetNodeIDFromPod(test, g, HeadPod(test, rayCluster), "ray-head")
	workerNodeID := GetNodeIDFromPod(test, g, FirstWorkerPod(test, rayCluster), "ray-worker")
	sessionPrefix := fmt.Sprintf("%s/", clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, sessionID))

	killContainerAndWaitForRestart(test, g, HeadPod(test, rayCluster), "ray-head")
	killContainerAndWaitForRestart(test, g, FirstWorkerPod(test, rayCluster), "ray-worker")

	verifySessionDirectoriesExist(test, g, rayCluster, sessionID)
	verifyAzureBlobSessionDirs(test, g, azureClient, sessionPrefix, headNodeID, workerNodeID)

	DeleteAzureBlobContainer(test, g, azureClient)
}

// testAzureBlobResumesUploadsOnRestart verifies that the Collector scans and resumes uploads from the prev-logs directory on startup.
func testAzureBlobResumesUploadsOnRestart(test Test, g *WithT, namespace *corev1.Namespace, azureClient *azblob.Client) {
	rayCluster := PrepareAzureBlobTestEnv(test, g, namespace, azureClient)

	prevLogsBaseDir := utils.GetRayPrevLogsPath()
	persistCompleteBaseDir := utils.GetRayPersistCompletePath()

	dummySessionID := fmt.Sprintf("test-recovery-session-%s", namespace.Name)
	dummyNodeID := fmt.Sprintf("head-node-%s", namespace.Name)
	sessionPrefix := fmt.Sprintf("%s/", clusterlogs.SessionDir("", "", "", rayCluster.Namespace, rayCluster.Name, dummySessionID))

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

	LogWithTimestamp(test.T(), "Killing collector container to test startup scanning of prev-logs")
	_, stderrKill := ExecPodCmd(test, headPod, "collector", []string{"kill", "1"})
	g.Expect(stderrKill.String()).To(BeEmpty())

	LogWithTimestamp(test.T(), "Waiting for collector container to restart and become ready")
	g.Eventually(func(gg Gomega) {
		updatedPod, err := GetHeadPod(test, rayCluster)
		gg.Expect(err).NotTo(HaveOccurred())
		cs, err := GetContainerStatusByName(updatedPod, "collector")
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(cs.RestartCount).To(BeNumerically(">", 0))
		gg.Expect(cs.Ready).To(BeTrue())
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying file2.log was uploaded to Azure Blob as a chunk (idempotency check)")
	g.Eventually(func(gg Gomega) {
		logsPrefix := sessionPrefix
		containerClient := azureClient.ServiceClient().NewContainerClient(AzureContainerName)
		pager := containerClient.NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{
			Prefix: &logsPrefix,
		})

		var uploadedKeys []string
		for pager.More() {
			resp, err := pager.NextPage(test.Ctx())
			gg.Expect(err).NotTo(HaveOccurred())
			for _, blob := range resp.Segment.BlobItems {
				if blob.Name == nil {
					continue
				}
				uploadedKeys = append(uploadedKeys, *blob.Name)
			}
		}
		LogWithTimestamp(test.T(), "Found uploaded objects: %v", uploadedKeys)

		// Append-only logs are uploaded by chunk, so file2.log lands under
		// "file2.log.chunks/<offset>" rather than as a whole object.
		hasFile2 := false
		for _, key := range uploadedKeys {
			if strings.Contains(key, "/file2.log"+clusterlogs.ChunkDirSuffix+"/") {
				hasFile2 = true
				break
			}
		}
		gg.Expect(hasFile2).To(BeTrue(), "file2.log should be uploaded to Azure Blob because it was in prev-logs")

		// Note: file1.log was only placed in persist-complete-logs (local marker),
		// it was never actually uploaded to Azure Blob in this test scenario.
		// The persist-complete-logs directory is just a local marker to prevent re-upload.
	}, TestTimeoutMedium).Should(Succeed())

	LogWithTimestamp(test.T(), "Verifying local state: node directory should be moved to %s", persistCompleteBaseDir)
	g.Eventually(func(gg Gomega) {
		currentHeadPod, err := GetHeadPod(test, rayCluster)
		gg.Expect(err).NotTo(HaveOccurred())
		persistPath := filepath.Join(persistCompleteBaseDir, dummySessionID, dummyNodeID)
		checkCmd := fmt.Sprintf("test -d %s && echo 'exists'", persistPath)
		stdout, stderrCheck := ExecPodCmd(test, currentHeadPod, "ray-head", []string{"sh", "-c", checkCmd})
		gg.Expect(stderrCheck.String()).To(BeEmpty())
		gg.Expect(strings.TrimSpace(stdout.String())).To(Equal("exists"), "Node directory should be in persist-complete-logs")

		prevPath := filepath.Join(prevLogsBaseDir, dummySessionID, dummyNodeID)
		checkGoneCmd := fmt.Sprintf("test ! -d %s && echo 'gone'", prevPath)
		stdoutGone, stderrGone := ExecPodCmd(test, currentHeadPod, "ray-head", []string{"sh", "-c", checkGoneCmd})
		gg.Expect(stderrGone.String()).To(BeEmpty())
		gg.Expect(strings.TrimSpace(stdoutGone.String())).To(Equal("gone"), "Node directory should be cleaned from prev-logs")
	}, TestTimeoutMedium).Should(Succeed())

	DeleteAzureBlobContainer(test, g, azureClient)
}

// verifyAzureBlobSessionDirs verifies file contents in logs/, node_events/, and job_events/ directories.
func verifyAzureBlobSessionDirs(test Test, g *WithT, azureClient *azblob.Client, sessionPrefix string, headNodeID string, workerNodeID string) {
	containerClient := azureClient.ServiceClient().NewContainerClient(AzureContainerName)

	headLogDirPrefix := fmt.Sprintf("%s%s/logs", sessionPrefix, headNodeID)
	workerLogDirPrefix := fmt.Sprintf("%s%s/logs", sessionPrefix, workerNodeID)

	LogWithTimestamp(test.T(), "Verifying raylet.out, gcs_server.out, and monitor.out exist in head log directory %s", headLogDirPrefix)
	for _, fileName := range []string{"raylet.out", "gcs_server.out", "monitor.out"} {
		assertAzureBlobFileExist(test, g, containerClient, headLogDirPrefix, fileName)
	}

	LogWithTimestamp(test.T(), "Verifying raylet.out exists in worker log directory %s", workerLogDirPrefix)
	assertAzureBlobFileExist(test, g, containerClient, workerLogDirPrefix, "raylet.out")

	LogWithTimestamp(test.T(), "Verifying all %d event types are covered, except for EVENT_TYPE_UNSPECIFIED: %v", len(types.AllEventTypes)-1, types.AllEventTypes)
	g.Eventually(func(gg Gomega) {
		events, err := loadRayEventsFromAzureBlob(containerClient, sessionPrefix)
		gg.Expect(err).NotTo(HaveOccurred())

		assertAllEventTypesCovered(test, gg, events)
	}, TestTimeoutMedium).Should(Succeed())
}

func listAzureBlobDirectories(containerClient *container.Client, prefix string) ([]string, error) {
	pager := containerClient.NewListBlobsHierarchyPager("/", &container.ListBlobsHierarchyOptions{
		Prefix: &prefix,
	})

	var directories []string
	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list Azure Blob directories under %s: %w", prefix, err)
		}

		for _, blobPrefix := range resp.Segment.BlobPrefixes {
			if blobPrefix.Name == nil {
				continue
			}
			dirName := strings.TrimSuffix(strings.TrimPrefix(*blobPrefix.Name, prefix), "/")
			if dirName != "" {
				directories = append(directories, dirName)
			}
		}
	}

	return directories, nil
}

func loadRayEventsFromAzureBlob(containerClient *container.Client, prefix string) ([]rayEvent, error) {
	var events []rayEvent

	pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix: &prefix,
	})

	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			return nil, fmt.Errorf("failed to list blobs under %s: %w", prefix, err)
		}

		for _, blob := range resp.Segment.BlobItems {
			if blob.Name == nil || strings.HasSuffix(*blob.Name, "/") || (!strings.Contains(*blob.Name, "/node_events/") && !strings.Contains(*blob.Name, "/job_events/")) {
				continue
			}

			blobClient := containerClient.NewBlobClient(*blob.Name)
			downloadResp, err := blobClient.DownloadStream(context.Background(), nil)
			if err != nil {
				return nil, fmt.Errorf("failed to download blob %s: %w", *blob.Name, err)
			}

			var reader io.Reader = downloadResp.Body
			var gzReader *gzip.Reader
			if strings.HasSuffix(*blob.Name, ".gz") {
				var err error
				gzReader, err = gzip.NewReader(downloadResp.Body)
				if err != nil {
					downloadResp.Body.Close()
					return nil, fmt.Errorf("failed to create gzip reader for %s: %w", *blob.Name, err)
				}
				reader = gzReader
			}

			fileEvents, decodeErr := decodeJSONLEvents(reader)
			if gzReader != nil {
				gzReader.Close()
			}
			downloadResp.Body.Close()

			if decodeErr != nil {
				return nil, fmt.Errorf("failed to decode Ray events from %s: %w", *blob.Name, decodeErr)
			}

			events = append(events, fileEvents...)
		}
	}

	return events, nil
}

// assertAzureBlobFileExist verifies that a file object exists under the given log directory prefix.
func assertAzureBlobFileExist(test Test, g *WithT, containerClient *container.Client, nodeLogDirPrefix string, fileName string) {
	fileKey := fmt.Sprintf("%s/%s", nodeLogDirPrefix, fileName)
	LogWithTimestamp(test.T(), "Verifying file %s exists", fileKey)
	g.Eventually(func(gg Gomega) {
		blobClient := containerClient.NewBlobClient(fileKey)
		if _, err := blobClient.GetProperties(context.Background(), nil); err == nil {
			LogWithTimestamp(test.T(), "Verified file %s exists", fileKey)
			return
		}
		// Append-only logs are stored as chunks under "<fileName>.chunks/".
		chunkPrefix := fileKey + clusterlogs.ChunkDirSuffix + "/"
		pager := containerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &chunkPrefix})
		// The collector writes a "<file>.chunks/" directory marker before the
		// first chunk; only count real chunk blobs.
		found := 0
		for pager.More() {
			page, err := pager.NextPage(context.Background())
			gg.Expect(err).NotTo(HaveOccurred())
			for _, blob := range page.Segment.BlobItems {
				if blob.Name != nil && !strings.HasSuffix(*blob.Name, "/") {
					found++
				}
			}
		}
		gg.Expect(found).NotTo(BeZero(), "neither %s nor any chunk under %s exists", fileKey, chunkPrefix)
		LogWithTimestamp(test.T(), "Verified file %s exists as %d chunk(s)", fileKey, found)
	}, TestTimeoutMedium).Should(Succeed(), "Failed to verify file %s exists", fileKey)
}
