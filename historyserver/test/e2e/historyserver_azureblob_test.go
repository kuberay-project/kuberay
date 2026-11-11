package e2e

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"

	. "github.com/ray-project/kuberay/historyserver/test/support"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestAzureHistoryServer(t *testing.T) {
	azureClient := EnsureAzureBlobClient(t)

	tests := []struct {
		name     string
		testFunc func(Test, *WithT, *corev1.Namespace, *azblob.Client)
	}{
		{
			name:     "Live cluster: historyserver endpoints should be accessible",
			testFunc: testAzureLiveClusters,
		},
		{
			name:     "Dead cluster: historyserver endpoints should be accessible",
			testFunc: testAzureDeadClusters,
		},
		{
			name:     "/v0/logs/file endpoint (live cluster)",
			testFunc: testAzureLogFileEndpointLiveCluster,
		},
		{
			name:     "/v0/logs/file endpoint (dead cluster)",
			testFunc: testAzureLogFileEndpointDeadCluster,
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

func testAzureLiveClusters(test Test, g *WithT, namespace *corev1.Namespace, azureClient *azblob.Client) {
	rayCluster := PrepareAzureBlobTestEnv(test, g, namespace, azureClient)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, AzureHistoryServerManifestPath, EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).To(Equal(LiveSessionName), "Live cluster should have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	verifyHistoryServerEndpoints(test, g, client, historyServerURL)

	DeleteAzureBlobContainer(test, g, azureClient)
	LogWithTimestamp(test.T(), "Azure live clusters E2E test completed successfully")
}

func testAzureDeadClusters(test Test, g *WithT, namespace *corev1.Namespace, azureClient *azblob.Client) {
	rayCluster := PrepareAzureBlobTestEnv(test, g, namespace, azureClient)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)

	ApplyHistoryServer(test, g, namespace, AzureHistoryServerManifestPath)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName), "Dead cluster should not have sessionName='live'")

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)
	verifyHistoryServerEndpoints(test, g, client, historyServerURL)

	DeleteAzureBlobContainer(test, g, azureClient)
	LogWithTimestamp(test.T(), "Azure dead clusters E2E test completed successfully")
}

func testAzureLogFileEndpointLiveCluster(test Test, g *WithT, namespace *corev1.Namespace, azureClient *azblob.Client) {
	rayCluster := PrepareAzureBlobTestEnv(test, g, namespace, azureClient)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)
	ApplyHistoryServer(test, g, namespace, AzureHistoryServerManifestPath, EnableLiveClustersArg)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	nodeID := GetOneOfNodeID(g, client, historyServerURL, false)

	test.T().Run("should return log content", func(t *testing.T) {
		VerifyLogFileEndpointReturnsContent(test, NewWithT(t), client, historyServerURL, nodeID)
	})

	test.T().Run("should reject path traversal", func(t *testing.T) {
		VerifyLogFileEndpointRejectsPathTraversal(test, NewWithT(t), client, historyServerURL, nodeID)
	})

	DeleteAzureBlobContainer(test, g, azureClient)
	LogWithTimestamp(test.T(), "Azure log file endpoint tests completed")
}

func testAzureLogFileEndpointDeadCluster(test Test, g *WithT, namespace *corev1.Namespace, azureClient *azblob.Client) {
	rayCluster := PrepareAzureBlobTestEnv(test, g, namespace, azureClient)
	ApplyRayJobAndWaitForCompletion(test, g, namespace, rayCluster)

	DeleteRayClusterAndWait(test, g, namespace.Name, rayCluster.Name)

	ApplyHistoryServer(test, g, namespace, AzureHistoryServerManifestPath)
	historyServerURL := GetHistoryServerURL(test, g, namespace)

	clusterInfo := getClusterFromList(test, g, historyServerURL, rayCluster.Name, namespace.Name)
	g.Expect(clusterInfo.SessionName).NotTo(Equal(LiveSessionName))

	client := CreateHTTPClientWithCookieJar(test, g)
	setClusterContext(test, g, client, historyServerURL, namespace.Name, rayCluster.Name, clusterInfo.SessionName)

	nodeID := GetOneOfNodeID(g, client, historyServerURL, false)

	test.T().Run("should return log content from Azure Blob", func(t *testing.T) {
		VerifyLogFileEndpointReturnsContent(test, NewWithT(t), client, historyServerURL, nodeID)
	})

	test.T().Run("should reject path traversal from Azure Blob", func(t *testing.T) {
		VerifyLogFileEndpointRejectsPathTraversal(test, NewWithT(t), client, historyServerURL, nodeID)
	})

	DeleteAzureBlobContainer(test, g, azureClient)
	LogWithTimestamp(test.T(), "Azure dead cluster log file endpoint tests completed")
}
