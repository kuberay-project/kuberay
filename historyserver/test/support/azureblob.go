package support

import (
	"context"
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

const (
	// Azurite configuration
	AzuriteNamespace    = "azurite-dev"
	AzuriteManifestPath = "../../config/azurite.yaml"
	AzuriteAPIPort      = 10000
	AzuriteAccountName  = "devstoreaccount1"
	AzuriteAccountKey   = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	AzureContainerName  = "ray-historyserver"

	// Azure-specific RayCluster config
	AzureRayClusterManifestPath    = "../testdata/raycluster-azureblob.yaml"
	AzureHistoryServerManifestPath = "../../config/historyserver-azureblob.yaml"
)

// ApplyAzurite deploys Azurite once per test namespace.
func ApplyAzurite(test Test, g *WithT) {
	KubectlApplyYAML(test, AzuriteManifestPath, AzuriteNamespace)

	g.Eventually(func(gg Gomega) {
		pods, err := test.Client().Core().CoreV1().Pods(AzuriteNamespace).List(
			test.Ctx(), metav1.ListOptions{
				LabelSelector: "app=azurite",
			},
		)
		gg.Expect(err).NotTo(HaveOccurred())
		gg.Expect(pods.Items).NotTo(BeEmpty())
		gg.Expect(AllPodsRunningAndReady(pods.Items)).To(BeTrue())
	}, TestTimeoutMedium).Should(Succeed())
}

// EnsureAzureBlobClient creates an Azure Blob client and ensures API endpoint accessibility.
func EnsureAzureBlobClient(t *testing.T) *azblob.Client {
	test := With(t)
	g := NewWithT(t)
	ApplyAzurite(test, g)

	PortForwardService(test, g, AzuriteNamespace, "azurite-service", AzuriteAPIPort)

	g.Eventually(func() error {
		azureClient, err := NewAzureBlobClient()
		if err != nil {
			return fmt.Errorf("failed to create Azure Blob client: %w", err)
		}
		_, err = azureClient.ServiceClient().NewListContainersPager(nil).NextPage(context.Background())
		if err != nil {
			return fmt.Errorf("failed to list containers: %w", err)
		}
		return nil
	}, TestTimeoutMedium).Should(Succeed(), "Azurite API endpoint should be ready")
	LogWithTimestamp(test.T(), "Port-forwarded Azurite API port to localhost:%d successfully", AzuriteAPIPort)

	azureClient, err := NewAzureBlobClient()
	g.Expect(err).NotTo(HaveOccurred())

	return azureClient
}

// NewAzureBlobClient creates a new Azure Blob client using Azurite connection string.
func NewAzureBlobClient() (*azblob.Client, error) {
	connectionString := fmt.Sprintf(
		"DefaultEndpointsProtocol=http;AccountName=%s;AccountKey=%s;BlobEndpoint=http://127.0.0.1:%d/%s;",
		AzuriteAccountName,
		AzuriteAccountKey,
		AzuriteAPIPort,
		AzuriteAccountName,
	)
	return azblob.NewClientFromConnectionString(connectionString, nil)
}

// DeleteAzureBlobContainer deletes all blobs in the Azure container.
func DeleteAzureBlobContainer(test Test, _ *WithT, azureClient *azblob.Client) {
	LogWithTimestamp(test.T(), "Deleting Azure Blob container %s", AzureContainerName)

	containerClient := azureClient.ServiceClient().NewContainerClient(AzureContainerName)
	pager := containerClient.NewListBlobsFlatPager(nil)

	for pager.More() {
		resp, err := pager.NextPage(context.Background())
		if err != nil {
			test.T().Logf("Failed to list blobs in container: %v", err)
			break
		}

		for _, blob := range resp.Segment.BlobItems {
			if blob.Name == nil {
				continue
			}
			blobClient := containerClient.NewBlobClient(*blob.Name)
			_, err := blobClient.Delete(context.Background(), nil)
			if err != nil {
				test.T().Logf("Failed to delete blob %s: %v", *blob.Name, err)
			}
		}
	}

	_, err := containerClient.Delete(context.Background(), nil)
	if err != nil {
		test.T().Logf("Failed to delete container %s: %v (this is OK if container doesn't exist)", AzureContainerName, err)
	} else {
		LogWithTimestamp(test.T(), "Deleted Azure Blob container %s successfully", AzureContainerName)
	}
}

// PrepareAzureBlobTestEnv prepares test environment for each Azure Blob test case.
func PrepareAzureBlobTestEnv(test Test, g *WithT, namespace *corev1.Namespace, azureClient *azblob.Client) *rayv1.RayCluster {
	rayCluster := ApplyAzureRayClusterWithCollector(test, g, namespace)

	headPod, err := GetHeadPod(test, rayCluster)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(headPod.Spec.Containers).To(ContainElement(
		WithTransform(func(c corev1.Container) string { return c.Name }, Equal("collector")),
	))

	containerClient := azureClient.ServiceClient().NewContainerClient(AzureContainerName)
	_, err = containerClient.GetProperties(context.Background(), nil)
	g.Expect(err).NotTo(HaveOccurred())

	return rayCluster
}

// ApplyAzureRayClusterWithCollector deploys a Ray cluster with the collector sidecar configured for Azure Blob Storage.
func ApplyAzureRayClusterWithCollector(test Test, g *WithT, namespace *corev1.Namespace) *rayv1.RayCluster {
	rayClusterFromYaml := DeserializeRayClusterYAML(test, AzureRayClusterManifestPath)
	rayClusterFromYaml.Namespace = namespace.Name

	// Inject namespace name as the ray-cluster-namespace for head group collector
	injectCollectorRayClusterNamespaceAndEnvVar(rayClusterFromYaml.Spec.HeadGroupSpec.Template.Spec.Containers, rayClusterFromYaml.Name, namespace.Name)

	// Inject namespace name as the ray-cluster-namespace for worker group collectors
	for wg := range rayClusterFromYaml.Spec.WorkerGroupSpecs {
		injectCollectorRayClusterNamespaceAndEnvVar(rayClusterFromYaml.Spec.WorkerGroupSpecs[wg].Template.Spec.Containers, rayClusterFromYaml.Name, namespace.Name)
	}

	rayCluster, err := test.Client().Ray().RayV1().
		RayClusters(namespace.Name).
		Create(test.Ctx(), rayClusterFromYaml, metav1.CreateOptions{})
	g.Expect(err).NotTo(HaveOccurred())
	LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

	LogWithTimestamp(test.T(), "Waiting for RayCluster %s/%s to become ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(RayCluster(test, rayCluster.Namespace, rayCluster.Name), TestTimeoutLong).
		Should(WithTransform(RayClusterState, Equal(rayv1.Ready)))

	LogWithTimestamp(test.T(), "Waiting for head pod of RayCluster %s/%s to be running and ready", rayCluster.Namespace, rayCluster.Name)
	g.Eventually(HeadPod(test, rayCluster), TestTimeoutMedium).
		Should(WithTransform(IsPodRunningAndReady, BeTrue()))

	return rayCluster
}
