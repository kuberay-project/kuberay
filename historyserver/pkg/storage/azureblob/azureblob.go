package azureblob

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const (
	// Timeout for upload operations
	uploadTimeout = 5 * time.Minute
	// Timeout for listing operations
	listTimeout = 2 * time.Minute
	// Timeout for download operations (longer to handle large files)
	downloadTimeout = 10 * time.Minute
)

type RayLogsHandler struct {
	ContainerClient     *container.Client
	LogFiles            chan string
	ContainerName       string
	SessionDir          string
	RootDir             string
	LogDir              string
	RayClusterName      string
	RayClusterNamespace string
	RayNodeName         string
	LogBatching         int
	PushInterval        time.Duration
}

func (r *RayLogsHandler) CreateDirectory(_ string) error {
	// Azure Blob Storage doesn't require explicit directory markers.
	// Virtual directories are automatically inferred from blob paths.
	// Creating empty marker blobs causes "<no name>" display issues
	// in Azure Storage Explorer.
	return nil
}

func (r *RayLogsHandler) WriteFile(file string, reader io.ReadSeeker) error {
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	blobClient := r.ContainerClient.NewBlockBlobClient(file)

	_, err := blobClient.UploadStream(ctx, reader, nil)
	if err != nil {
		logrus.Errorf("Failed to upload file %s: %v", file, err)
		return err
	}

	return nil
}

func (r *RayLogsHandler) listBlobs(prefix string, delimiter string, onlyBase bool) []string {
	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()

	files := []string{}
	prefixWithSlash := prefix + "/"

	if delimiter != "" {
		// Hierarchical listing (directory-like)
		pager := r.ContainerClient.NewListBlobsHierarchyPager(delimiter, &container.ListBlobsHierarchyOptions{
			Prefix:     &prefixWithSlash,
			MaxResults: new(int32(100)),
		})

		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				logrus.Errorf("Failed to list blobs from %s: %v", prefixWithSlash, err)
				return []string{}
			}

			logrus.Infof("[ListFiles]Returned blobs in %v. length of Segment.BlobItems: %v, length of Segment.BlobPrefixes: %v",
				prefixWithSlash, len(resp.Segment.BlobItems), len(resp.Segment.BlobPrefixes))

			for _, blob := range resp.Segment.BlobItems {
				objName := *blob.Name
				if onlyBase {
					objName = path.Base(*blob.Name)
				}
				files = append(files, objName)
			}

			for _, prefix := range resp.Segment.BlobPrefixes {
				objName := *prefix.Name
				if onlyBase {
					objName = path.Base(*prefix.Name)
				}
				files = append(files, objName+"/")
			}
		}
	} else {
		// Flat listing
		pager := r.ContainerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
			Prefix:     &prefixWithSlash,
			MaxResults: new(int32(100)),
		})

		for pager.More() {
			resp, err := pager.NextPage(ctx)
			if err != nil {
				logrus.Errorf("Failed to list blobs from %s: %v", prefixWithSlash, err)
				return []string{}
			}

			logrus.Infof("[ListFiles]Returned blobs in %v. length of Segment.BlobItems: %v",
				prefixWithSlash, len(resp.Segment.BlobItems))

			for _, blob := range resp.Segment.BlobItems {
				objName := *blob.Name
				if onlyBase {
					objName = path.Base(*blob.Name)
				}
				files = append(files, objName)
			}
		}
	}

	return files
}

func (r *RayLogsHandler) ListFiles(prefix string, dir string) []string {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("Recovered from panic: %v", r)
		}
	}()

	fullPrefix := path.Join(r.RootDir, prefix, dir)
	logrus.Debugf("Prepare to list files ...")
	return r.listBlobs(fullPrefix, "/", true)
}

func (r *RayLogsHandler) List() (res []utils.ClusterInfo) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Errorf("Recovered from panic: %v", r)
		}
	}()

	clusters := make(utils.ClusterInfoList, 0, 10)
	logrus.Debugf("Prepare to get list clusters info ...")

	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()

	prefix := clustermetadata.Prefix(r.RootDir)

	pager := r.ContainerClient.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{
		Prefix:     &prefix,
		MaxResults: new(int32(100)),
	})

	for pager.More() {
		resp, err := pager.NextPage(ctx)
		if err != nil {
			logrus.Errorf("Failed to list blobs from %s: %v", prefix, err)
			break
		}

		logrus.Infof("[List]Returned blobs in %v. length of Segment.BlobItems: %v",
			prefix, len(resp.Segment.BlobItems))

		for _, blob := range resp.Segment.BlobItems {
			c, err := clustermetadata.DecodePath(*blob.Name, r.RootDir)
			if err != nil {
				logrus.Errorf("Failed to parse meta file path: %s, error: %v", *blob.Name, err)
				continue
			}
			clusters = append(clusters, c)
		}
	}

	sort.Sort(clusters)
	return clusters
}

func (r *RayLogsHandler) GetContent(prefix string, fileName string) io.Reader {
	fullPath := path.Join(r.RootDir, prefix, fileName)
	logrus.Infof("Prepare to get blob %s info ...", fullPath)

	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()

	blobClient := r.ContainerClient.NewBlobClient(fullPath)

	resp, err := blobClient.DownloadStream(ctx, nil)
	if err != nil {
		// Close the response body if it exists to prevent connection leak
		if resp.Body != nil {
			resp.Body.Close()
		}
		logrus.Errorf("Failed to get blob %s: %v", fullPath, err)

		// Try to find the file by listing direct children only (use delimiter)
		dirPath := path.Dir(fullPath)
		allFiles := r.listBlobs(dirPath, "/", false)
		for _, f := range allFiles {
			// Match full path to avoid returning wrong content from nested directories
			if f == fullPath {
				logrus.Infof("Get blob %s info success", f)
				blobClient = r.ContainerClient.NewBlobClient(f)
				// Create fresh context for retry to avoid timeout from listing operation
				retryCtx, retryCancel := context.WithTimeout(context.Background(), downloadTimeout)
				resp, err = blobClient.DownloadStream(retryCtx, nil)
				if err != nil {
					retryCancel()
					if resp.Body != nil {
						resp.Body.Close()
					}
					logrus.Errorf("Failed to get blob %s: %v", f, err)
					return nil
				}
				// Read body before canceling context to avoid incomplete stream
				defer retryCancel()
				defer resp.Body.Close()
				data, err := io.ReadAll(resp.Body)
				if err != nil {
					logrus.Errorf("Failed to read all data from blob %s: %v", fileName, err)
					return nil
				}
				return bytes.NewReader(data)
			}
		}
		logrus.Errorf("Failed to get blob by listing all files %s", fileName)
		return nil
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		logrus.Errorf("Failed to read all data from blob %s: %v", fileName, err)
		return nil
	}
	return bytes.NewReader(data)
}

func NewReader(c *types.RayHistoryServerConfig, jd map[string]any) (storage.StorageReader, error) {
	cfg := &config{}
	cfg.completeHSConfig(c, jd)
	return New(cfg)
}

func NewWriter(c *types.RayCollectorConfig, jd map[string]any) (storage.StorageWriter, error) {
	cfg := &config{}
	cfg.complete(c, jd)
	return New(cfg)
}

func createAzureBlobClient(c *config) (*azblob.Client, error) {
	// Auto-detect auth mode if not specified
	authMode := c.AuthMode
	if authMode == "" {
		if c.ConnectionString != "" {
			authMode = AuthModeConnectionString
		} else if c.AccountURL != "" {
			authMode = AuthModeDefault
		} else {
			return nil, fmt.Errorf("either AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL must be set")
		}
	}

	if authMode == AuthModeConnectionString {
		logrus.Info("Using connection string authentication")
		return azblob.NewClientFromConnectionString(c.ConnectionString, nil)
	}

	// Token-based authentication
	var cred *azidentity.DefaultAzureCredential
	var err error
	if authMode == AuthModeWorkloadIdentity {
		logrus.Info("Using workload identity authentication")
		wiCred, err := azidentity.NewWorkloadIdentityCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create workload identity credential: %w", err)
		}
		return azblob.NewClient(c.AccountURL, wiCred, nil)
	}

	logrus.Info("Using default Azure credential chain")
	cred, err = azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create default credential: %w", err)
	}
	return azblob.NewClient(c.AccountURL, cred, nil)
}

func ensureContainerExists(ctx context.Context, client *azblob.Client, containerName string) error {
	containerClient := client.ServiceClient().NewContainerClient(containerName)

	_, err := containerClient.GetProperties(ctx, nil)
	if err != nil {
		// Container doesn't exist, try to create it
		logrus.Infof("Container %s does not exist, creating...", containerName)
		_, err = containerClient.Create(ctx, nil)
		if err != nil {
			// Check if container already exists (race condition)
			var respErr *azcore.ResponseError
			if errors.As(err, &respErr) && respErr.ErrorCode == "ContainerAlreadyExists" {
				logrus.Infof("Container %s already exists", containerName)
				return nil
			}
			logrus.Errorf("Failed to create container %s: %v", containerName, err)
			return fmt.Errorf("failed to create container %s: %w", containerName, err)
		}
		logrus.Infof("Successfully created container %s", containerName)
		return nil
	}

	logrus.Infof("Container %s already exists", containerName)
	return nil
}

func New(c *config) (*RayLogsHandler, error) {
	logrus.Infof("Begin to create azure blob client ...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := createAzureBlobClient(c)
	if err != nil {
		logrus.Errorf("Failed to create azure blob client: %v", err)
		return nil, err
	}

	// Ensure container exists
	if err := ensureContainerExists(ctx, client, c.ContainerName); err != nil {
		return nil, fmt.Errorf("failed to ensure container exists: %w", err)
	}

	containerClient := client.ServiceClient().NewContainerClient(c.ContainerName)

	sessionDir := strings.TrimSpace(c.SessionDir)
	sessionDir = filepath.Clean(sessionDir)

	logdir := strings.TrimSpace(path.Join(sessionDir, utils.RAY_SESSIONDIR_LOGDIR_NAME))
	logdir = filepath.Clean(logdir)
	logrus.Infof("Clean logdir is %s", logdir)

	return &RayLogsHandler{
		ContainerClient:     containerClient,
		LogFiles:            make(chan string, 100),
		ContainerName:       c.ContainerName,
		SessionDir:          sessionDir,
		RootDir:             c.RootDir,
		LogDir:              logdir,
		RayClusterName:      c.RayClusterName,
		RayClusterNamespace: c.RayClusterNamespace,
		RayNodeName:         c.RayNodeName,
		LogBatching:         c.LogBatching,
		PushInterval:        c.PushInterval,
	}, nil
}
