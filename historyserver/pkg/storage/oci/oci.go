// Package oci implements the history server storage backend for Oracle Cloud
// Infrastructure Object Storage.
package oci

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/common/auth"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
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
	// Timeout for resolving the namespace and checking the bucket at startup
	setupTimeout = time.Minute
	// listPageSize is the largest page ListObjects accepts.
	listPageSize = 1000
)

// objectStorageClient is the subset of objectstorage.ObjectStorageClient the backend
// uses. Tests substitute an in-memory implementation.
type objectStorageClient interface {
	GetNamespace(ctx context.Context, request objectstorage.GetNamespaceRequest) (objectstorage.GetNamespaceResponse, error)
	HeadBucket(ctx context.Context, request objectstorage.HeadBucketRequest) (objectstorage.HeadBucketResponse, error)
	CreateBucket(ctx context.Context, request objectstorage.CreateBucketRequest) (objectstorage.CreateBucketResponse, error)
	HeadObject(ctx context.Context, request objectstorage.HeadObjectRequest) (objectstorage.HeadObjectResponse, error)
	PutObject(ctx context.Context, request objectstorage.PutObjectRequest) (objectstorage.PutObjectResponse, error)
	GetObject(ctx context.Context, request objectstorage.GetObjectRequest) (objectstorage.GetObjectResponse, error)
	ListObjects(ctx context.Context, request objectstorage.ListObjectsRequest) (objectstorage.ListObjectsResponse, error)
}

var (
	_ objectStorageClient   = (*objectstorage.ObjectStorageClient)(nil)
	_ storage.StorageWriter = (*RayLogsHandler)(nil)
	_ storage.StorageReader = (*RayLogsHandler)(nil)
)

type RayLogsHandler struct {
	Client              objectStorageClient
	LogFiles            chan string
	Namespace           string
	Bucket              string
	SessionDir          string
	RootDir             string
	LogDir              string
	RayClusterName      string
	RayClusterNamespace string
	RayNodeName         string
	LogBatching         int
	PushInterval        time.Duration
}

// requestMetadata attaches the SDK default retry policy (exponential backoff on
// throttling and transient 5xx errors). Without it every request is issued exactly once.
func requestMetadata() common.RequestMetadata {
	policy := common.DefaultRetryPolicy()
	return common.RequestMetadata{RetryPolicy: &policy}
}

// objectName joins path elements into an Object Storage name. Object Storage is a
// flat namespace, so a leading slash would become part of the name and hide the
// object from the Console folder view.
func objectName(elem ...string) string {
	return strings.TrimPrefix(path.Join(elem...), "/")
}

func isHTTPStatus(err error, status int) bool {
	if serviceErr, ok := common.IsServiceError(err); ok {
		return serviceErr.GetHTTPStatusCode() == status
	}
	return false
}

// readSeekCloser lets the SDK rewind the request body when it retries an upload.
type readSeekCloser struct {
	io.ReadSeeker
}

func (readSeekCloser) Close() error { return nil }

// remainingSize returns the number of bytes between the current position and the
// end of the stream, leaving the position where it was.
func remainingSize(rs io.ReadSeeker) (int64, error) {
	current, err := rs.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, err
	}
	end, err := rs.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if _, err := rs.Seek(current, io.SeekStart); err != nil {
		return 0, err
	}
	return end - current, nil
}

func (r *RayLogsHandler) putObject(ctx context.Context, name string, body io.ReadSeeker) error {
	size, err := remainingSize(body)
	if err != nil {
		return fmt.Errorf("determine size of %s: %w", name, err)
	}
	_, err = r.Client.PutObject(ctx, objectstorage.PutObjectRequest{
		NamespaceName:   &r.Namespace,
		BucketName:      &r.Bucket,
		ObjectName:      &name,
		ContentLength:   &size,
		PutObjectBody:   readSeekCloser{body},
		RequestMetadata: requestMetadata(),
	})
	return err
}

func (r *RayLogsHandler) getObject(ctx context.Context, name string) ([]byte, error) {
	resp, err := r.Client.GetObject(ctx, objectstorage.GetObjectRequest{
		NamespaceName:   &r.Namespace,
		BucketName:      &r.Bucket,
		ObjectName:      &name,
		RequestMetadata: requestMetadata(),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Content.Close()
	return io.ReadAll(resp.Content)
}

// CreateDirectory stores a zero-byte "<dir>/" marker so the directory shows up as a
// folder in the OCI Console, matching the S3, GCS and Aliyun OSS backends.
func (r *RayLogsHandler) CreateDirectory(d string) error {
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	marker := objectName(d) + "/"
	_, err := r.Client.HeadObject(ctx, objectstorage.HeadObjectRequest{
		NamespaceName:   &r.Namespace,
		BucketName:      &r.Bucket,
		ObjectName:      &marker,
		RequestMetadata: requestMetadata(),
	})
	if err == nil {
		return nil
	}
	if !isHTTPStatus(err, http.StatusNotFound) {
		logrus.Errorf("Failed to check if directory %s exists: %v", marker, err)
		return err
	}

	logrus.Infof("Begin to create oci dir %s ...", marker)
	if err := r.putObject(ctx, marker, bytes.NewReader(nil)); err != nil {
		logrus.Errorf("Failed to create directory '%s': %v", marker, err)
		return err
	}
	logrus.Infof("Create oci dir %s success", marker)
	return nil
}

func (r *RayLogsHandler) WriteFile(file string, reader io.ReadSeeker) error {
	ctx, cancel := context.WithTimeout(context.Background(), uploadTimeout)
	defer cancel()

	name := objectName(file)
	if err := r.putObject(ctx, name, reader); err != nil {
		logrus.Errorf("Failed to upload file %s: %v", name, err)
		return err
	}
	return nil
}

// listObjects returns every object name under prefix and, when delimiter is "/",
// the common prefixes one level below it. An empty delimiter lists recursively.
func (r *RayLogsHandler) listObjects(ctx context.Context, prefix string, delimiter string) (objects []string, prefixes []string, err error) {
	request := objectstorage.ListObjectsRequest{
		NamespaceName:   &r.Namespace,
		BucketName:      &r.Bucket,
		Prefix:          new(prefix),
		Fields:          new("name"),
		Limit:           new(listPageSize),
		RequestMetadata: requestMetadata(),
	}
	if delimiter != "" {
		request.Delimiter = new(delimiter)
	}

	for {
		resp, err := r.Client.ListObjects(ctx, request)
		if err != nil {
			return nil, nil, err
		}
		for _, object := range resp.Objects {
			if object.Name != nil {
				objects = append(objects, *object.Name)
			}
		}
		prefixes = append(prefixes, resp.Prefixes...)
		if resp.NextStartWith == nil || *resp.NextStartWith == "" {
			return objects, prefixes, nil
		}
		request.Start = resp.NextStartWith
	}
}

// ListFiles returns the base names of the files directly under dir plus its
// subdirectories suffixed with "/". Directory markers are not reported as files.
func (r *RayLogsHandler) ListFiles(clusterId string, dir string) []string {
	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()

	prefix := objectName(r.RootDir, clusterId, dir) + "/"
	logrus.Debugf("Prepare to list files under %s ...", prefix)
	objects, prefixes, err := r.listObjects(ctx, prefix, "/")
	if err != nil {
		logrus.Errorf("Failed to list objects from %s: %v", prefix, err)
		return []string{}
	}
	logrus.Infof("[ListFiles]Returned objects in %v. length of Objects: %v, length of Prefixes: %v",
		prefix, len(objects), len(prefixes))

	files := make([]string, 0, len(objects)+len(prefixes))
	for _, name := range objects {
		if strings.HasSuffix(name, "/") {
			continue
		}
		files = append(files, path.Base(name))
	}
	for _, p := range prefixes {
		files = append(files, path.Base(p)+"/")
	}
	return files
}

func (r *RayLogsHandler) List() []utils.ClusterInfo {
	ctx, cancel := context.WithTimeout(context.Background(), listTimeout)
	defer cancel()

	clusters := make(utils.ClusterInfoList, 0, 10)
	prefix := strings.TrimPrefix(clustermetadata.Prefix(r.RootDir), "/")
	logrus.Debugf("Prepare to get list clusters info from %s ...", prefix)

	objects, _, err := r.listObjects(ctx, prefix, "")
	if err != nil {
		logrus.Errorf("Failed to list objects from %s: %v", prefix, err)
		return clusters
	}
	logrus.Infof("[List]Returned objects in %v. length of Objects: %v", prefix, len(objects))

	for _, name := range objects {
		if strings.HasSuffix(name, "/") {
			continue
		}
		c, err := clustermetadata.DecodePath(name, r.RootDir)
		if err != nil {
			logrus.Errorf("Failed to parse meta file path: %s, error: %v", name, err)
			continue
		}
		clusters = append(clusters, c)
	}

	sort.Sort(clusters)
	return clusters
}

func (r *RayLogsHandler) GetContent(clusterId string, fileName string) io.Reader {
	ctx, cancel := context.WithTimeout(context.Background(), downloadTimeout)
	defer cancel()

	fullPath := objectName(r.RootDir, clusterId, fileName)
	logrus.Infof("Prepare to get object %s ...", fullPath)

	data, err := r.getObject(ctx, fullPath)
	if err == nil {
		return bytes.NewReader(data)
	}
	logrus.Errorf("Failed to get object %s: %v", fullPath, err)
	if !isHTTPStatus(err, http.StatusNotFound) {
		return nil
	}

	// Fall back to a recursive listing of the parent directory and match on the base
	// name, so a file stored one level deeper (for example under a node id) is still
	// served. This mirrors the S3 and Aliyun OSS backends.
	dirPrefix := path.Dir(fullPath) + "/"
	objects, _, err := r.listObjects(ctx, dirPrefix, "")
	if err != nil {
		logrus.Errorf("Failed to list objects from %s: %v", dirPrefix, err)
		return nil
	}
	for _, name := range objects {
		if strings.HasSuffix(name, "/") || path.Base(name) != path.Base(fullPath) {
			continue
		}
		data, err := r.getObject(ctx, name)
		if err != nil {
			logrus.Errorf("Failed to get object %s: %v", name, err)
			return nil
		}
		logrus.Infof("Get object %s success", name)
		return bytes.NewReader(data)
	}
	logrus.Errorf("Failed to get object by listing all files under %s", dirPrefix)
	return nil
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

func newConfigurationProvider(c *config, authType AuthType) (common.ConfigurationProvider, error) {
	switch authType {
	case AuthTypeAPIKey:
		if c.ConfigFile == "" && c.ConfigProfile == "" {
			// ~/.oci/config or OCI_CONFIG_FILE, the DEFAULT profile, and OCI_* environment variables.
			return common.DefaultConfigProvider(), nil
		}
		return common.CustomProfileConfigProvider(c.ConfigFile, c.profile()), nil
	case AuthTypeSessionToken:
		provider := common.CustomProfileSessionTokenConfigProvider(c.ConfigFile, c.profile())
		if provider == nil {
			return nil, fmt.Errorf("profile %q in %s is not a valid session token profile", c.profile(), c.configFilePath())
		}
		return provider, nil
	case AuthTypeInstancePrincipal:
		return auth.InstancePrincipalConfigurationProvider()
	case AuthTypeWorkloadIdentity:
		return auth.OkeWorkloadIdentityConfigurationProvider()
	case AuthTypeResourcePrincipal:
		return auth.ResourcePrincipalConfigurationProvider()
	default:
		return nil, fmt.Errorf("unsupported OCI auth type %q", authType)
	}
}

func newObjectStorageClient(c *config) (*objectstorage.ObjectStorageClient, error) {
	authType, err := c.resolveAuthType()
	if err != nil {
		return nil, err
	}
	provider, err := newConfigurationProvider(c, authType)
	if err != nil {
		return nil, fmt.Errorf("failed to create %s configuration provider: %w", authType, err)
	}
	logrus.Infof("Using OCI %s authentication", authType)

	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, fmt.Errorf("failed to create object storage client: %w", err)
	}
	if c.Region != "" {
		client.SetRegion(c.Region)
	}
	// The SDK default HTTP client gives up after 60s, which is too short for large log
	// downloads. Keep its transport (proxy and TLS settings) and only extend the deadline.
	if httpClient, ok := client.HTTPClient.(*http.Client); ok {
		httpClient.Timeout = downloadTimeout
	}
	return &client, nil
}

func resolveNamespace(ctx context.Context, client objectStorageClient, compartmentID string) (string, error) {
	request := objectstorage.GetNamespaceRequest{RequestMetadata: requestMetadata()}
	if compartmentID != "" {
		request.CompartmentId = &compartmentID
	}
	resp, err := client.GetNamespace(ctx, request)
	if err != nil {
		return "", err
	}
	if resp.Value == nil || *resp.Value == "" {
		return "", errors.New("GetNamespace returned an empty namespace")
	}
	return *resp.Value, nil
}

// ensureBucketExists checks the bucket and, when a compartment is configured,
// creates it if missing. Object Storage answers 404 both when the bucket does not
// exist and when the principal may not read it, so the error message mentions both.
func (r *RayLogsHandler) ensureBucketExists(ctx context.Context, compartmentID string) error {
	_, err := r.Client.HeadBucket(ctx, objectstorage.HeadBucketRequest{
		NamespaceName:   &r.Namespace,
		BucketName:      &r.Bucket,
		RequestMetadata: requestMetadata(),
	})
	if err == nil {
		logrus.Infof("Bucket %s already exists", r.Bucket)
		return nil
	}
	if !isHTTPStatus(err, http.StatusNotFound) {
		return fmt.Errorf("failed to check bucket %s: %w", r.Bucket, err)
	}
	if compartmentID == "" {
		return fmt.Errorf("bucket %s not found in namespace %s (or the principal is not allowed to read it); create it or set %s to have it created",
			r.Bucket, r.Namespace, CompartmentIDEnvVar)
	}

	logrus.Infof("Bucket %s does not exist, creating...", r.Bucket)
	_, err = r.Client.CreateBucket(ctx, objectstorage.CreateBucketRequest{
		NamespaceName: &r.Namespace,
		CreateBucketDetails: objectstorage.CreateBucketDetails{
			Name:          &r.Bucket,
			CompartmentId: &compartmentID,
		},
		RequestMetadata: requestMetadata(),
	})
	if err != nil {
		if isHTTPStatus(err, http.StatusConflict) {
			logrus.Infof("Bucket %s already exists", r.Bucket)
			return nil
		}
		return fmt.Errorf("failed to create bucket %s: %w", r.Bucket, err)
	}
	logrus.Infof("Successfully created bucket %s", r.Bucket)
	return nil
}

// newHandler wires a client to the configuration: it resolves the namespace when it
// is not configured, makes sure the bucket exists and normalizes the session paths.
func newHandler(client objectStorageClient, c *config) (*RayLogsHandler, error) {
	ctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
	defer cancel()

	namespace := c.Namespace
	if namespace == "" {
		resolved, err := resolveNamespace(ctx, client, c.CompartmentID)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve object storage namespace: %w", err)
		}
		namespace = resolved
		logrus.Infof("Resolved OCI object storage namespace %s", namespace)
	}

	handler := &RayLogsHandler{
		Client:              client,
		LogFiles:            make(chan string, 100),
		Namespace:           namespace,
		Bucket:              c.Bucket,
		RootDir:             c.RootDir,
		RayClusterName:      c.RayClusterName,
		RayClusterNamespace: c.RayClusterNamespace,
		RayNodeName:         c.RayNodeName,
		LogBatching:         c.LogBatching,
		PushInterval:        c.PushInterval,
	}

	logrus.Infof("Checking if bucket %s exists in namespace %s...", handler.Bucket, handler.Namespace)
	if err := handler.ensureBucketExists(ctx, c.CompartmentID); err != nil {
		return nil, fmt.Errorf("failed to ensure bucket exists: %w", err)
	}

	sessionDir := filepath.Clean(strings.TrimSpace(c.SessionDir))
	logdir := filepath.Clean(strings.TrimSpace(path.Join(sessionDir, utils.RAY_SESSIONDIR_LOGDIR_NAME)))
	logrus.Infof("Clean logdir is %s", logdir)
	handler.SessionDir = sessionDir
	handler.LogDir = logdir

	return handler, nil
}

func New(c *config) (*RayLogsHandler, error) {
	logrus.Infof("Begin to create oci object storage client ...")
	client, err := newObjectStorageClient(c)
	if err != nil {
		logrus.Errorf("Failed to create oci object storage client: %v", err)
		return nil, err
	}
	return newHandler(client, c)
}
