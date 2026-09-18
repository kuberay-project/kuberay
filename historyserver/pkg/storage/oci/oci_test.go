package oci

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clustermetadata"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const (
	testNamespace = "testnamespace"
	testBucket    = "test-bucket"
	testRootDir   = "ray_historyserver"
)

// fakeServiceError mimics the errors returned by the OCI SDK for HTTP failures.
type fakeServiceError struct {
	status  int
	code    string
	message string
}

func (e fakeServiceError) Error() string {
	return fmt.Sprintf("%d %s: %s", e.status, e.code, e.message)
}
func (e fakeServiceError) GetHTTPStatusCode() int  { return e.status }
func (e fakeServiceError) GetMessage() string      { return e.message }
func (e fakeServiceError) GetCode() string         { return e.code }
func (e fakeServiceError) GetOpcRequestID() string { return "fake-request-id" }

var _ common.ServiceError = fakeServiceError{}

func notFound(code string) error {
	return fakeServiceError{status: http.StatusNotFound, code: code, message: code}
}

// fakeClient is an in-memory objectStorageClient for a single namespace.
type fakeClient struct {
	mu sync.Mutex

	namespace string
	buckets   map[string]bool
	objects   map[string][]byte
	// pageSize overrides the request limit when > 0 so pagination can be exercised.
	pageSize int
	// getObjectErr is returned by GetObject when set.
	getObjectErr error

	contentLengths   map[string]int64
	putCalls         int
	listCalls        int
	getNamespaceArgs []string
	createBucketArgs []string
}

var _ objectStorageClient = (*fakeClient)(nil)

func newFakeClient(objects map[string]string) *fakeClient {
	f := &fakeClient{
		namespace:      testNamespace,
		buckets:        map[string]bool{testBucket: true},
		objects:        map[string][]byte{},
		contentLengths: map[string]int64{},
	}
	for name, content := range objects {
		f.objects[name] = []byte(content)
	}
	return f
}

func (f *fakeClient) checkBucket(namespace, bucket *string) error {
	if namespace == nil || *namespace != f.namespace {
		return notFound("NamespaceNotFound")
	}
	if bucket == nil || !f.buckets[*bucket] {
		return notFound("BucketNotFound")
	}
	return nil
}

func (f *fakeClient) GetNamespace(_ context.Context, request objectstorage.GetNamespaceRequest) (objectstorage.GetNamespaceResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	compartment := ""
	if request.CompartmentId != nil {
		compartment = *request.CompartmentId
	}
	f.getNamespaceArgs = append(f.getNamespaceArgs, compartment)
	return objectstorage.GetNamespaceResponse{Value: new(f.namespace)}, nil
}

func (f *fakeClient) HeadBucket(_ context.Context, request objectstorage.HeadBucketRequest) (objectstorage.HeadBucketResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return objectstorage.HeadBucketResponse{}, f.checkBucket(request.NamespaceName, request.BucketName)
}

func (f *fakeClient) CreateBucket(_ context.Context, request objectstorage.CreateBucketRequest) (objectstorage.CreateBucketResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if request.NamespaceName == nil || *request.NamespaceName != f.namespace {
		return objectstorage.CreateBucketResponse{}, notFound("NamespaceNotFound")
	}
	if request.Name == nil || request.CompartmentId == nil {
		return objectstorage.CreateBucketResponse{}, fakeServiceError{status: http.StatusBadRequest, code: "MissingParameter"}
	}
	f.createBucketArgs = append(f.createBucketArgs, *request.CompartmentId)
	if f.buckets[*request.Name] {
		return objectstorage.CreateBucketResponse{}, fakeServiceError{status: http.StatusConflict, code: "BucketAlreadyExists"}
	}
	f.buckets[*request.Name] = true
	return objectstorage.CreateBucketResponse{}, nil
}

func (f *fakeClient) HeadObject(_ context.Context, request objectstorage.HeadObjectRequest) (objectstorage.HeadObjectResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkBucket(request.NamespaceName, request.BucketName); err != nil {
		return objectstorage.HeadObjectResponse{}, err
	}
	if _, ok := f.objects[*request.ObjectName]; !ok {
		return objectstorage.HeadObjectResponse{}, notFound("ObjectNotFound")
	}
	return objectstorage.HeadObjectResponse{}, nil
}

func (f *fakeClient) PutObject(_ context.Context, request objectstorage.PutObjectRequest) (objectstorage.PutObjectResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.checkBucket(request.NamespaceName, request.BucketName); err != nil {
		return objectstorage.PutObjectResponse{}, err
	}
	f.putCalls++
	data, err := io.ReadAll(request.PutObjectBody)
	if err != nil {
		return objectstorage.PutObjectResponse{}, err
	}
	f.objects[*request.ObjectName] = data
	if request.ContentLength != nil {
		f.contentLengths[*request.ObjectName] = *request.ContentLength
	}
	return objectstorage.PutObjectResponse{}, nil
}

func (f *fakeClient) GetObject(_ context.Context, request objectstorage.GetObjectRequest) (objectstorage.GetObjectResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.getObjectErr != nil {
		return objectstorage.GetObjectResponse{}, f.getObjectErr
	}
	if err := f.checkBucket(request.NamespaceName, request.BucketName); err != nil {
		return objectstorage.GetObjectResponse{}, err
	}
	data, ok := f.objects[*request.ObjectName]
	if !ok {
		return objectstorage.GetObjectResponse{}, notFound("ObjectNotFound")
	}
	return objectstorage.GetObjectResponse{Content: io.NopCloser(bytes.NewReader(data))}, nil
}

type fakeListEntry struct {
	key      string
	isPrefix bool
}

// ListObjects emulates prefix, delimiter, start and limit the way Object Storage does:
// results are sorted, common prefixes are collapsed, and NextStartWith points at the
// first entry of the next page.
func (f *fakeClient) ListObjects(_ context.Context, request objectstorage.ListObjectsRequest) (objectstorage.ListObjectsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listCalls++
	if err := f.checkBucket(request.NamespaceName, request.BucketName); err != nil {
		return objectstorage.ListObjectsResponse{}, err
	}

	prefix, delimiter, start := "", "", ""
	if request.Prefix != nil {
		prefix = *request.Prefix
	}
	if request.Delimiter != nil {
		delimiter = *request.Delimiter
	}
	if request.Start != nil {
		start = *request.Start
	}
	limit := 1000
	if request.Limit != nil {
		limit = *request.Limit
	}
	if f.pageSize > 0 {
		limit = f.pageSize
	}

	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) && key >= start {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)

	var entries []fakeListEntry
	for _, key := range keys {
		if delimiter != "" {
			rest := key[len(prefix):]
			if idx := strings.Index(rest, delimiter); idx >= 0 {
				commonPrefix := prefix + rest[:idx+len(delimiter)]
				if len(entries) == 0 || entries[len(entries)-1].key != commonPrefix {
					entries = append(entries, fakeListEntry{key: commonPrefix, isPrefix: true})
				}
				continue
			}
		}
		entries = append(entries, fakeListEntry{key: key})
	}

	resp := objectstorage.ListObjectsResponse{}
	if len(entries) > limit {
		resp.NextStartWith = new(entries[limit].key)
		entries = entries[:limit]
	}
	for _, entry := range entries {
		if entry.isPrefix {
			resp.Prefixes = append(resp.Prefixes, entry.key)
		} else {
			resp.Objects = append(resp.Objects, objectstorage.ObjectSummary{Name: new(entry.key)})
		}
	}
	return resp, nil
}

func newTestHandler(client *fakeClient) *RayLogsHandler {
	return &RayLogsHandler{
		Client:    client,
		Namespace: testNamespace,
		Bucket:    testBucket,
		RootDir:   testRootDir,
	}
}

func TestNewHandlerResolvesNamespaceWhenNotConfigured(t *testing.T) {
	client := newFakeClient(nil)
	cfg := &config{Bucket: testBucket, CompartmentID: "ocid1.compartment.oc1..example"}
	cfg.RootDir = testRootDir
	cfg.SessionDir = " /tmp/ray/session_latest/ "

	handler, err := newHandler(client, cfg)
	require.NoError(t, err)
	assert.Equal(t, testNamespace, handler.Namespace)
	assert.Equal(t, []string{"ocid1.compartment.oc1..example"}, client.getNamespaceArgs)
	assert.Empty(t, client.createBucketArgs, "existing bucket must not be recreated")
	assert.Equal(t, "/tmp/ray/session_latest", handler.SessionDir)
	assert.Equal(t, "/tmp/ray/session_latest/"+utils.RAY_SESSIONDIR_LOGDIR_NAME, handler.LogDir)
	assert.Equal(t, testRootDir, handler.RootDir)
}

func TestNewHandlerUsesConfiguredNamespace(t *testing.T) {
	client := newFakeClient(nil)
	cfg := &config{Bucket: testBucket, Namespace: testNamespace}

	handler, err := newHandler(client, cfg)
	require.NoError(t, err)
	assert.Equal(t, testNamespace, handler.Namespace)
	assert.Empty(t, client.getNamespaceArgs, "GetNamespace must not be called when the namespace is configured")
}

func TestNewHandlerCreatesMissingBucketInCompartment(t *testing.T) {
	client := newFakeClient(nil)
	cfg := &config{Bucket: "new-bucket", Namespace: testNamespace, CompartmentID: "ocid1.compartment.oc1..example"}

	handler, err := newHandler(client, cfg)
	require.NoError(t, err)
	assert.Equal(t, "new-bucket", handler.Bucket)
	assert.True(t, client.buckets["new-bucket"])
	assert.Equal(t, []string{"ocid1.compartment.oc1..example"}, client.createBucketArgs)
}

func TestNewHandlerFailsForMissingBucketWithoutCompartment(t *testing.T) {
	client := newFakeClient(nil)
	cfg := &config{Bucket: "new-bucket", Namespace: testNamespace}

	_, err := newHandler(client, cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "new-bucket")
	assert.Contains(t, err.Error(), CompartmentIDEnvVar)
	assert.Empty(t, client.createBucketArgs)
}

func TestEnsureBucketExistsTreatsConflictAsSuccess(t *testing.T) {
	client := newFakeClient(nil)
	handler := newTestHandler(client)
	// Simulate a race: the bucket appears between HeadBucket and CreateBucket.
	delete(client.buckets, testBucket)
	racing := &racingClient{fakeClient: client}
	handler.Client = racing

	require.NoError(t, handler.ensureBucketExists(context.Background(), "ocid1.compartment.oc1..example"))
	assert.Equal(t, 1, racing.createCalls)
}

// racingClient makes the bucket exist right before CreateBucket runs.
type racingClient struct {
	*fakeClient
	createCalls int
}

func (r *racingClient) CreateBucket(ctx context.Context, request objectstorage.CreateBucketRequest) (objectstorage.CreateBucketResponse, error) {
	r.createCalls++
	r.fakeClient.buckets[*request.Name] = true
	return r.fakeClient.CreateBucket(ctx, request)
}

func TestCreateDirectory(t *testing.T) {
	client := newFakeClient(nil)
	handler := newTestHandler(client)

	require.NoError(t, handler.CreateDirectory("new/dir"))
	_, ok := client.objects["new/dir/"]
	assert.True(t, ok, "expected directory marker new/dir/")
	assert.Equal(t, 1, client.putCalls)
	assert.Equal(t, int64(0), client.contentLengths["new/dir/"])

	require.NoError(t, handler.CreateDirectory("new/dir"))
	assert.Equal(t, 1, client.putCalls, "existing directory must not be recreated")

	require.NoError(t, handler.CreateDirectory("/leading/slash/"))
	_, ok = client.objects["leading/slash/"]
	assert.True(t, ok, "leading slash must be trimmed and trailing slash normalized")
}

func TestWriteFile(t *testing.T) {
	client := newFakeClient(nil)
	handler := newTestHandler(client)

	require.NoError(t, handler.WriteFile("test/file.txt", strings.NewReader("hello world")))
	assert.Equal(t, "hello world", string(client.objects["test/file.txt"]))
	assert.Equal(t, int64(len("hello world")), client.contentLengths["test/file.txt"])

	// A reader that is not at the start only uploads what is left.
	reader := strings.NewReader("skip:payload")
	_, err := reader.Seek(int64(len("skip:")), io.SeekStart)
	require.NoError(t, err)
	require.NoError(t, handler.WriteFile("/test/partial.txt", reader))
	assert.Equal(t, "payload", string(client.objects["test/partial.txt"]))
	assert.Equal(t, int64(len("payload")), client.contentLengths["test/partial.txt"])
}

func TestWriteFileReturnsUploadError(t *testing.T) {
	client := newFakeClient(nil)
	handler := newTestHandler(client)
	handler.Bucket = "missing-bucket"

	err := handler.WriteFile("test/file.txt", strings.NewReader("x"))
	require.Error(t, err)
	assert.True(t, isHTTPStatus(err, http.StatusNotFound))
}

func listFilesFixture() map[string]string {
	return map[string]string{
		testRootDir + "/cluster1/logs/":                 "",
		testRootDir + "/cluster1/logs/file1.txt":        "a",
		testRootDir + "/cluster1/logs/file2.log":        "b",
		testRootDir + "/cluster1/logs/subdir/":          "",
		testRootDir + "/cluster1/logs/subdir/file3.txt": "c",
		testRootDir + "/cluster1/other/file4.txt":       "d",
		testRootDir + "/cluster2/logs/file5.txt":        "e",
		testRootDir + "/cluster2/logs/subdir2/":         "",
	}
}

func TestListFiles(t *testing.T) {
	client := newFakeClient(listFilesFixture())
	handler := newTestHandler(client)

	tests := []struct {
		name      string
		clusterID string
		directory string
		expected  []string
	}{
		{
			name:      "list_files",
			clusterID: "cluster1",
			directory: utils.RAY_SESSIONDIR_LOGDIR_NAME,
			expected:  []string{"file1.txt", "file2.log", "subdir/"},
		},
		{
			name:      "list_other",
			clusterID: "cluster1",
			directory: "other",
			expected:  []string{"file4.txt"},
		},
		{
			name:      "list_nonexistent",
			clusterID: "cluster1",
			directory: "nonexistent",
			expected:  []string{},
		},
		{
			name:      "list_cluster2",
			clusterID: "cluster2",
			directory: utils.RAY_SESSIONDIR_LOGDIR_NAME,
			expected:  []string{"file5.txt", "subdir2/"},
		},
		{
			name:      "list_empty_subdir_marker_only",
			clusterID: "cluster2",
			directory: "logs/subdir2",
			expected:  []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := handler.ListFiles(tc.clusterID, tc.directory)
			sort.Strings(files)
			sort.Strings(tc.expected)
			if diff := cmp.Diff(tc.expected, files); diff != "" {
				t.Errorf("ListFiles(%q, %q) returned diff (-want +got):\n%s", tc.clusterID, tc.directory, diff)
			}
		})
	}
}

func TestListFilesTrimsLeadingSlashInRootDir(t *testing.T) {
	client := newFakeClient(listFilesFixture())
	handler := newTestHandler(client)
	handler.RootDir = "/" + testRootDir

	files := handler.ListFiles("cluster1", "other")
	assert.Equal(t, []string{"file4.txt"}, files)
}

func TestListFilesPaginates(t *testing.T) {
	objects := map[string]string{}
	var expected []string
	for i := range 5 {
		name := fmt.Sprintf("file%d.txt", i)
		objects[testRootDir+"/cluster1/logs/"+name] = "x"
		expected = append(expected, name)
	}
	for i := range 2 {
		dir := fmt.Sprintf("dir%d", i)
		objects[testRootDir+"/cluster1/logs/"+dir+"/inner.txt"] = "y"
		expected = append(expected, dir+"/")
	}
	client := newFakeClient(objects)
	client.pageSize = 2
	handler := newTestHandler(client)

	files := handler.ListFiles("cluster1", utils.RAY_SESSIONDIR_LOGDIR_NAME)
	sort.Strings(files)
	sort.Strings(expected)
	assert.Equal(t, expected, files)
	assert.GreaterOrEqual(t, client.listCalls, 4, "expected several pages of size 2")
}

func TestListFilesReturnsEmptyOnError(t *testing.T) {
	client := newFakeClient(listFilesFixture())
	handler := newTestHandler(client)
	handler.Bucket = "missing-bucket"

	files := handler.ListFiles("cluster1", utils.RAY_SESSIONDIR_LOGDIR_NAME)
	assert.Equal(t, []string{}, files)
}

func TestList(t *testing.T) {
	newer := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	older := newer.Add(-time.Hour)
	newerSession := "session_" + newer.Format("2006-01-02_15-04-05_000000")
	olderSession := "session_" + older.Format("2006-01-02_15-04-05_000000")

	meta := testRootDir + "/" + clustermetadata.ClusterMetadataDir + "/"
	client := newFakeClient(map[string]string{
		meta:                 "",
		meta + "raycluster/": "",
		meta + "raycluster/defaultns_mycluster1/" + olderSession:              "",
		meta + "raycluster/testns_mycluster2/" + newerSession:                 "",
		meta + "rayjob/defaultns_myrayjob_mycluster3/" + newerSession:         "",
		meta + "rayservice/defaultns_myraysvc_mycluster4/" + olderSession:     "",
		meta + "raycluster/defaultns_broken/not-a-session":                    "",
		testRootDir + "/defaultns_mycluster1/" + olderSession + "/logs/a.log": "unrelated",
	})
	handler := newTestHandler(client)

	result := handler.List()

	format := func(ts time.Time) string { return ts.UTC().Format("2006-01-02T15:04:05Z") }
	expected := []utils.ClusterInfo{
		{Name: "mycluster2", Namespace: "testns", SessionName: newerSession, CreateTimeStamp: newer.Unix(), CreateTime: format(newer)},
		{Name: "mycluster3", OwnerKind: "rayjob", OwnerName: "myrayjob", Namespace: "defaultns", SessionName: newerSession, CreateTimeStamp: newer.Unix(), CreateTime: format(newer)},
		{Name: "mycluster1", Namespace: "defaultns", SessionName: olderSession, CreateTimeStamp: older.Unix(), CreateTime: format(older)},
		{Name: "mycluster4", OwnerKind: "rayservice", OwnerName: "myraysvc", Namespace: "defaultns", SessionName: olderSession, CreateTimeStamp: older.Unix(), CreateTime: format(older)},
	}

	// The list is sorted newest first; entries with the same timestamp keep a stable order by name for the comparison.
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].CreateTimeStamp != result[j].CreateTimeStamp {
			return result[i].CreateTimeStamp > result[j].CreateTimeStamp
		}
		return result[i].Name < result[j].Name
	})
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("List() returned diff (-want +got):\n%s", diff)
	}
}

func TestListReturnsEmptyOnError(t *testing.T) {
	client := newFakeClient(nil)
	handler := newTestHandler(client)
	handler.Bucket = "missing-bucket"

	assert.Empty(t, handler.List())
}

func TestGetContent(t *testing.T) {
	client := newFakeClient(map[string]string{
		testRootDir + "/clusterA/logs/direct.log":       "direct content",
		testRootDir + "/clusterA/logs/node1/nested.log": "nested content",
		testRootDir + "/clusterA/logs/node1/":           "",
		testRootDir + "/clusterB/logs/nested.log":       "other cluster",
	})
	handler := newTestHandler(client)

	t.Run("direct_hit", func(t *testing.T) {
		reader := handler.GetContent("clusterA", "logs/direct.log")
		require.NotNil(t, reader)
		content, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "direct content", string(content))
	})

	t.Run("fallback_to_nested_file_in_same_directory", func(t *testing.T) {
		reader := handler.GetContent("clusterA", "logs/nested.log")
		require.NotNil(t, reader)
		content, err := io.ReadAll(reader)
		require.NoError(t, err)
		assert.Equal(t, "nested content", string(content))
	})

	t.Run("missing_file_returns_nil", func(t *testing.T) {
		assert.Nil(t, handler.GetContent("clusterA", "logs/missing.log"))
	})

	t.Run("non_404_error_does_not_list", func(t *testing.T) {
		before := client.listCalls
		client.getObjectErr = fakeServiceError{status: http.StatusInternalServerError, code: "InternalServerError"}
		defer func() { client.getObjectErr = nil }()

		assert.Nil(t, handler.GetContent("clusterA", "logs/direct.log"))
		assert.Equal(t, before, client.listCalls, "a non-404 failure must not trigger the listing fallback")
	})
}

func TestIsHTTPStatus(t *testing.T) {
	assert.True(t, isHTTPStatus(notFound("ObjectNotFound"), http.StatusNotFound))
	assert.False(t, isHTTPStatus(notFound("ObjectNotFound"), http.StatusConflict))
	assert.False(t, isHTTPStatus(errors.New("plain error"), http.StatusNotFound))
	assert.False(t, isHTTPStatus(nil, http.StatusNotFound))
}

func TestObjectName(t *testing.T) {
	assert.Equal(t, "a/b/c", objectName("a", "b", "c"))
	assert.Equal(t, "a/b/c", objectName("/a", "b/", "c"))
	assert.Equal(t, "b/c", objectName("", "b", "c"))
	assert.Equal(t, "cluster/file", objectName("", "cluster", "file"))
}

// --- configuration -----------------------------------------------------------

func clearOCIEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		BucketEnvVar, NamespaceEnvVar, RegionEnvVar, CompartmentIDEnvVar, AuthTypeEnvVar,
		ConfigFileEnvVar, ConfigProfileEnvVar, resourcePrincipalVersionEnvVar, kubernetesServiceHostEnvVar,
	} {
		t.Setenv(name, "")
	}
}

func TestConfigDefaults(t *testing.T) {
	clearOCIEnv(t)

	cfg := &config{}
	cfg.completeHSConfig(&types.RayHistoryServerConfig{RootDir: "root"}, nil)

	assert.Equal(t, DefaultBucket, cfg.Bucket)
	assert.Empty(t, cfg.Namespace)
	assert.Empty(t, cfg.Region)
	assert.Empty(t, cfg.CompartmentID)
	assert.Empty(t, cfg.AuthType)
	assert.Equal(t, "root", cfg.RootDir)
	assert.Equal(t, defaultConfigProfile, cfg.profile())
}

func TestConfigFromEnv(t *testing.T) {
	clearOCIEnv(t)
	t.Setenv(BucketEnvVar, "env-bucket")
	t.Setenv(NamespaceEnvVar, "env-namespace")
	t.Setenv(RegionEnvVar, "us-ashburn-1")
	t.Setenv(CompartmentIDEnvVar, "ocid1.compartment.oc1..env")
	t.Setenv(AuthTypeEnvVar, "instance_principal")
	t.Setenv(ConfigFileEnvVar, "/etc/oci/config")
	t.Setenv(ConfigProfileEnvVar, "PROD")

	collectorConfig := &types.RayCollectorConfig{RootDir: "root", RayClusterName: "rc", SessionDir: "/tmp/ray/session_x"}
	cfg := &config{}
	cfg.complete(collectorConfig, nil)

	assert.Equal(t, "env-bucket", cfg.Bucket)
	assert.Equal(t, "env-namespace", cfg.Namespace)
	assert.Equal(t, "us-ashburn-1", cfg.Region)
	assert.Equal(t, "ocid1.compartment.oc1..env", cfg.CompartmentID)
	assert.Equal(t, "instance_principal", cfg.AuthType)
	assert.Equal(t, "/etc/oci/config", cfg.ConfigFile)
	assert.Equal(t, "PROD", cfg.ConfigProfile)
	assert.Equal(t, *collectorConfig, cfg.RayCollectorConfig)
}

func TestConfigJSONOverridesEnv(t *testing.T) {
	clearOCIEnv(t)
	t.Setenv(BucketEnvVar, "env-bucket")
	t.Setenv(RegionEnvVar, "us-ashburn-1")

	cfg := &config{}
	cfg.completeHSConfig(&types.RayHistoryServerConfig{RootDir: "root"}, map[string]any{
		"ociBucket":        "json-bucket",
		"ociNamespace":     "json-namespace",
		"ociCompartmentId": "ocid1.compartment.oc1..json",
		"ociAuthType":      "api_key",
		"ociConfigFile":    "/var/oci/config",
		"ociConfigProfile": "DEV",
		"ociRegion":        42, // wrong type is ignored, env value wins
	})

	assert.Equal(t, "json-bucket", cfg.Bucket)
	assert.Equal(t, "json-namespace", cfg.Namespace)
	assert.Equal(t, "us-ashburn-1", cfg.Region)
	assert.Equal(t, "ocid1.compartment.oc1..json", cfg.CompartmentID)
	assert.Equal(t, "api_key", cfg.AuthType)
	assert.Equal(t, "/var/oci/config", cfg.ConfigFile)
	assert.Equal(t, "DEV", cfg.ConfigProfile)
	assert.Equal(t, types.RayCollectorConfig{RootDir: "root"}, cfg.RayCollectorConfig)
}

func TestParseAuthType(t *testing.T) {
	tests := []struct {
		raw      string
		expected AuthType
		wantErr  bool
	}{
		{raw: "", expected: ""},
		{raw: "api_key", expected: AuthTypeAPIKey},
		{raw: " API_KEY ", expected: AuthTypeAPIKey},
		{raw: "apikey", expected: AuthTypeAPIKey},
		{raw: "config_file", expected: AuthTypeAPIKey},
		{raw: "session_token", expected: AuthTypeSessionToken},
		{raw: "security_token", expected: AuthTypeSessionToken},
		{raw: "instance_principal", expected: AuthTypeInstancePrincipal},
		{raw: "oke_workload_identity", expected: AuthTypeWorkloadIdentity},
		{raw: "workload_identity", expected: AuthTypeWorkloadIdentity},
		{raw: "resource_principal", expected: AuthTypeResourcePrincipal},
		{raw: "bogus", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := parseAuthType(tc.raw)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "bogus")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func writeOCIConfig(t *testing.T, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config")
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
	return p
}

const defaultProfileConfig = `[DEFAULT]
user=ocid1.user.oc1..example
fingerprint=aa:bb
tenancy=ocid1.tenancy.oc1..example
region=us-ashburn-1
key_file=/nonexistent/key.pem
`

const sessionProfileConfig = defaultProfileConfig + `
[dev]
# session profile created by "oci session authenticate"
fingerprint=cc:dd
tenancy=ocid1.tenancy.oc1..example
region=us-ashburn-1
key_file = /nonexistent/session_key.pem
security_token_file = /nonexistent/token
`

func TestProfileHasSessionToken(t *testing.T) {
	p := writeOCIConfig(t, sessionProfileConfig)
	assert.False(t, profileHasSessionToken(p, "DEFAULT"))
	assert.True(t, profileHasSessionToken(p, "dev"))
	assert.False(t, profileHasSessionToken(p, "missing"))
	assert.False(t, profileHasSessionToken(filepath.Join(t.TempDir(), "nope"), "dev"))
}

func TestResolveAuthType(t *testing.T) {
	missingFile := filepath.Join(t.TempDir(), "missing-config")

	tests := []struct {
		name     string
		env      map[string]string
		cfg      config
		expected AuthType
		wantErr  bool
	}{
		{
			name:     "explicit value wins over environment",
			env:      map[string]string{resourcePrincipalVersionEnvVar: "2.2", kubernetesServiceHostEnvVar: "10.0.0.1"},
			cfg:      config{AuthType: "api_key", ConfigFile: missingFile},
			expected: AuthTypeAPIKey,
		},
		{
			name:    "explicit invalid value fails",
			cfg:     config{AuthType: "bogus"},
			wantErr: true,
		},
		{
			name:     "resource principal inside a pod is OKE workload identity",
			env:      map[string]string{resourcePrincipalVersionEnvVar: "2.2", kubernetesServiceHostEnvVar: "10.0.0.1"},
			cfg:      config{ConfigFile: missingFile},
			expected: AuthTypeWorkloadIdentity,
		},
		{
			name:     "resource principal outside a pod",
			env:      map[string]string{resourcePrincipalVersionEnvVar: "2.2"},
			cfg:      config{ConfigFile: missingFile},
			expected: AuthTypeResourcePrincipal,
		},
		{
			name:     "api key profile in config file",
			cfg:      config{ConfigFile: writeOCIConfig(t, defaultProfileConfig)},
			expected: AuthTypeAPIKey,
		},
		{
			name:     "session token profile in config file",
			cfg:      config{ConfigFile: writeOCIConfig(t, sessionProfileConfig), ConfigProfile: "dev"},
			expected: AuthTypeSessionToken,
		},
		{
			name:     "config file from OCI_CONFIG_FILE",
			env:      map[string]string{ConfigFileEnvVar: writeOCIConfig(t, sessionProfileConfig), ConfigProfileEnvVar: "dev"},
			cfg:      config{ConfigProfile: "dev"},
			expected: AuthTypeSessionToken,
		},
		{
			name:     "nothing available falls back to instance principal",
			cfg:      config{ConfigFile: missingFile},
			expected: AuthTypeInstancePrincipal,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearOCIEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			got, err := tc.cfg.resolveAuthType()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestNewConfigurationProvider(t *testing.T) {
	t.Run("api key with explicit profile", func(t *testing.T) {
		cfg := &config{ConfigFile: writeOCIConfig(t, defaultProfileConfig), ConfigProfile: "DEFAULT"}
		provider, err := newConfigurationProvider(cfg, AuthTypeAPIKey)
		require.NoError(t, err)
		require.NotNil(t, provider)
		region, err := provider.Region()
		require.NoError(t, err)
		assert.Equal(t, "us-ashburn-1", region)
	})

	t.Run("session token profile", func(t *testing.T) {
		cfg := &config{ConfigFile: writeOCIConfig(t, sessionProfileConfig), ConfigProfile: "dev"}
		provider, err := newConfigurationProvider(cfg, AuthTypeSessionToken)
		require.NoError(t, err)
		require.NotNil(t, provider)
		region, err := provider.Region()
		require.NoError(t, err)
		assert.Equal(t, "us-ashburn-1", region)
	})

	t.Run("unknown auth type", func(t *testing.T) {
		_, err := newConfigurationProvider(&config{}, AuthType("bogus"))
		require.Error(t, err)
	})
}

func TestNewRejectsInvalidAuthType(t *testing.T) {
	clearOCIEnv(t)
	cfg := &config{Bucket: testBucket, AuthType: "bogus"}

	_, err := New(cfg)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bogus")
}
