package gcs

import (
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	gstorage "cloud.google.com/go/storage"
	"github.com/fsouza/fake-gcs-server/fakestorage"
	"github.com/google/go-cmp/cmp"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// setupFakeGCS creates a fake GCS server and a client connected to it.
func setupFakeGCS(t *testing.T, initialObjects ...fakestorage.Object) (*fakestorage.Server, *gstorage.Client, string) {
	t.Helper()

	// The bucket name(s) must be specified within the initialObjects
	// We can extract a bucket name if needed, assuming they all use the same one in this helper
	var bucketName string
	if len(initialObjects) > 0 {
		bucketName = initialObjects[0].BucketName
	} else {
		bucketName = "default-test-bucket" // Fallback if no initial objects
	}

	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: initialObjects,
	})
	if err != nil {
		t.Fatalf("Failed to create fake GCS server: %v", err)
	}
	t.Cleanup(func() { server.Stop() })

	// If no initial objects were provided, create the fallback bucket
	if len(initialObjects) == 0 {
		server.CreateBucketWithOpts(fakestorage.CreateBucketOpts{Name: bucketName})
	}

	client := server.Client()
	return server, client, bucketName
}

// createRayLogsHandler creates a RayLogsHandler instance with the fake client.
func createRayLogsHandler(client *gstorage.Client, bucketName string) *RayLogsHandler {
	return &RayLogsHandler{
		StorageClient: client,
		GCSBucket:     bucketName,
		RootDir:       "ray_historyserver",
	}
}

func TestCreateDirectory(t *testing.T) {
	server, client, bucketName := setupFakeGCS(t)
	handler := createRayLogsHandler(client, bucketName)

	tests := []struct {
		name        string
		path        string
		expectedObj string
	}{
		{
			name:        "new_directory",
			path:        "new/dir",
			expectedObj: "new/dir/",
		},
		{
			name:        "existing_directory",
			path:        "new/dir",
			expectedObj: "new/dir/",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := handler.CreateDirectory(tc.path); err != nil {
				t.Fatalf("CreateDirectory(%q) failed: %v", tc.path, err)
			}

			_, err := server.GetObject(bucketName, tc.expectedObj)
			if err != nil {
				t.Errorf("Expected directory object %q not found: %v", tc.expectedObj, err)
			}
		})
	}
}

func TestWriteFile(t *testing.T) {
	server, client, bucketName := setupFakeGCS(t)
	handler := createRayLogsHandler(client, bucketName)

	fileName := "test/file.txt"
	fileContent := "hello world"
	reader := strings.NewReader(fileContent)

	err := handler.WriteFile(fileName, reader)
	if err != nil {
		t.Fatalf("WriteFile(%q) failed: %v", fileName, err)
	}

	obj, err := server.GetObject(bucketName, fileName)
	if err != nil {
		t.Fatalf("Failed to get object %q from fake server: %v", fileName, err)
	}

	if string(obj.Content) != fileContent {
		t.Errorf("File content mismatch: got %q, want %q", string(obj.Content), fileContent)
	}
}

func TestListFiles(t *testing.T) {
	initialObjects := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster1/logs/file1.txt",
			},
			Content: []byte("a"),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster1/logs/file2.log",
			},
			Content: []byte("b"),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster1/logs/subdir/",
			},
			Content: []byte(""),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster1/logs/subdir/file3.txt",
			},
			Content: []byte("c"),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster1/other/file4.txt",
			},
			Content: []byte("d"),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster2/logs/file5.txt",
			},
			Content: []byte("e"),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster2/logs/subdir2/",
			},
			Content: []byte("e"),
		},
	}
	_, client, bucketName := setupFakeGCS(t, initialObjects...)
	handler := createRayLogsHandler(client, bucketName)

	tests := []struct {
		name      string
		prefix    string
		directory string
		expected  []string
	}{
		{
			name:      "list_files",
			prefix:    "cluster1",
			directory: utils.RAY_SESSIONDIR_LOGDIR_NAME,
			expected:  []string{"file1.txt", "file2.log", "subdir/"},
		},
		{
			name:      "list_other",
			prefix:    "cluster1",
			directory: "other",
			expected:  []string{"file4.txt"},
		},
		{
			name:      "list_nonexistent",
			prefix:    "cluster1",
			directory: "nonexistent",
			expected:  nil,
		},
		{
			name:      "list_cluster2",
			prefix:    "cluster2",
			directory: utils.RAY_SESSIONDIR_LOGDIR_NAME,
			expected:  []string{"file5.txt", "subdir2/"},
		},
		{
			name:      "list_empty_subdir",
			prefix:    "cluster2",
			directory: "logs/subdir2",
			expected:  nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			files := handler.ListFiles(tc.prefix, tc.directory)
			sort.Strings(files)
			sort.Strings(tc.expected)
			if diff := cmp.Diff(tc.expected, files); diff != "" {
				t.Errorf("ListFiles(%q, %q) returned diff (-want +got):\n%s", tc.prefix, tc.directory, diff)
			}
		})
	}
}

func TestList(t *testing.T) {
	// RootDir is "ray_historyserver"
	// Path format: {RootDir}/cluster-metadata/{ClusterKey}/{SessionName}
	ts := time.Now().UTC()
	sessionID := "session_" + ts.Format("2006-01-02_15-04-05_000000")

	initialObjects := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster-metadata/raycluster/defaultns_mycluster1/" + sessionID,
			},
			Content: []byte(""),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster-metadata/raycluster/testns_mycluster2/" + sessionID,
			},
			Content: []byte(""),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster-metadata/rayjob/defaultns_myrayjob_mycluster3/" + sessionID,
			},
			Content: []byte(""),
		},
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       "ray_historyserver/cluster-metadata/rayservice/defaultns_myraysvc_mycluster4/" + sessionID,
			},
			Content: []byte(""),
		},
	}
	_, client, bucketName := setupFakeGCS(t, initialObjects...)
	handler := createRayLogsHandler(client, bucketName)

	expected := []utils.ClusterInfo{
		{Name: "mycluster1", Namespace: "defaultns", SessionName: sessionID, CreateTimeStamp: ts.Unix(), CreateTime: ts.UTC().Format("2006-01-02T15:04:05Z")},
		{Name: "mycluster2", Namespace: "testns", SessionName: sessionID, CreateTimeStamp: ts.Unix(), CreateTime: ts.UTC().Format("2006-01-02T15:04:05Z")},
		{Name: "mycluster3", OwnerKind: "rayjob", OwnerName: "myrayjob", Namespace: "defaultns", SessionName: sessionID, CreateTimeStamp: ts.Unix(), CreateTime: ts.UTC().Format("2006-01-02T15:04:05Z")},
		{Name: "mycluster4", OwnerKind: "rayservice", OwnerName: "myraysvc", Namespace: "defaultns", SessionName: sessionID, CreateTimeStamp: ts.Unix(), CreateTime: ts.UTC().Format("2006-01-02T15:04:05Z")},
	}

	result := handler.List()

	// Sort results for consistent comparison
	sort.Slice(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		return result[i].Namespace < result[j].Namespace
	})

	if diff := cmp.Diff(expected, result); diff != "" {
		t.Errorf("List() returned diff (-want +got):\n%s", diff)
	}
}

// contentMatchGlob is asserted directly because fake-gcs-server does not
// implement matchGlob, so no test in this package can observe what the pattern
// actually selects. This checks the pattern that gets sent, not its effect.
func TestContentMatchGlobIsRooted(t *testing.T) {
	tests := []struct {
		name      string
		rootDir   string
		clusterID string
		fileName  string
		want      string
	}{
		{
			name:      "rooted",
			rootDir:   "ray_historyserver",
			clusterID: "clusters/clusterA_ns",
			fileName:  "session123/logs/important.log",
			want:      "ray_historyserver/**/clusters/clusterA_ns*/**/session123/logs/important.log",
		},
		{
			name:      "no root dir keeps the previous unanchored pattern",
			rootDir:   "",
			clusterID: "clusters/clusterA_ns",
			fileName:  "session123/logs/important.log",
			want:      "**/clusters/clusterA_ns*/**/session123/logs/important.log",
		},
		{
			name:      "leading slash on root dir is trimmed",
			rootDir:   "/ray_historyserver",
			clusterID: "clusterA",
			fileName:  "important.log",
			want:      "ray_historyserver/**/clusterA*/**/important.log",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := contentMatchGlob(tt.rootDir, tt.clusterID, tt.fileName)
			if got != tt.want {
				t.Errorf("contentMatchGlob(%q, %q, %q) = %q, want %q",
					tt.rootDir, tt.clusterID, tt.fileName, got, tt.want)
			}
		})
	}
}

func TestGetContent(t *testing.T) {
	prefix := "clusterA"
	fileName := "important.log"
	objPath := "ray_historyserver/clusters/clusterA_ns/sessions/session123/logs/" + fileName
	fileContent := "Log content here"

	initialObjects := []fakestorage.Object{
		{
			ObjectAttrs: fakestorage.ObjectAttrs{
				BucketName: "test-bucket",
				Name:       objPath,
			},
			Content: []byte(fileContent),
		},
	}
	_, client, bucketName := setupFakeGCS(t, initialObjects...)
	handler := createRayLogsHandler(client, bucketName)

	reader := handler.GetContent(prefix, fileName)
	if reader == nil {
		t.Fatalf("GetContent(%q, %q) returned nil reader, expected non-nil", prefix, fileName)
	}

	content, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read content: %v", err)
	}

	if string(content) != fileContent {
		t.Errorf("GetContent(%q, %q) content mismatch: got %q, want %q", prefix, fileName, string(content), fileContent)
	}
}
